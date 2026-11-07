// Package aecdiag provides an opt-in, one-session AEC diagnostic. It never
// opens a microphone, sends audio, renews a lease or changes DSP decisions.
package aecdiag

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const Root = "/data/local/tmp/phase5-aec.local"
const MaxSamples = 20 * 16000
const PrecisionScope = "one-dot-aec-fullprecision"
const ReferenceScope = "one-dot-aec-reference-integrity"
const MaxLoopbackBytes = MaxSamples * 2 * 4
const MaxPrecisionBytes = MaxSamples * 7 * 4
const maxBatches = 640

var epoch = time.Now()

// Stamp is time since process start, not an ALSA hardware capture timestamp.
func Stamp() int64 { return time.Since(epoch).Nanoseconds() }

type Permit struct {
	Approved       bool   `json:"approved"`
	Scope          string `json:"scope"`
	ExpiresAt      int64  `json:"expires_at"`
	MaximumSeconds int    `json:"maximum_seconds"`
}

// SampleStart and Batch count all processed input, including before the permit.
// Arrival deltas expose scheduling gaps; ALSA/subscriber loss remains in mic logs.
type Meta struct {
	Stream           uint64 `json:"mic_stream"`
	Batch            uint64 `json:"batch"`
	SampleStart      uint64 `json:"sample_start"`
	ReceivedNS       int64  `json:"received_mono_ns"`
	Selected         int    `json:"selected_channel"`
	Clipped          uint64 `json:"clipped_total"` // S16 gain monitor; not float AEC input saturation
	GainDB           int    `json:"gain_db"`
	AEC              bool   `json:"aec_enabled"`
	Hardware         bool   `json:"hardware_reference_selected"`
	InputFormat      string `json:"aec_input_format,omitempty"`
	RefValid         bool   `json:"reference_valid"`
	Samples          int    `json:"samples"`
	Offset           int    `json:"pcm_sample_offset"`
	PostDSPHash      string `json:"post_dsp_sha256"`
	ArrivalExcessNS  int64  `json:"arrival_excess_ns"`
	SampleGap        bool   `json:"sample_gap"`
	FilterGeneration uint64 `json:"filter_generation,omitempty"`
	FilterHWFrames   uint64 `json:"filter_hw_frames,omitempty"`
	FilterDomain     string `json:"filter_domain,omitempty"`
	FilterLoadStatus string `json:"filter_load_status,omitempty"`
	FilterLoadSHA256 string `json:"filter_load_sha256,omitempty"`
	FilterLoadBytes  int    `json:"filter_load_bytes,omitempty"`
}

type Recorder struct {
	mu                 sync.Mutex
	active             atomic.Bool
	used               bool
	statusPath         string
	session            uint32
	stream             [16]byte
	deadline           time.Time
	startedNS          int64
	root               string
	pcm                []byte
	referenceIntegrity bool
	loopbacks          []byte
	precision          bool
	microphones        []byte
	rows               []Meta
	limit              int
	timer              *time.Timer
	done               chan struct{}
	err                error
}

func (r *Recorder) Active() bool { return r.active.Load() }
func (r *Recorder) PrecisionActive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active.Load() && r.precision
}
func (r *Recorder) ReferenceActive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active.Load() && r.referenceIntegrity
}
func (r *Recorder) Session() uint32 { r.mu.Lock(); defer r.mu.Unlock(); return r.session }

// Start consumes a fresh permit atomically before retaining any audio. One
// capture per process and per directory; existing evidence is never overwritten.
func (r *Recorder) Start(root string, session uint32, stream [16]byte, lease time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used || session == 0 || stream == [16]byte{} {
		return errors.New("diagnostic unavailable")
	}
	now := time.Now()
	if !lease.After(now) {
		return errors.New("closed microphone lease")
	}
	st, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return errors.New("private diagnostic directory required")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "permit.local.json" {
		return errors.New("diagnostic directory must contain only a fresh permit")
	}
	path := filepath.Join(root, "permit.local.json")
	st, err = os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Size() > 4096 || st.Mode().Perm()&0077 != 0 {
		return errors.New("invalid permit file")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	closeErr := f.Close()
	if err != nil || closeErr != nil || len(raw) > 4096 {
		return errors.New("cannot read bounded permit")
	}
	var p Permit
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&p) != nil || dec.Decode(new(any)) != io.EOF || !p.Approved || (p.Scope != "one-dot-aec-uplink" && p.Scope != PrecisionScope && p.Scope != ReferenceScope) || p.MaximumSeconds < 1 || p.MaximumSeconds > 20 || p.ExpiresAt <= now.Unix() || p.ExpiresAt > now.Unix()+300 {
		return errors.New("invalid or expired diagnostic permit")
	}
	if err = os.Rename(path, filepath.Join(root, "permit.used.local.json")); err != nil {
		return err
	}
	r.used = true
	// A consumed recorder must never be advertised as available. Failure stops
	// this diagnostic before retaining PCM, while leaving normal audio intact.
	if err = r.publishStatusLocked(); err != nil {
		_ = os.Remove(r.statusPath)
		return err
	}
	r.root, r.session, r.stream = root, session, stream
	r.limit = p.MaximumSeconds * 16000
	r.referenceIntegrity = p.Scope == ReferenceScope
	r.precision = p.Scope == PrecisionScope || r.referenceIntegrity
	if r.referenceIntegrity {
		r.loopbacks = make([]byte, 0, r.limit*8)
	}
	if r.precision {
		r.microphones = make([]byte, 0, r.limit*28)
	}
	r.deadline = now.Add(time.Duration(p.MaximumSeconds) * time.Second)
	if lease.Before(r.deadline) {
		r.deadline = lease
	}
	// Consent expiry also bounds capture, not merely permit claiming.
	if expiry := time.Unix(p.ExpiresAt, 0); expiry.Before(r.deadline) {
		r.deadline = expiry
	}
	r.pcm = make([]byte, 0, r.limit*6)
	r.rows = make([]Meta, 0, maxBatches)
	r.done = make(chan struct{})
	r.startedNS = Stamp()
	r.active.Store(true)
	r.timer = time.AfterFunc(time.Until(r.deadline), func() { r.Stop("deadline") })
	return nil
}

// Capture copies just three selected channels. No file I/O or writer queue is
// used on the audio goroutine. Missing hardware reference is zero-filled and
// explicitly invalid; it must never be interpreted as a measured silent ref.
func (r *Recorder) Capture(m Meta, pre, post, ref, dsp []byte, microphones ...[][]float32) {
	if !r.active.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return
	}
	if !time.Now().Before(r.deadline) {
		r.stopLocked("deadline")
		return
	}
	if m.ReceivedNS < r.startedNS {
		return
	} // no pre-permit microphone batch
	n := len(pre) / 2
	if n == 0 || n > 4096 || len(pre)%2 != 0 || len(post) != len(pre) || len(dsp) != len(pre) || m.Selected < 0 || m.Selected > 6 {
		r.stopLocked("invalid_batch")
		return
	}
	if len(r.rows) >= maxBatches {
		r.stopLocked("metadata_limit")
		return
	}
	if r.precision {
		groups := 1
		if r.referenceIntegrity {
			groups = 2
		}
		if len(microphones) != groups || len(microphones[0]) != 7 || !validFilterMeta(m) {
			r.stopLocked("invalid_precision_batch")
			return
		}
		if r.referenceIntegrity && len(microphones[1]) != 2 {
			r.stopLocked("invalid_reference_batch")
			return
		}
		channels := microphones[0]
		if r.referenceIntegrity {
			channels = append(append([][]float32(nil), channels...), microphones[1]...)
		}
		for _, channel := range channels {
			if len(channel) != n {
				r.stopLocked("invalid_precision_batch")
				return
			}
			for _, v := range channel {
				if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) || v < -32768 || v >= 32768 {
					r.stopLocked("invalid_precision_batch")
					return
				}
			}
		}
	}
	m.RefValid = len(ref) == len(pre)
	m.Offset = len(r.pcm) / 6
	if n > r.limit-m.Offset {
		n = r.limit - m.Offset
	}
	m.Samples = n
	if len(r.rows) > 0 {
		p := r.rows[len(r.rows)-1]
		m.ArrivalExcessNS = m.ReceivedNS - p.ReceivedNS - int64(p.Samples)*int64(time.Second)/16000
		m.SampleGap = m.Stream != p.Stream || m.SampleStart != p.SampleStart+uint64(p.Samples) || m.Batch != p.Batch+1
	}
	sum := sha256.Sum256(dsp[:n*2])
	m.PostDSPHash = hex.EncodeToString(sum[:])
	if r.precision {
		for i := 0; i < n; i++ {
			for ch := 0; ch < 7; ch++ {
				r.microphones = binary.LittleEndian.AppendUint32(r.microphones, math.Float32bits(microphones[0][ch][i]))
			}
		}
	}
	if r.referenceIntegrity {
		for i := 0; i < n; i++ {
			for ch := 0; ch < 2; ch++ {
				r.loopbacks = binary.LittleEndian.AppendUint32(r.loopbacks, math.Float32bits(microphones[1][ch][i]))
			}
		}
	}
	for i := 0; i < n*2; i += 2 {
		r.pcm = append(r.pcm, pre[i], pre[i+1], post[i], post[i+1])
		if m.RefValid {
			r.pcm = append(r.pcm, ref[i], ref[i+1])
		} else {
			r.pcm = append(r.pcm, 0, 0)
		}
	}
	r.rows = append(r.rows, m)
	if len(r.pcm)/6 >= r.limit {
		r.stopLocked("sample_limit")
	}
}

func (r *Recorder) StopFor(stream [16]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stream == stream {
		r.stopLocked("session_closed")
	}
}
func (r *Recorder) Stop(reason string) { r.mu.Lock(); defer r.mu.Unlock(); r.stopLocked(reason) }
func (r *Recorder) stopLocked(reason string) {
	if !r.active.Swap(false) {
		return
	}
	if r.timer != nil {
		r.timer.Stop()
	}
	pcm, rows, microphones, loopbacks := r.pcm, r.rows, r.microphones, r.loopbacks
	r.loopbacks = nil
	r.pcm, r.rows = nil, nil
	r.microphones = nil
	ended := Stamp()
	// Only this single bounded write can be outstanding, even if disk stalls.
	go func() {
		err := r.save(pcm, rows, microphones, loopbacks, reason, ended)
		r.mu.Lock()
		r.err = err
		close(r.done)
		r.mu.Unlock()
	}()
}
func exclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, e := f.Write(data)
	c := f.Close()
	if e != nil {
		return e
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return c
}
func (r *Recorder) save(pcm []byte, rows []Meta, microphones []byte, loopbacks []byte, reason string, ended int64) error {
	var events bytes.Buffer
	enc := json.NewEncoder(&events)
	for _, m := range rows {
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	if events.Len() > 512*1024 || len(pcm) > MaxSamples*6 {
		return errors.New("diagnostic output exceeds bounds")
	}
	if err := exclusive(filepath.Join(r.root, "aec.s16le"), pcm); err != nil {
		return err
	}
	if err := exclusive(filepath.Join(r.root, "events.local.jsonl"), events.Bytes()); err != nil {
		return err
	}
	ph, eh := sha256.Sum256(pcm), sha256.Sum256(events.Bytes())
	summary := map[string]any{"version": 2, "session": r.session, "stream": hex.EncodeToString(r.stream[:]), "rate": 16000, "channels": []string{"selected_gain_monitor", "selected_post_aec", "hardware_reference"}, "samples": len(pcm) / 6, "batches": len(rows), "started_mono_ns": r.startedNS, "ended_mono_ns": ended, "reason": reason, "pcm_sha256": hex.EncodeToString(ph[:]), "events_sha256": hex.EncodeToString(eh[:]), "physical_acceptance": false}
	if r.precision {
		if len(microphones) > MaxPrecisionBytes || len(microphones) != len(pcm)/6*28 {
			return errors.New("precision output outside bounds")
		}
		if err := exclusive(filepath.Join(r.root, "microphones.f32le"), microphones); err != nil {
			return err
		}
		sum := sha256.Sum256(microphones)
		summary["version"] = 3
		summary["microphone_format"] = "float32-le-s16-units"
		summary["microphone_channels"] = 7
		summary["microphone_sha256"] = hex.EncodeToString(sum[:])
		summary["initial_signal_history_captured"] = false
		summary["learned_filter_bytes_captured"] = false
	}
	if r.referenceIntegrity {
		if len(loopbacks) > MaxLoopbackBytes || len(loopbacks) != len(pcm)/6*8 {
			return errors.New("reference output outside bounds")
		}
		if err := exclusive(filepath.Join(r.root, "loopbacks.f32le"), loopbacks); err != nil {
			return err
		}
		sum := sha256.Sum256(loopbacks)
		summary["version"] = 4
		summary["scope"] = ReferenceScope
		summary["loopback_format"] = "float32-le-s16-units"
		summary["loopback_channels"] = []int{7, 8}
		summary["loopback_sha256"] = hex.EncodeToString(sum[:])
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	// Manifest is last: absent manifest means incomplete evidence, never success.
	return exclusive(filepath.Join(r.root, "capture.local.json"), raw)
}

// Wait is for offline tests/collection status, never the microphone goroutine.
func (r *Recorder) Wait() error {
	r.mu.Lock()
	done := r.done
	r.mu.Unlock()
	if done != nil {
		<-done
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}
