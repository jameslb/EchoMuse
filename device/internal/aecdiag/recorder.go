// Package aecdiag provides an opt-in, one-session AEC diagnostic. It never
// opens a microphone, sends audio, renews a lease or changes DSP decisions.
package aecdiag

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const Root = "/data/local/tmp/phase5-aec.local"
const MaxSamples = 20 * 16000
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
	Stream          uint64 `json:"mic_stream"`
	Batch           uint64 `json:"batch"`
	SampleStart     uint64 `json:"sample_start"`
	ReceivedNS      int64  `json:"received_mono_ns"`
	Selected        int    `json:"selected_channel"`
	Clipped         uint64 `json:"clipped_total"`
	GainDB          int    `json:"gain_db"`
	AEC             bool   `json:"aec_enabled"`
	Hardware        bool   `json:"hardware_reference_selected"`
	RefValid        bool   `json:"reference_valid"`
	Samples         int    `json:"samples"`
	Offset          int    `json:"pcm_sample_offset"`
	PostDSPHash     string `json:"post_dsp_sha256"`
	ArrivalExcessNS int64  `json:"arrival_excess_ns"`
	SampleGap       bool   `json:"sample_gap"`
}

type Recorder struct {
	mu        sync.Mutex
	active    atomic.Bool
	used      bool
	session   uint32
	stream    [16]byte
	deadline  time.Time
	startedNS int64
	root      string
	pcm       []byte
	rows      []Meta
	limit     int
	timer     *time.Timer
	done      chan struct{}
	err       error
}

func (r *Recorder) Active() bool    { return r.active.Load() }
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
	if dec.Decode(&p) != nil || dec.Decode(new(any)) != io.EOF || !p.Approved || p.Scope != "one-dot-aec-uplink" || p.MaximumSeconds < 1 || p.MaximumSeconds > 20 || p.ExpiresAt <= now.Unix() || p.ExpiresAt > now.Unix()+300 {
		return errors.New("invalid or expired diagnostic permit")
	}
	if err = os.Rename(path, filepath.Join(root, "permit.used.local.json")); err != nil {
		return err
	}
	r.used = true
	r.root, r.session, r.stream = root, session, stream
	r.limit = p.MaximumSeconds * 16000
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
func (r *Recorder) Capture(m Meta, pre, post, ref, dsp []byte) {
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
	pcm, rows := r.pcm, r.rows
	r.pcm, r.rows = nil, nil
	ended := Stamp()
	// Only this single bounded write can be outstanding, even if disk stalls.
	go func() {
		err := r.save(pcm, rows, reason, ended)
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
func (r *Recorder) save(pcm []byte, rows []Meta, reason string, ended int64) error {
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
	summary := map[string]any{"version": 1, "session": r.session, "stream": hex.EncodeToString(r.stream[:]), "rate": 16000, "channels": []string{"selected_pre_aec", "selected_post_aec", "hardware_reference"}, "samples": len(pcm) / 6, "batches": len(rows), "started_mono_ns": r.startedNS, "ended_mono_ns": ended, "reason": reason, "pcm_sha256": hex.EncodeToString(ph[:]), "events_sha256": hex.EncodeToString(eh[:]), "physical_acceptance": false}
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
