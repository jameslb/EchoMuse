// Package pairedinput implements an opt-in, lease-bound experimental input link.
// It never opens a microphone, renews a lease, records audio or changes DSP.
package pairedinput

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"
)

const Capability = "phase5_paired_aec_input_v1"
const FrameType byte = 0x0a
const Samples = 512
const HeaderSize = 56
const WireBytes = HeaderSize + Samples*6
const MaxAge = 250 * time.Millisecond

// The existing ALSA subscription delivers five32ms periods per160ms batch.
// Keep at most that single batch; never accumulate a second one.
const QueueLimit = 5

type Identity struct {
	Stream  string `json:"stream"`
	Epoch   uint64 `json:"epoch"`
	Session uint32 `json:"session"`
}
type Command struct {
	Type string `json:"type"`
	Identity
}
type Filter struct {
	Generation             uint64
	Domain, Status, SHA256 string
	Hardware               bool
}
type Block struct {
	Sample    uint64
	Captured  int64
	Residual  [Samples]float32
	Reference [Samples]int16
}
type Ready struct {
	Type string `json:"type"`
	Identity
	Generation uint32 `json:"generation"`
	Domain     string `json:"domain"`
	Status     string `json:"load_status"`
	Bank       string `json:"bank_sha256"`
	Hardware   bool   `json:"hardware_reference"`
	SourceNow  int64  `json:"source_now_ns"`
	Encoding   string `json:"encoding"`
	ClockKind  string `json:"clock_kind"`
}

func Decode(raw []byte) (Command, error) {
	var c Command
	if len(raw) > 512 || json.Unmarshal(raw, &c) != nil {
		return c, errors.New("invalid paired command")
	}
	id, e := hex.DecodeString(c.Stream)
	var nonzero byte
	for _, b := range id {
		nonzero |= b
	}
	if e != nil || len(id) != 16 || nonzero == 0 || c.Epoch == 0 || c.Session == 0 {
		return c, errors.New("invalid paired identity")
	}
	c.Stream = hex.EncodeToString(id)
	if c.Type != "pair_open" && c.Type != "pair_ack" && c.Type != "pair_close" {
		return c, errors.New("invalid paired operation")
	}
	return c, nil
}
func validFilter(f Filter) bool {
	b, e := hex.DecodeString(f.SHA256)
	var nonzero byte
	for _, x := range b {
		nonzero |= x
	}
	return e == nil && len(b) == 32 && nonzero != 0 && f.Generation > 0 && f.Generation <= math.MaxUint32 && f.Domain == "unity_f32_s24" && f.Status == "loaded" && f.Hardware
}

// Lease must read the existing device privacy gate on every operation.
type Lease func(uint32) (time.Time, bool)
type Sender struct {
	sourceClock        func() int64
	mu                 sync.Mutex
	lease              Lease
	identity           Identity
	deadline           time.Time
	open, ready, acked bool
	readyAt            time.Time
	filter             Filter
	queue              []Block
	events             []Ready
	sequence           uint32
	nextSample         uint64
	haveSample         bool
	lastClock          int64
	seen               bool
	lastEpoch          uint64
	lastSession        uint32
}

func New(lease Lease, clock ...func() int64) *Sender {
	s := &Sender{lease: lease}
	if len(clock) > 0 {
		s.sourceClock = clock[0]
	}
	return s
}
func (s *Sender) closeLocked() {
	if !s.open {
		return
	}
	id := s.identity
	s.open = false
	s.ready = false
	s.acked = false
	clear(s.queue)
	s.queue = nil
	s.events = nil
	s.haveSample = false
	s.events = []Ready{{Type: "pair_closed", Identity: id}}
}
func (s *Sender) Close() { s.mu.Lock(); defer s.mu.Unlock(); s.closeLocked() }

// ResetLink retires audio and counters only on an actual link teardown. It
// never opens a new lease or negotiation when a replacement link arrives.
func (s *Sender) ResetLink() { s.mu.Lock(); defer s.mu.Unlock(); s.closeLocked(); s.seen = false }
func (s *Sender) CloseStream(stream string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity.Stream == stream {
		s.closeLocked()
	}
}
func (s *Sender) CloseIdentity(id Identity) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity == id {
		s.closeLocked()
	}
}
func FrameIdentity(frame []byte) (Identity, error) {
	if len(frame) != WireBytes || string(frame[:4]) != "P5I1" {
		return Identity{}, errors.New("invalid paired frame")
	}
	return Identity{hex.EncodeToString(frame[4:20]), binary.BigEndian.Uint64(frame[20:]), binary.BigEndian.Uint32(frame[28:])}, nil
}
func (s *Sender) SendAllowed(id Identity, f Filter, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.allowed(now) || !s.acked || s.identity != id {
		return false
	}
	if f != s.filter {
		s.closeLocked()
		return false
	}
	return true
}
func (s *Sender) allowed(now time.Time) bool {
	if !s.open || s.lease == nil {
		return false
	}
	_, ok := s.lease(s.identity.Session)
	if !ok || !now.Before(s.deadline) || (!s.acked && s.ready && now.Sub(s.readyAt) > 100*time.Millisecond) {
		s.closeLocked()
		return false
	}
	return true
}
func (s *Sender) Control(c Command, now time.Time, secure, negotiated bool, bind ...func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !secure || !negotiated {
		s.closeLocked()
		return errors.New("paired link not authenticated/negotiated")
	}
	if c.Type == "pair_open" {
		if s.open && s.allowed(now) {
			return errors.New("paired link busy")
		}
		if s.seen && (c.Epoch < s.lastEpoch || c.Epoch == s.lastEpoch && c.Session <= s.lastSession) {
			return errors.New("retired negotiation")
		}
		if s.lease == nil {
			return errors.New("no lease reader")
		}
		deadline, ok := s.lease(c.Session)
		if !ok || !now.Before(deadline) || deadline.Sub(now) > 30*time.Second {
			return errors.New("no existing bounded lease")
		}
		s.closeLocked()
		s.identity = c.Identity
		s.events = nil
		if len(bind) > 0 && bind[0] != nil {
			bind[0]()
		} // install link/mic ownership before Capture can observe open
		s.deadline = deadline
		s.open = true
		s.sequence = 0
		s.lastClock = -1
		s.seen = true
		s.lastEpoch = c.Epoch
		s.lastSession = c.Session
		return nil
	}
	if !s.open || c.Identity != s.identity {
		return errors.New("retired paired command")
	}
	if c.Type == "pair_close" {
		s.closeLocked()
		return nil
	}
	if c.Type != "pair_ack" || !s.allowed(now) || !s.ready {
		return errors.New("unready paired link")
	}
	s.acked = true
	return nil
}
func (s *Sender) Capture(b Block, f Filter, now time.Time, sourceNow int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sourceClock != nil {
		sourceNow = s.sourceClock()
	}
	if !s.allowed(now) {
		return nil
	}
	if !validFilter(f) {
		s.closeLocked()
		return errors.New("unready AEC filter")
	}
	if sourceNow < s.lastClock || b.Captured < 0 || sourceNow < b.Captured || sourceNow-b.Captured > int64(MaxAge) {
		s.closeLocked()
		return errors.New("invalid source clock")
	}
	s.lastClock = sourceNow
	if s.haveSample && b.Sample != s.nextSample {
		s.closeLocked()
		return errors.New("paired source gap")
	}
	if b.Sample > math.MaxUint64-Samples {
		s.closeLocked()
		return errors.New("source counter overflow")
	}
	for _, x := range b.Residual {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			s.closeLocked()
			return errors.New("nonfinite residual")
		}
	}
	if s.ready && f != s.filter {
		s.closeLocked()
		return errors.New("filter changed")
	}
	s.nextSample = b.Sample + Samples
	s.haveSample = true
	if !s.ready {
		s.filter = f
		s.ready = true
		s.readyAt = now
		s.events = append(s.events, Ready{"pair_ready", s.identity, uint32(f.Generation), f.Domain, f.Status, f.SHA256, f.Hardware, sourceNow, "unity_f32_from_gained_s16_v1", "mic_observation_v1"})
	}
	if len(s.queue) == QueueLimit {
		if s.acked {
			s.closeLocked()
			return errors.New("paired queue overflow")
		}
		// Bounded unpublished preroll only. Sequence numbering starts on delivery.
		copy(s.queue, s.queue[1:])
		s.queue = s.queue[:QueueLimit-1]
	}
	s.queue = append(s.queue, b)
	return nil
}
func (s *Sender) Event(now time.Time) (Ready, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowed(now) // may enqueue the single truthful closure event
	if len(s.events) == 0 {
		return Ready{}, false
	}
	e := s.events[0]
	s.events = s.events[1:]
	return e, true
}

// FromGainedPCM preserves the declared quantized provenance. It cannot recover
// samples already clipped before transport; this is not the original ADC float.
func FromGainedPCM(mono, reference []byte, gain float64, sample uint64, observed int64) (Block, error) {
	b := Block{Sample: sample, Captured: observed}
	if len(mono) != Samples*2 || len(reference) != Samples*2 || gain <= 0 || math.IsNaN(gain) || math.IsInf(gain, 0) {
		return b, errors.New("invalid gained input")
	}
	for i := range b.Residual {
		b.Residual[i] = float32(float64(int16(binary.LittleEndian.Uint16(mono[i*2:]))) / gain)
		b.Reference[i] = int16(binary.LittleEndian.Uint16(reference[i*2:]))
	}
	return b, nil
}
func (s *Sender) Poll(now time.Time, sourceNow int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sourceClock != nil {
		sourceNow = s.sourceClock()
	}
	if !s.allowed(now) || !s.acked || len(s.queue) == 0 {
		return nil, nil
	}
	b := s.queue[0]
	if sourceNow < s.lastClock || sourceNow < b.Captured || sourceNow-b.Captured > int64(MaxAge) {
		s.closeLocked()
		return nil, errors.New("stale paired queue")
	}
	s.lastClock = sourceNow
	if s.sequence == math.MaxUint32 {
		s.closeLocked()
		return nil, errors.New("sequence exhausted")
	}
	frame := make([]byte, WireBytes)
	copy(frame, "P5I1")
	id, _ := hex.DecodeString(s.identity.Stream)
	copy(frame[4:20], id)
	binary.BigEndian.PutUint64(frame[20:], s.identity.Epoch)
	binary.BigEndian.PutUint32(frame[28:], s.identity.Session)
	binary.BigEndian.PutUint32(frame[32:], s.sequence)
	binary.BigEndian.PutUint64(frame[36:], b.Sample)
	binary.BigEndian.PutUint64(frame[44:], uint64(b.Captured))
	binary.BigEndian.PutUint32(frame[52:], uint32(s.filter.Generation))
	for i, x := range b.Residual {
		binary.LittleEndian.PutUint32(frame[56+i*4:], math.Float32bits(x))
	}
	for i, x := range b.Reference {
		binary.LittleEndian.PutUint16(frame[56+Samples*4+i*2:], uint16(x))
	}
	s.sequence++
	s.queue[0] = Block{}
	s.queue = s.queue[1:]
	return frame, nil
}
