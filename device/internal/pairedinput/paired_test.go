package pairedinput

import (
	"encoding/binary"
	"encoding/json"
	"github.com/wilbowes/EchoMuse/internal/listen"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testID = Identity{strings.Repeat("31", 16), 7, 9}

func fixture(t *testing.T) (*Sender, time.Time, Filter) {
	t.Helper()
	now := time.Unix(100, 0)
	s := New(func(id uint32) (time.Time, bool) { return now.Add(30 * time.Second), id == 9 })
	if e := s.Control(Command{"pair_open", testID}, now, true, true); e != nil {
		t.Fatal(e)
	}
	return s, now, Filter{3, "unity_f32_s24", "loaded", strings.Repeat("42", 32), true}
}
func block(sample uint64) Block {
	b := Block{Sample: sample, Captured: 1_000_000}
	b.Residual[0] = .25
	b.Residual[511] = -.5
	b.Reference[0] = -123
	b.Reference[511] = 32767
	return b
}
func ack(t *testing.T, s *Sender, now time.Time) {
	t.Helper()
	if e := s.Control(Command{"pair_ack", testID}, now, true, true); e != nil {
		t.Fatal(e)
	}
}
func TestWireAndNoAudioBeforeAck(t *testing.T) {
	s, now, f := fixture(t)
	if e := s.Capture(block(1234), f, now, 1_000_000); e != nil {
		t.Fatal(e)
	}
	if b, _ := s.Poll(now, 1_000_000); b != nil {
		t.Fatal("audio before ACK")
	}
	ready, ok := s.Event(now)
	if !ok || ready.Type != "pair_ready" {
		t.Fatal("missing ready")
	}
	ack(t, s, now)
	wire, e := s.Poll(now, 1_000_000)
	if e != nil || len(wire) != WireBytes {
		t.Fatal(e, len(wire))
	}
	if binary.BigEndian.Uint64(wire[36:]) != 1234 || binary.LittleEndian.Uint16(wire[56+Samples*4:]) != 65413 {
		t.Fatal("wire offsets")
	}
	if dir := os.Getenv("PAIR_FIXTURE_DIR"); dir != "" {
		raw, _ := json.Marshal(ready)
		if e = os.WriteFile(filepath.Join(dir, "ready.json"), raw, 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "frame.bin"), wire, 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func TestExistingLeaseCannotBeCreatedOrRenewed(t *testing.T) {
	now := time.Unix(100, 0)
	clock := now
	g := listen.New(0, 0, time.Second)
	s := New(func(id uint32) (time.Time, bool) { return g.Lease(id, clock) })
	if s.Control(Command{"pair_open", testID}, now, true, true) == nil {
		t.Fatal("fabricated lease")
	}
	id, ok := g.Open(now, now)
	if !ok {
		t.Fatal("open fixture")
	}
	g.Ack(id)
	identity := testID
	identity.Session = id
	if e := s.Control(Command{"pair_open", identity}, now, true, true); e != nil {
		t.Fatal(e)
	}
	clock = now.Add(time.Second)
	if b, _ := s.Poll(clock, 1_000_000); b != nil || s.open {
		t.Fatal("lease extended")
	}
}
func TestAuthenticationAndReadiness(t *testing.T) {
	s, now, f := fixture(t)
	s.Close()
	for _, flags := range [][2]bool{{false, true}, {true, false}} {
		if s.Control(Command{"pair_open", testID}, now, flags[0], flags[1]) == nil {
			t.Fatal("negotiation bypass")
		}
	}
	for _, status := range []string{"missing", "refused", "unknown"} {
		s, now, f = fixture(t)
		f.Status = status
		if s.Capture(block(0), f, now, 1_000_000) == nil || s.open {
			t.Fatal("cold accepted")
		}
	}
}
func TestNativeBatchPrerollIsBoundedAndUnpublished(t *testing.T) {
	s, now, f := fixture(t)
	for i := 0; i < QueueLimit+1; i++ {
		if e := s.Capture(block(uint64(i*Samples)), f, now, 1_000_000); e != nil {
			t.Fatal(e)
		}
	}
	if len(s.queue) != QueueLimit {
		t.Fatal("unbounded pending input")
	}
	ack(t, s, now)
	wire, e := s.Poll(now, 1_000_000)
	if e != nil || binary.BigEndian.Uint32(wire[32:]) != 0 || binary.BigEndian.Uint64(wire[36:]) != Samples {
		t.Fatal("bad initial anchor")
	}
}
func TestActiveOverflowAndGapPurge(t *testing.T) {
	for _, mode := range []string{"overflow", "gap"} {
		s, now, f := fixture(t)
		s.Capture(block(0), f, now, 1_000_000)
		ack(t, s, now)
		if mode == "gap" {
			if s.Capture(block(2*Samples), f, now, 1_000_000) == nil {
				t.Fatal("gap accepted")
			}
		} else {
			for i := 1; i <= QueueLimit; i++ {
				s.Capture(block(uint64(i*Samples)), f, now, 1_000_000)
			}
		}
		if s.open || len(s.queue) != 0 {
			t.Fatal("failed to purge", mode)
		}
	}
}
func TestPrivacyIsRecheckedAfterPollBeforeSend(t *testing.T) {
	s, now, f := fixture(t)
	s.Capture(block(0), f, now, 1_000_000)
	ack(t, s, now)
	wire, _ := s.Poll(now, 1_000_000)
	if wire == nil {
		t.Fatal("no fixture packet")
	}
	s.Close()
	if s.SendAllowed(testID, f, now) {
		t.Fatal("retired in-flight frame allowed")
	}
	s, now, f = fixture(t)
	s.Capture(block(0), f, now, 1_000_000)
	ack(t, s, now)
	changed := f
	changed.Generation++
	if s.SendAllowed(testID, changed, now) || s.open {
		t.Fatal("changed filter allowed")
	}
}
func TestClockAndAckDeadline(t *testing.T) {
	s, now, f := fixture(t)
	s.Capture(block(0), f, now, 1_000_000)
	if s.Control(Command{"pair_ack", testID}, now.Add(101*time.Millisecond), true, true) == nil || s.open {
		t.Fatal("late ack")
	}
	s, now, f = fixture(t)
	s.Capture(block(0), f, now, 1_000_000)
	ack(t, s, now)
	if _, e := s.Poll(now, 0); e == nil || s.open {
		t.Fatal("clock rollback")
	}
}
func TestForeignCloseCannotCloseCurrentStream(t *testing.T) {
	s, _, _ := fixture(t)
	s.CloseStream("foreign")
	if !s.open {
		t.Fatal("foreign close")
	}
	s.CloseStream(testID.Stream)
	if s.open {
		t.Fatal("close ignored")
	}
}
func TestDecodeBounds(t *testing.T) {
	for _, raw := range []string{`{}`, strings.Repeat("x", 513), `{"type":"mic_start"}`} {
		if _, e := Decode([]byte(raw)); e == nil {
			t.Fatal("invalid command")
		}
	}
}

func TestRetiredPacketCannotUseNewNegotiation(t *testing.T) {
	s, now, f := fixture(t)
	s.Capture(block(0), f, now, 1_000_000)
	ack(t, s, now)
	old, _ := s.Poll(now, 1_000_000)
	id, _ := FrameIdentity(old)
	s.Close()
	if s.Control(Command{"pair_open", testID}, now, true, true) == nil {
		t.Fatal("retired identity reopened")
	}
	next := testID
	next.Epoch++
	next.Stream = strings.Repeat("32", 16)
	if e := s.Control(Command{"pair_open", next}, now, true, true); e != nil {
		t.Fatal(e)
	}
	s.Capture(block(512), f, now, 1_000_000)
	if e := s.Control(Command{"pair_ack", next}, now, true, true); e != nil {
		t.Fatal(e)
	}
	if s.SendAllowed(id, f, now) {
		t.Fatal("old packet authorised by new handshake")
	}
	s.CloseIdentity(id)
	if !s.open {
		t.Fatal("old packet poisoned new context")
	}
}
func TestClosureReceiptAndGainedPrecision(t *testing.T) {
	s, now, _ := fixture(t)
	s.Close()
	event, ok := s.Event(now)
	if !ok || event.Type != "pair_closed" {
		t.Fatal("missing closure")
	}
	if _, ok = s.Event(now); ok {
		t.Fatal("duplicate closure")
	}
	pcm := make([]byte, 1024)
	ref := make([]byte, 1024)
	binary.LittleEndian.PutUint16(pcm, 25)
	binary.LittleEndian.PutUint16(ref, 65413)
	for _, gain := range []float64{1, 10, 100} {
		b, e := FromGainedPCM(pcm, ref, gain, 123, 7)
		if e != nil || b.Residual[0] != float32(25/gain) || b.Reference[0] != -123 {
			t.Fatal("gain/reference")
		}
	}
	if _, e := FromGainedPCM(pcm, ref, 0, 0, 0); e == nil {
		t.Fatal("invalid gain")
	}
}
func TestSourceClockIsReadAfterMutexAcquisition(t *testing.T) {
	_, now, f := fixture(t)
	s := New(func(id uint32) (time.Time, bool) { return now.Add(time.Second), true }, func() int64 { return 1_000_000 })
	if e := s.Control(Command{"pair_open", testID}, now, true, true); e != nil {
		t.Fatal(e)
	}
	if e := s.Capture(block(0), f, now, 9_000_000_000); e != nil {
		t.Fatal("used pre-lock clock", e)
	}
	ack(t, s, now)
	wire, e := s.Poll(now, 0)
	if e != nil || wire == nil {
		t.Fatal("used pre-lock poll clock", e)
	}
}
func TestLinkResetDoesNotRearmAndAllowsNewControllerEpoch(t *testing.T) {
	now := time.Unix(100, 0)
	leased := uint32(9)
	s := New(func(id uint32) (time.Time, bool) { return now.Add(time.Second), id == leased })
	if e := s.Control(Command{"pair_open", testID}, now, true, true); e != nil {
		t.Fatal(e)
	}
	s.ResetLink()
	leased = 0
	if s.Control(Command{"pair_open", testID}, now, true, true) == nil {
		t.Fatal("old lease revived")
	}
	if wire, _ := s.Poll(now, 1); wire != nil {
		t.Fatal("automatic rearm")
	}
	leased = 10
	next := Identity{strings.Repeat("32", 16), 1, 10}
	if e := s.Control(Command{"pair_open", next}, now, true, true); e != nil {
		t.Fatal("fresh controller cannot negotiate", e)
	}
}
