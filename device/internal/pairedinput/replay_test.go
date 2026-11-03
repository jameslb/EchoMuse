package pairedinput

// Optional saved-input fixture test. Never opens a device/socket or records audio.
import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetainedPairedInput(t *testing.T) {
	capture, inputs, out := os.Getenv("PAIR_CAPTURE_ROOT"), os.Getenv("PAIR_INPUT_ROOT"), os.Getenv("PAIR_FIXTURE_DIR")
	if capture == "" || inputs == "" || out == "" {
		t.Skip("saved capture not supplied")
	}
	diag, e := os.ReadFile(filepath.Join(capture, "aec.local/aec.s16le"))
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(capture, "aec.local/events.local.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	start := -1
	filter := Filter{}
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		var r struct {
			Offset     int    `json:"pcm_sample_offset"`
			Hardware   bool   `json:"hardware_reference_selected"`
			Generation uint64 `json:"filter_generation"`
			Domain     string `json:"filter_domain"`
			Status     string `json:"filter_load_status"`
			SHA        string `json:"filter_load_sha256"`
		}
		if e = json.Unmarshal(line, &r); e != nil {
			t.Fatal(e)
		}
		if r.Hardware && start < 0 {
			start = r.Offset
			filter = Filter{r.Generation, r.Domain, r.Status, r.SHA, true}
		}
	}
	if start < 0 || !validFilter(filter) || len(diag) != 320000*6 {
		t.Fatal("invalid saved fixture")
	}
	for _, mode := range []string{"echo", "overlap", "quiet-overlap", "near-only"} {
		pcm, e := os.ReadFile(filepath.Join(inputs, mode+".s16le"))
		if e != nil {
			t.Fatal(e)
		}
		if len(pcm) != 640000 {
			t.Fatal("invalid PCM size")
		}
		base := time.Unix(100, 0)
		clock := base
		s := New(func(uint32) (time.Time, bool) { return base.Add(20 * time.Second), true })
		if e = s.Control(Command{"pair_open", testID}, base, true, true); e != nil {
			t.Fatal(e)
		}
		var wire bytes.Buffer
		var ready Ready
		seq := 0
		for off := start; off < 320000; off += Samples * 5 {
			stamp := int64(1_000_000_000) + int64(seq)*160_000_000
			clock = base.Add(time.Duration(seq) * 160 * time.Millisecond)
			for i := 0; i < 5; i++ {
				sample := off + i*Samples
				ref := make([]byte, Samples*2)
				for j := 0; j < Samples; j++ {
					copy(ref[j*2:], diag[(sample+j)*6+4:(sample+j)*6+6])
				}
				b, err := FromGainedPCM(pcm[sample*2:(sample+Samples)*2], ref, math.Pow(10, 27./20), uint64(sample), stamp)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Capture(b, filter, clock, stamp); err != nil {
					t.Fatal(err)
				}
			}
			if seq == 0 {
				var ok bool
				ready, ok = s.Event(clock)
				if !ok {
					t.Fatal("no ready")
				}
				if e = s.Control(Command{"pair_ack", testID}, clock.Add(time.Millisecond), true, true); e != nil {
					t.Fatal(e)
				}
			}
			for i := 0; i < 5; i++ {
				packet, err := s.Poll(clock.Add(time.Millisecond), stamp+1_000_000)
				if err != nil || packet == nil {
					t.Fatal(err)
				}
				wire.Write(packet)
			}
			seq++
		}
		if wire.Len() != 440*WireBytes {
			t.Fatal("incomplete fixture", wire.Len())
		}
		payload, _ := json.Marshal(ready)
		if e = os.WriteFile(filepath.Join(out, mode+"-ready.json"), payload, 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(out, mode+"-frames.bin"), wire.Bytes(), 0600); e != nil {
			t.Fatal(e)
		}
		s.Close()
	}
}
