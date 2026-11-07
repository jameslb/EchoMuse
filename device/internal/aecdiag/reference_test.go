package aecdiag

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func referencePermit(t *testing.T) string {
	root := precisePermit(t, 20)
	b, _ := json.Marshal(Permit{true, ReferenceScope, time.Now().Unix() + 120, 20})
	if err := os.WriteFile(filepath.Join(root, "permit.local.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
func TestReferenceScopeCopiesBothLoopbacksWithHash(t *testing.T) {
	root := referencePermit(t)
	r := start(t, root, time.Now().Add(time.Minute))
	if !r.ReferenceActive() || !r.PrecisionActive() {
		t.Fatal("missing capability")
	}
	loops := [][]float32{{-32768, -1.0 / 256}, {32767.996, 1.0 / 256}}
	pcm := make([]byte, 4)
	r.Capture(preciseMeta(), pcm, pcm, pcm, pcm, microphones(2), loops)
	loops[0][1] = 123
	r.Stop("test")
	if err := r.Wait(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "loopbacks.f32le"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 16 || math.Float32frombits(binary.LittleEndian.Uint32(raw[8:])) != -1.0/256 {
		t.Fatal("wrong channel/sample/copy")
	}
	sum := sha256.Sum256(raw)
	m := result(t, root)
	if m["version"] != float64(4) || m["scope"] != ReferenceScope || m["loopback_sha256"] != hex.EncodeToString(sum[:]) {
		t.Fatal("invalid manifest")
	}
	channels := m["loopback_channels"].([]any)
	if len(channels) != 2 || channels[0] != float64(7) || channels[1] != float64(8) {
		t.Fatal("wrong channel map")
	}
	if r.loopbacks != nil {
		t.Fatal("retained buffer after stop")
	}
}
func TestReferenceScopeRejectsMalformedAndStaleInput(t *testing.T) {
	for _, kind := range []string{"missing", "one_channel", "short", "nan", "range", "stale"} {
		t.Run(kind, func(t *testing.T) {
			root := referencePermit(t)
			r := start(t, root, time.Now().Add(time.Minute))
			loops := [][]float32{{1, 2}, {3, 4}}
			m := preciseMeta()
			pcm := make([]byte, 4)
			switch kind {
			case "one_channel":
				loops = loops[:1]
			case "short":
				loops[1] = loops[1][:1]
			case "nan":
				loops[0][0] = float32(math.NaN())
			case "range":
				loops[1][0] = 32768
			case "stale":
				m.ReceivedNS = r.startedNS - 1
			}
			if kind == "missing" {
				r.Capture(m, pcm, pcm, pcm, pcm, microphones(2))
			} else {
				r.Capture(m, pcm, pcm, pcm, pcm, microphones(2), loops)
			}
			r.Stop("test")
			if err := r.Wait(); err != nil {
				t.Fatal(err)
			}
			if result(t, root)["samples"] != float64(0) {
				t.Fatal("retained invalid or pre-permit PCM")
			}
		})
	}
}
func TestOldPermitNeverRecordsLoopbacks(t *testing.T) {
	for _, precision := range []bool{false, true} {
		root := permit(t, 20)
		if precision {
			root = precisePermit(t, 20)
		}
		r := start(t, root, time.Now().Add(time.Minute))
		pcm := make([]byte, 4)
		r.Capture(preciseMeta(), pcm, pcm, pcm, pcm, microphones(2), [][]float32{{1, 2}, {3, 4}})
		r.Stop("test")
		if err := r.Wait(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(root, "loopbacks.f32le")); !os.IsNotExist(err) {
			t.Fatal("old consent expanded")
		}
		if r.ReferenceActive() {
			t.Fatal("old permit advertises expanded capability")
		}
	}
}

func TestReferenceSampleLimitAndSessionStop(t *testing.T) {
	for _, mode := range []string{"limit", "session", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			root := referencePermit(t)
			b, _ := json.Marshal(Permit{true, ReferenceScope, time.Now().Unix() + 120, 1})
			if err := os.WriteFile(filepath.Join(root, "permit.local.json"), b, 0600); err != nil {
				t.Fatal(err)
			}
			r := start(t, root, time.Now().Add(time.Minute))
			pcm := make([]byte, 5120)
			mics := microphones(2560)
			loops := [][]float32{make([]float32, 2560), make([]float32, 2560)}
			if mode == "session" {
				r.StopFor(r.stream)
			}
			if mode == "deadline" {
				r.mu.Lock()
				r.deadline = time.Now().Add(-time.Second)
				r.mu.Unlock()
			}
			for i := 0; i < 8; i++ {
				r.Capture(preciseMeta(), pcm, pcm, pcm, pcm, mics, loops)
			}
			r.Stop("test")
			if err := r.Wait(); err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "limit" {
				want = 16000
			}
			if result(t, root)["samples"] != float64(want) {
				t.Fatal("wrong sample cap")
			}
			raw, err := os.ReadFile(filepath.Join(root, "loopbacks.f32le"))
			if err != nil || len(raw) != want*8 {
				t.Fatal("wrong sidecar bounds", err)
			}
			if r.Active() || r.ReferenceActive() {
				t.Fatal("still armed")
			}
		})
	}
}
