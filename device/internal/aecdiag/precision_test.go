package aecdiag

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func precisePermit(t *testing.T, seconds int) string {
	root := permit(t, seconds)
	p := Permit{true, "one-dot-aec-fullprecision", time.Now().Unix() + 120, seconds}
	b, _ := json.Marshal(p)
	if err := os.WriteFile(filepath.Join(root, "permit.local.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
func preciseMeta() Meta {
	m := meta()
	m.InputFormat = "unity_f32_s24"
	m.FilterGeneration = 2
	m.FilterDomain = "unity_f32_s24"
	m.FilterLoadStatus = "missing"
	return m
}
func microphones(n int) [][]float32 {
	m := make([][]float32, 7)
	for ch := range m {
		m[ch] = make([]float32, n)
		for i := range m[ch] {
			m[ch][i] = float32(ch) + float32(i)/256
		}
	}
	return m
}
func TestFullPrecisionRequiresSeparatePermitAndPreservesFractionalAllMics(t *testing.T) {
	for _, full := range []bool{false, true} {
		root := permit(t, 20)
		if full {
			root = precisePermit(t, 20)
		}
		r := start(t, root, time.Now().Add(time.Minute))
		if r.PrecisionActive() != full {
			t.Fatal("wrong scope")
		}
		input := microphones(3)
		pcm := make([]byte, 6)
		r.Capture(preciseMeta(), pcm, pcm, pcm, pcm, input)
		input[0][1] = 999 // Recorder must copy, never retain mutable audio arrays.
		r.Stop("test")
		if err := r.Wait(); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(root, "microphones.f32le"))
		if !full {
			if !os.IsNotExist(err) || result(t, root)["version"] != float64(2) {
				t.Fatal("old permit expanded recording")
			}
			continue
		}
		if err != nil || len(raw) != 3*7*4 {
			t.Fatal("wrong full precision length", err)
		}
		got := math.Float32frombits(binary.LittleEndian.Uint32(raw[7*4:]))
		if got != 1.0/256 {
			t.Fatal("fractional precision lost or input mutated", got)
		}
		manifest := result(t, root)
		if manifest["version"] != float64(3) || manifest["initial_signal_history_captured"] != false {
			t.Fatal("misleading replay claim")
		}
	}
}
func TestFullPrecisionRejectsMissingMalformedAndNonfiniteInput(t *testing.T) {
	for _, kind := range []string{"missing", "short", "nan", "range", "provenance"} {
		t.Run(kind, func(t *testing.T) {
			root := precisePermit(t, 20)
			r := start(t, root, time.Now().Add(time.Minute))
			m := preciseMeta()
			input := microphones(2)
			pcm := make([]byte, 4)
			switch kind {
			case "missing":
				input = nil
			case "short":
				input[3] = input[3][:1]
			case "nan":
				input[2][0] = float32(math.NaN())
			case "range":
				input[2][0] = 32768
			case "provenance":
				m.FilterGeneration = 0
			}
			r.Capture(m, pcm, pcm, pcm, pcm, input)
			r.Wait()
			if r.Active() || result(t, root)["samples"] != float64(0) {
				t.Fatal("invalid input accepted")
			}
		})
	}
}
func TestFullPrecisionMaximumBytesAndProcessFuse(t *testing.T) {
	root := precisePermit(t, 1)
	r := start(t, root, time.Now().Add(time.Minute))
	pcm := make([]byte, 8192)
	input := microphones(4096)
	for i := 0; i < 4; i++ {
		r.Capture(preciseMeta(), pcm, pcm, pcm, pcm, input)
	}
	if r.Active() {
		t.Fatal("sample cap")
	}
	r.Wait()
	raw, err := os.ReadFile(filepath.Join(root, "microphones.f32le"))
	if err != nil || len(raw) != 16000*28 {
		t.Fatal("unbounded precision bytes", len(raw), err)
	}
	if err := r.Start(precisePermit(t, 1), 8, [16]byte{2}, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("process fuse reset")
	}
}
