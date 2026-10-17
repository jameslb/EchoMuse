package aecdiag

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func permit(t *testing.T, seconds int) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	p := Permit{true, "one-dot-aec-uplink", time.Now().Unix() + 120, seconds}
	raw, _ := json.Marshal(p)
	if err := os.WriteFile(filepath.Join(root, "permit.local.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
func start(t *testing.T, root string, lease time.Time) *Recorder {
	t.Helper()
	r := new(Recorder)
	if e := r.Start(root, 7, [16]byte{1}, lease); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		r.Stop("test_end")
		if e := r.Wait(); e != nil {
			t.Error(e)
		}
	})
	return r
}
func meta() Meta { return Meta{Stream: 1, ReceivedNS: Stamp(), Selected: 6, AEC: true, Hardware: true} }
func result(t *testing.T, root string) map[string]any {
	t.Helper()
	raw, e := os.ReadFile(filepath.Join(root, "capture.local.json"))
	if e != nil {
		t.Fatal(e)
	}
	var m map[string]any
	if e = json.Unmarshal(raw, &m); e != nil {
		t.Fatal(e)
	}
	return m
}

func TestPermitRequiredAndStrict(t *testing.T) {
	for _, kind := range []string{"missing", "expired", "future", "scope", "unapproved", "long", "extra", "symlink", "evidence", "lease"} {
		t.Run(kind, func(t *testing.T) {
			root := permit(t, 20)
			path := filepath.Join(root, "permit.local.json")
			lease := time.Now().Add(time.Minute)
			p := Permit{true, "one-dot-aec-uplink", time.Now().Unix() + 120, 20}
			switch kind {
			case "missing":
				os.Remove(path)
			case "expired":
				p.ExpiresAt = time.Now().Unix() - 1
			case "future":
				p.ExpiresAt = time.Now().Unix() + 301
			case "scope":
				p.Scope = "wrong"
			case "unapproved":
				p.Approved = false
			case "long":
				p.MaximumSeconds = 21
			case "lease":
				lease = time.Now().Add(-time.Second)
			case "evidence":
				os.WriteFile(filepath.Join(root, "aec.s16le"), []byte{1}, 0600)
			case "symlink":
				os.Rename(path, path+".target")
				os.Symlink(path+".target", path)
			}
			if kind != "missing" && kind != "symlink" {
				raw, _ := json.Marshal(p)
				if kind == "extra" {
					raw = append(raw, []byte(" {}")...)
				}
				os.WriteFile(path, raw, 0600)
			}
			var r Recorder
			if e := r.Start(root, 7, [16]byte{1}, lease); e == nil {
				t.Fatal("invalid permit/lease enabled capture")
			}
			r.Capture(meta(), []byte{1, 2}, []byte{3, 4}, nil, []byte{5, 6})
			if r.Active() || r.pcm != nil {
				t.Fatal("retained unapproved PCM")
			}
		})
	}
}

func TestThreeChannelsHashesAndNoMutation(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(time.Minute))
	pre, post, ref, dsp := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, []byte{9, 10, 11, 12}, []byte{13, 14, 15, 16}
	r.Capture(meta(), pre, post, ref, dsp)
	pre[0] = 99 // retaining caller buffers would corrupt the captured bytes
	r.Stop("test")
	if e := r.Wait(); e != nil {
		t.Fatal(e)
	}
	got, _ := os.ReadFile(filepath.Join(root, "aec.s16le"))
	if !bytes.Equal(got, []byte{1, 2, 5, 6, 9, 10, 3, 4, 7, 8, 11, 12}) {
		t.Fatalf("channels: %v", got)
	}
	if !bytes.Equal(post, []byte{5, 6, 7, 8}) || !bytes.Equal(dsp, []byte{13, 14, 15, 16}) {
		t.Fatal("DSP input changed")
	}
	events, _ := os.ReadFile(filepath.Join(root, "events.local.jsonl"))
	var m Meta
	json.Unmarshal(bytes.TrimSpace(events), &m)
	sum := sha256.Sum256(dsp)
	if m.PostDSPHash != hex.EncodeToString(sum[:]) || !m.RefValid || m.Samples != 2 {
		t.Fatal(m)
	}
	if result(t, root)["physical_acceptance"] != false {
		t.Fatal("claimed acceptance")
	}
	if _, e := os.Stat(filepath.Join(root, "permit.local.json")); !os.IsNotExist(e) {
		t.Fatal("permit not consumed")
	}
	if e := r.Start(root, 7, [16]byte{1}, time.Now().Add(time.Minute)); e == nil {
		t.Fatal("rearmed")
	}
	var restarted Recorder
	if e := restarted.Start(root, 7, [16]byte{1}, time.Now().Add(time.Minute)); e == nil {
		t.Fatal("replayed after restart")
	}
}

func TestSampleLimitAndMissingReference(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(time.Minute))
	b := make([]byte, 8192)
	for i := 0; i < 100; i++ {
		m := meta()
		m.Batch = uint64(i)
		m.SampleStart = uint64(i * 4096)
		r.Capture(m, b, b, nil, b)
	}
	if r.Active() {
		t.Fatal("sample cap failed")
	}
	if e := r.Wait(); e != nil {
		t.Fatal(e)
	}
	got, _ := os.ReadFile(filepath.Join(root, "aec.s16le"))
	if len(got) != 1920000 {
		t.Fatal(len(got))
	}
	raw, _ := os.ReadFile(filepath.Join(root, "events.local.jsonl"))
	var m Meta
	json.Unmarshal(bytes.Split(raw, []byte{'\n'})[0], &m)
	if m.RefValid {
		t.Fatal("missing reference called valid")
	}
	if result(t, root)["reason"] != "sample_limit" {
		t.Fatal("wrong close reason")
	}
}

func TestDeadlineWithoutIncomingAudio(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(30*time.Millisecond))
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("wall deadline needs PCM")
	}
	if r.Active() {
		t.Fatal("still recording")
	}
	if result(t, root)["samples"] != float64(0) {
		t.Fatal("unexpected PCM")
	}
}

func TestCloseAndOldFrames(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(time.Minute))
	b := []byte{1, 2}
	m := meta()
	m.ReceivedNS = 0
	r.Capture(m, b, b, b, b)
	r.StopFor([16]byte{2})
	if !r.Active() {
		t.Fatal("stale stream stopped session")
	}
	m = meta()
	r.Capture(m, b, b, b, b)
	r.StopFor([16]byte{1})
	r.Capture(m, b, b, b, b)
	r.Wait()
	if result(t, root)["samples"] != float64(1) {
		t.Fatal("pre-permit or post-close audio retained")
	}
}

func TestConcurrentCaptureAndStop(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(time.Minute))
	var wg sync.WaitGroup
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				b := []byte{1, 2}
				r.Capture(meta(), b, b, b, b)
			}
		}()
	}
	r.Stop("mute")
	wg.Wait()
	if e := r.Wait(); e != nil {
		t.Fatal(e)
	}
	if r.Active() {
		t.Fatal("active after mute")
	}
}

func TestMetadataLimitAndGaps(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(time.Minute))
	b := []byte{1, 2}
	for i := 0; i < maxBatches+1; i++ {
		m := meta()
		m.Batch = uint64(i * 2)
		m.SampleStart = uint64(i * 2)
		r.Capture(m, b, b, b, b)
	}
	r.Wait()
	if result(t, root)["reason"] != "metadata_limit" {
		t.Fatal("metadata bound failed")
	}
	raw, _ := os.ReadFile(filepath.Join(root, "events.local.jsonl"))
	var m Meta
	json.Unmarshal(bytes.Split(raw, []byte{'\n'})[1], &m)
	if !m.SampleGap {
		t.Fatal("hidden gap")
	}
}

func TestWriteFailurePreservesEvidenceAndHasNoSuccessManifest(t *testing.T) {
	root := permit(t, 20)
	var r Recorder
	if e := r.Start(root, 7, [16]byte{1}, time.Now().Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(root, "aec.s16le")
	os.WriteFile(path, []byte{99}, 0600)
	r.Stop("test")
	if e := r.Wait(); e == nil {
		t.Fatal("write collision reported success")
	}
	raw, _ := os.ReadFile(path)
	if !bytes.Equal(raw, []byte{99}) {
		t.Fatal("overwrote existing evidence")
	}
	if _, e := os.Stat(filepath.Join(root, "capture.local.json")); !os.IsNotExist(e) {
		t.Fatal("false completion manifest")
	}
}

func TestInvalidBatchStopsWithoutRetainingAudio(t *testing.T) {
	root := permit(t, 20)
	r := start(t, root, time.Now().Add(time.Minute))
	r.Capture(meta(), []byte{1, 2}, []byte{3}, nil, []byte{1, 2})
	r.Wait()
	if result(t, root)["reason"] != "invalid_batch" {
		t.Fatal("invalid data retained")
	}
}

func BenchmarkCapture(b *testing.B) {
	// Memory-only cost of a normal 160 ms batch; no disk or microphone.
	data := make([]byte, 5120)
	r := new(Recorder)
	r.limit = MaxSamples
	r.deadline = time.Now().Add(time.Hour)
	r.pcm = make([]byte, 0, MaxSamples*6)
	r.rows = make([]Meta, 0, maxBatches)
	r.active.Store(true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.pcm = r.pcm[:0]
		r.rows = r.rows[:0]
		r.Capture(meta(), data, data, data, data)
	}
}
