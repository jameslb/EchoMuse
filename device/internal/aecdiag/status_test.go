package aecdiag

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAvailabilitySurvivesFreshPermitButNotUsedRecorder(t *testing.T) {
	var r Recorder
	status := filepath.Join(t.TempDir(), "status.json")
	if err := r.PublishStatus(status); err != nil {
		t.Fatal(err)
	}
	read := func() map[string]any {
		raw, err := os.ReadFile(status)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := read()
	if before["available"] != true || before["pid"] != float64(os.Getpid()) || before["start_ticks"] == "" {
		t.Fatal(before)
	}
	root := permit(t, 1)
	if err := r.Start(root, 7, [16]byte{1}, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	r.Stop("test")
	if err := r.Wait(); err != nil {
		t.Fatal(err)
	}
	after := read()
	if after["available"] != false || after["start_ticks"] != before["start_ticks"] {
		t.Fatal(after)
	}
	// Archiving old output and creating a fresh permit cannot reset the fuse.
	if err := r.Start(permit(t, 1), 8, [16]byte{2}, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("rearmed used process")
	}
	if read()["available"] != false {
		t.Fatal("reported available after second permit")
	}
}

func TestUnavailableReportFailsBeforeAudio(t *testing.T) {
	var r Recorder
	parent := filepath.Join(t.TempDir(), "status")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "state.json")
	if err := r.PublishStatus(path); err != nil {
		t.Fatal(err)
	}
	os.Remove(path)
	os.Remove(parent)
	if err := r.Start(permit(t, 1), 7, [16]byte{1}, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("missing availability report accepted")
	}
	if r.Active() || r.pcm != nil {
		t.Fatal("retained PCM despite reporting failure")
	}
}
