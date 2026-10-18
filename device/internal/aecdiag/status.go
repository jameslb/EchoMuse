package aecdiag

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// StatusPath contains no audio. The authenticated controller reads it before a
// diagnostic; PID and process start ticks prevent reuse after process restart.
const StatusPath = "/tmp/em-aec-status.json"

func (r *Recorder) PublishStatus(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statusPath = path
	err := r.publishStatusLocked()
	if err != nil {
		_ = os.Remove(path)
	}
	return err
}

func (r *Recorder) publishStatusLocked() error {
	if r.statusPath == "" {
		return nil
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return err
	}
	end := strings.LastIndex(string(stat), ")")
	if end < 0 {
		return errors.New("invalid process identity")
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) <= 19 {
		return errors.New("missing process start ticks")
	}
	raw, err := json.Marshal(map[string]any{"version": 1, "pid": os.Getpid(),
		"start_ticks": fields[19], "available": !r.used})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(r.statusPath), ".em-aec-status-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(raw)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), r.statusPath)
}
