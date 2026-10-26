package aec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticObservationDoesNotChangeAudioOrLearnedState(t *testing.T) {
	observed, ordinary := hwCanceller(64), hwCanceller(64)
	signal := synth(20 * FrameSize)
	for frame := 0; frame < 20; frame++ {
		mics := make([][]float32, NumMics)
		for ch := range mics {
			mics[ch] = make([]float32, FrameSize)
			for i := range mics[ch] {
				mics[ch][i] = float32(signal[frame*FrameSize+i]) / 5
			}
		}
		ref := toBytes(signal[frame*FrameSize : (frame+1)*FrameSize])
		want := ordinary.ProcessMicsFloatWithRef(mics, 6, ref, 22.4)
		got, state := observed.ProcessMicsFloatDiagnostic(mics, 6, ref, 22.4)
		if !bytes.Equal(got, want) || state.HardwareFrames != uint64(frame+1) {
			t.Fatal("observation changed audio/frame provenance")
		}
	}
	a, _ := observed.exportMicsForTest()
	b, _ := ordinary.exportMicsForTest()
	if !bytes.Equal(a, b) {
		t.Fatal("observation changed adaptation")
	}
}

func TestDiagnosticDistinguishesFloatBankLoadRefusalAndMissing(t *testing.T) {
	c := hwCanceller(64)
	c.setFloatMicLocked(true)
	bank, err := c.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"loaded", "refused", "missing"} {
		t.Run(kind, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "bank")
			data := append([]byte(nil), bank...)
			if kind == "refused" {
				data[0] = 'X'
			}
			if kind != "missing" {
				if err := os.WriteFile(base+".unity-f32.mics", data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			fresh := New()
			fresh.SetStatePath(base)
			fresh.SetParams(true, 0, 200)
			fresh.SetHardwareRef(true)
			fresh.setFloatMicLocked(true)
			before, _ := fresh.exportMicsForTest()
			state := fresh.DiagnosticState()
			after, _ := fresh.exportMicsForTest()
			if string(before) != string(after) {
				t.Fatal("diagnostic changed filter")
			}
			if state.LoadStatus != kind || state.Domain != "unity_f32_s24" || state.Generation < 2 {
				t.Fatal(state)
			}
			if kind != "missing" {
				sum := sha256.Sum256(data)
				if state.LoadSHA256 != hex.EncodeToString(sum[:]) {
					t.Fatal("wrong loaded/refused file identity")
				}
			}
			fresh.SetParams(false, 0, 200)
			if fresh.DiagnosticState().LoadStatus != "inactive" {
				t.Fatal("disabled state falsely loaded")
			}
		})
	}
}
