package aec

import (
	"encoding/binary"
	"math"
	"testing"
)

// Near-end speech below one S16 unit remains meaningful in S24 capture.
// With a silent far end, the echo-only stage must preserve its level.
func TestResidualPreservesSubS16NearEnd(t *testing.T) {
	c := hwCanceller(64)
	c.setResidualSuppression(true)
	defer func() { c.mu.Lock(); defer c.mu.Unlock(); c.freeLocked() }()
	var inputEnergy, outputEnergy float64
	count := 0
	for f := 0; f < 12; f++ {
		mics := make([][]float32, 7)
		for ch := range mics {
			mics[ch] = make([]float32, FrameSize)
			for i := range mics[ch] {
				mics[ch][i] = float32(.25 * math.Sin(2*math.Pi*997*float64(f*FrameSize+i)/16000))
			}
		}
		out := c.ProcessMicsFloatWithRef(mics, 6, make([]byte, FrameSize*2), 256)
		if f < 3 {
			continue
		}
		for i := 0; i < FrameSize; i++ {
			v := float64(int16(binary.LittleEndian.Uint16(out[i*2:])))
			outputEnergy += v * v
			u := float64(mics[6][i]) * 256
			inputEnergy += u * u
			count++
		}
	}
	ratio := math.Sqrt(outputEnergy / inputEnergy)
	if count == 0 || math.IsNaN(ratio) || ratio < .8 || ratio > 1.2 {
		t.Fatalf("silent-far-end near-end amplitude ratio %g, want 0.8..1.2", ratio)
	}
}

func TestResidualDoesNotChangeLearnedEchoPath(t *testing.T) {
	a, b := hwCanceller(64), hwCanceller(64)
	b.setResidualSuppression(true)
	defer func() {
		a.mu.Lock()
		a.freeLocked()
		a.mu.Unlock()
		b.mu.Lock()
		b.freeLocked()
		b.mu.Unlock()
	}()
	const frames = 24
	far := make([]int16, frames*FrameSize)
	seed := uint32(27)
	for i := range far {
		seed = 1664525*seed + 1013904223
		far[i] = int16((seed>>20)&4095) - 2048
	}
	for f := 0; f < frames; f++ {
		mics := make([][]float32, 7)
		ref := make([]byte, FrameSize*2)
		for i := 0; i < FrameSize; i++ {
			binary.LittleEndian.PutUint16(ref[i*2:], uint16(far[f*FrameSize+i]))
		}
		for ch := range mics {
			mics[ch] = make([]float32, FrameSize)
			for i := range mics[ch] {
				n := f*FrameSize + i - 33
				if n >= 0 {
					mics[ch][i] = float32(far[n]) * float32(ch+1) / 16
				}
			}
		}
		a.ProcessMicsFloatWithRef(mics, 6, ref, 16)
		b.ProcessMicsFloatWithRef(mics, 6, ref, 16)
	}
	first, err := a.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("residual post-filter changed learned AEC state")
	}
}

func TestResidualSurvivesAECRebuild(t *testing.T) {
	c := hwCanceller(64)
	c.setResidualSuppression(true)
	defer func() { c.mu.Lock(); defer c.mu.Unlock(); c.freeLocked() }()
	for epoch := 0; epoch < 3; epoch++ {
		c.SetParams(false, 0, 64)
		c.SetParams(true, 0, 64)
		for f := 0; f < 5; f++ {
			mics := make([][]float32, 7)
			for ch := range mics {
				mics[ch] = make([]float32, FrameSize)
				for i := range mics[ch] {
					mics[ch][i] = .25
				}
			}
			out := c.ProcessMicsFloatWithRef(mics, 6, make([]byte, FrameSize*2), 256)
			if len(out) != FrameSize*2 {
				t.Fatal("lost output period after rebuild")
			}
		}
	}
}

func TestResidualNeverReplaysAnIdleMicrophoneFrame(t *testing.T) {
	c := hwCanceller(64)
	c.setResidualSuppression(true)
	defer func() { c.mu.Lock(); defer c.mu.Unlock(); c.freeLocked() }()
	makeMics := func(voiced bool) [][]float32 {
		m := make([][]float32, 7)
		for ch := range m {
			m[ch] = make([]float32, FrameSize)
		}
		if voiced {
			for i := range m[0] {
				m[0][i] = float32(1000 * math.Sin(2*math.Pi*16*float64(i)/FrameSize))
			}
		}
		return m
	}
	ref := make([]byte, FrameSize*2)
	for f := 0; f < quietHold; f++ {
		c.ProcessMicsFloatWithRef(makeMics(true), 6, ref, 1)
	}
	for f := 0; f < 4; f++ {
		c.ProcessMicsFloatWithRef(makeMics(false), 6, ref, 1)
	}
	out := c.ProcessMicsFloatWithRef(makeMics(false), 0, ref, 1)
	var energy float64
	for i := 0; i < FrameSize; i++ {
		v := float64(int16(binary.LittleEndian.Uint16(out[i*2:])))
		energy += v * v
	}
	if rms := math.Sqrt(energy / FrameSize); rms > 40 {
		t.Fatalf("replayed idle mic history: RMS %g", rms)
	}
}
