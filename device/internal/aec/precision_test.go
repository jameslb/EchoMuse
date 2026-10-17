package aec

import (
	"bytes"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/wilbowes/EchoMuse/internal/beamformer"
)

// A known linear echo path is representable by the unchanged 64ms filter.
// Applying the installed 27dB gain before S16 AEC creates a nonlinear path
// and freezes adaptation on clipped frames. This is a synthetic causal
// control, not a reconstruction of peaks lost in the physical recording.
func TestPrecisionCancelsEchoBeforeGainClipping(t *testing.T) {
	const frames = 200
	signal := synth(frames * FrameSize)
	gain := math.Pow(10, 27.0/20)
	c := hwCanceller(64)
	legacy := hwCanceller(64)
	beam := beamformer.New()
	var input, residual float64
	var legacyResidual float64
	clipped := 0
	for f := 0; f < frames; f++ {
		mics := make([][]float32, NumMics)
		for ch := range mics {
			mics[ch] = make([]float32, FrameSize)
			for i := range mics[ch] {
				j := f*FrameSize + i - 48 // retained physical lag approximately 3ms
				if j >= 0 {
					mics[ch][i] = float32(signal[j]) * 0.35
				}
			}
		}
		// Exercise actual S24 extraction, and retain the old production path
		// as a negative control so a weakened fixture cannot pass unnoticed.
		raw := make([]byte, FrameSize*27)
		for ch := range mics {
			for i, v := range mics[ch] {
				s := int32(v * 256)
				p := i*27 + ch*3
				raw[p], raw[p+1], raw[p+2] = byte(s), byte(s>>8), byte(s>>16)
			}
		}
		mics = beamformer.MicChannelsFloat(raw)
		ref := toBytes(signal[f*FrameSize : (f+1)*FrameSize])
		oldMono, _ := beam.Process(raw, -1, gain)
		oldOut := legacy.ProcessMicsWithRef(beam.MicChannels(raw, gain, 6, oldMono), 6, ref)
		out := c.ProcessMicsFloatWithRef(mics, 6, ref, gain)
		for _, v := range mics[6] {
			if math.Abs(float64(v)*gain) >= 32768 {
				clipped++
			}
			if f >= frames-30 {
				input += float64(v) * float64(v) * gain * gain
			}
		}
		if f >= frames-30 {
			r := rms(out)
			residual += r * r * FrameSize
			r = rms(oldOut)
			legacyResidual += r * r * FrameSize
		}
	}
	att := 10 * math.Log10(input/residual)
	oldAtt := 10 * math.Log10(input/legacyResidual)
	t.Logf("actual legacy beamformer/AEC control %.2fdB", oldAtt)
	if oldAtt > 6 {
		t.Fatalf("legacy control no longer reproduces clipping failure: %.2fdB", oldAtt)
	}
	t.Logf("pre-gain clipping control=%d samples; cancellation=%.2fdB", clipped, att)
	if clipped == 0 {
		t.Fatal("fixture did not exercise pre-AEC clipping")
	}
	if att < 20 {
		t.Fatalf("linear echo cancellation %.2fdB, want >=20dB", att)
	}
}

func TestPrecisionGainChangeKeepsLearnedPath(t *testing.T) {
	const frames = 150
	signal := synth((frames + 1) * FrameSize)
	c := hwCanceller(64)
	peer := hwCanceller(64)
	var last [][]float32
	for f := 0; f < frames; f++ {
		last = make([][]float32, NumMics)
		for ch := range last {
			last[ch] = make([]float32, FrameSize)
			for i := range last[ch] {
				last[ch][i] = float32(signal[f*FrameSize+i]) * 0.25
			}
		}
		c.ProcessMicsFloatWithRef(last, 6, toBytes(signal[f*FrameSize:(f+1)*FrameSize]), 10)
		peer.ProcessMicsFloatWithRef(last, 6, toBytes(signal[f*FrameSize:(f+1)*FrameSize]), 10)
	}
	ref := toBytes(signal[(frames-1)*FrameSize : frames*FrameSize])
	first := c.ProcessMicsFloatWithRef(last, 6, ref, 10)
	state1, err := c.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	second := peer.ProcessMicsFloatWithRef(last, 6, ref, 20)
	state2, err := peer.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state1, state2) {
		t.Fatal("post-AEC gain altered learned echo path")
	}
	for i := 0; i < len(first)/2; i++ {
		a := int(int16(binary.LittleEndian.Uint16(first[i*2:])))
		b := int(int16(binary.LittleEndian.Uint16(second[i*2:])))
		if b-2*a < 0 || b-2*a > 1 {
			t.Fatalf("gain not applied to full precision residual: %d %d", a, b)
		}
	}
}

func TestPrecisionRejectsOldGainStateAndPreservesRollback(t *testing.T) {
	c := hwCanceller(64)
	c.SetStatePath(filepath.Join(t.TempDir(), "echo.state"))
	old, err := c.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	oldPath := c.micsPath()
	mics := make([][]float32, NumMics)
	for ch := range mics {
		mics[ch] = make([]float32, FrameSize)
	}
	c.ProcessMicsFloatWithRef(mics, 6, make([]byte, FrameSize*2), 22.4)
	if err := c.importMicsForTest(old); err == nil {
		t.Fatal("amplified S16 filter accepted in unity float domain")
	}
	if c.micsPath() == oldPath {
		t.Fatal("new filter would overwrite rollback filter")
	}
	saved, err := c.exportMicsForTest()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.importMicsForTest(saved); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(old, saved) {
		t.Fatal("saved state lacks amplitude-domain identity")
	}
}

// A real near-end voice must survive while the far-end plays. Test the
// known voice projection rather than calling low residual energy success.
func TestPrecisionPreservesNearEndDuringPlayback(t *testing.T) {
	const frames = 180
	signal := synth(frames * FrameSize)
	c := hwCanceller(64)
	gain := math.Pow(10, 27.0/20)
	var projection, power float64
	for f := 0; f < frames; f++ {
		mics := make([][]float32, NumMics)
		for ch := range mics {
			mics[ch] = make([]float32, FrameSize)
			for i := range mics[ch] {
				j := f*FrameSize + i
				if j >= 48 {
					mics[ch][i] = float32(signal[j-48]) * 0.35
				}
				if f >= 150 {
					mics[ch][i] += float32(80 * math.Sin(2*math.Pi*733*float64(j)/16000))
				}
			}
		}
		out := c.ProcessMicsFloatWithRef(mics, 6, toBytes(signal[f*FrameSize:(f+1)*FrameSize]), gain)
		if f >= 152 {
			for i := 0; i < len(out)/2; i++ {
				v := int16(binary.LittleEndian.Uint16(out[i*2:]))
				want := 80 * gain * math.Sin(2*math.Pi*733*float64(f*FrameSize+i)/16000)
				projection += float64(v) * want
				power += want * want
			}
		}
	}
	ratio := projection / power
	t.Logf("near-end voice projection %.3f", ratio)
	if ratio < 0.8 || ratio > 1.2 {
		t.Fatalf("near-end voice suppressed/distorted: %.3f", ratio)
	}
}

// A quarter of an S16 LSB is a real S24 microphone signal. Amplifying after
// first rounding either input or residual to S16 would erase this speech.
func TestPrecisionRetainsSubLSBMicrophone(t *testing.T) {
	c := hwCanceller(64)
	const frames = 20
	gain := math.Pow(10, 27.0/20)
	var energy float64
	for f := 0; f < frames; f++ {
		mics := make([][]float32, NumMics)
		for ch := range mics {
			mics[ch] = make([]float32, FrameSize)
			for i := range mics[ch] {
				mics[ch][i] = float32(0.25 * math.Sin(2*math.Pi*1000*float64(f*FrameSize+i)/16000))
			}
		}
		out := c.ProcessMicsFloatWithRef(mics, 6, make([]byte, FrameSize*2), gain)
		if f >= 10 {
			energy += rms(out)
		}
	}
	t.Logf("sub-LSB speech RMS after gain %.3f", energy/10)
	if energy/10 < 3 || energy/10 > 5 {
		t.Fatalf("lost or distorted quiet microphone precision: RMS %.3f", energy/10)
	}
}
