package aec

import (
	"encoding/binary"
	"math"
)

// ProcessMicsFloatWithRef accepts unity-gain microphones in S16 amplitude
// units, retaining all eight fractional bits of S24 capture. Only the final
// residual receives the configured gain and S16 quantisation. The reference
// stays at unity gain. ADC saturation still freezes Speex adaptation.
// This entry point is for the confirmed hardware-reference path only.
func (c *Canceller) ProcessMicsFloatWithRef(mics [][]float32, active int, ref []byte, gain float64) []byte {
	if active < 0 || active >= len(mics) {
		return nil
	}
	mono := mics[active]
	valid := len(mics) == NumMics && len(mono) > 0 && len(mono)%FrameSize == 0 && len(ref) == len(mono)*2
	for _, m := range mics {
		valid = valid && len(m) == len(mono)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !valid || !c.enabled || !c.hwRef || c.st == nil {
		if c.enabled && c.hwRef {
			c.hwMissing++
		}
		return gainedPCM(mono, gain)
	}
	c.setFloatMicLocked(true)
	out := make([]byte, len(mono)*2)
	for off := 0; off < len(mono); off += FrameSize {
		quiet := c.setFloatReferenceLocked(ref[off*2 : (off+FrameSize)*2])
		if quiet {
			c.farQuiet++
		} else {
			c.farQuiet = 0
		}
		c.hwFrames++
		for k := 1; k <= NumMics; k++ {
			ch := (active + k) % NumMics
			if ch != active && c.farQuiet > quietHold {
				continue
			}
			copy(c.floatIn[:], mics[ch][off:off+FrameSize])
			c.cancelFloatLocked(ch)
			c.micFrames++
		}
		putGainedPCM(out[off*2:], c.floatOut[:], gain)
		var refSq float64
		for i := 0; i < FrameSize; i++ {
			v := float64(int16(binary.LittleEndian.Uint16(ref[(off+i)*2:])))
			refSq += v * v
		}
		c.noteAttLocked(floatRMS(c.floatIn[:]), floatRMS(c.floatOut[:]), math.Sqrt(refSq/FrameSize), true, active)
	}
	return out
}

func floatRMS(samples []float32) float64 {
	var sum float64
	for _, v := range samples {
		sum += float64(v) * float64(v)
	}
	return math.Sqrt(sum / float64(len(samples)))
}

func gainedPCM(samples []float32, gain float64) []byte {
	out := make([]byte, len(samples)*2)
	putGainedPCM(out, samples, gain)
	return out
}

func putGainedPCM(out []byte, samples []float32, gain float64) {
	for i, s := range samples {
		v := math.Floor(float64(s) * gain)
		v = math.Max(-32768, math.Min(32767, v))
		binary.LittleEndian.PutUint16(out[i*2:], uint16(int16(v)))
	}
}
