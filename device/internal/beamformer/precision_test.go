package beamformer

import "testing"

func TestMicChannelsFloatPreservesS24(t *testing.T) {
	values := []int32{-8388608, -32001 * 256, -257, -1, 1, 255, 8388607}
	raw := make([]byte, 2*frameSize)
	for i := 0; i < 2; i++ {
		for ch, v := range values {
			p := i*frameSize + ch*3
			raw[p], raw[p+1], raw[p+2] = byte(v), byte(v>>8), byte(v>>16)
		}
	}
	got := MicChannelsFloat(raw)
	for ch, v := range values {
		for i := range got[ch] {
			if got[ch][i] != float32(v)/256 {
				t.Fatalf("ch%d sample%d lost S24 bits", ch, i)
			}
		}
	}
}
