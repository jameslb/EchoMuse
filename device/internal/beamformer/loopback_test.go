package beamformer

import "testing"

func TestLoopbackFractionalSignedMapping(t *testing.T) {
	raw := make([]byte, frameSize*2)
	values := []int32{-8388608, 8388607, -1, 1}
	for i, v := range values {
		base := (i/2)*frameSize + (7+i%2)*byteSample
		raw[base] = byte(v)
		raw[base+1] = byte(v >> 8)
		raw[base+2] = byte(v >> 16)
	}
	got := LoopbackChannelsFloat(raw)
	for i, v := range values {
		if got[i%2][i/2] != float32(v)/256 {
			t.Fatal("lost S24 sign/fraction/channel")
		}
	}
	if LoopbackChannelsFloat(nil) != nil || LoopbackChannelsFloat(raw[:len(raw)-1]) != nil {
		t.Fatal("accepted partial frame")
	}
}
