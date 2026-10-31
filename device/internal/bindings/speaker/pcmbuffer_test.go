package speaker

import (
	"testing"
	"time"
)

func TestHardwareSchedulingBufferContract(t *testing.T) {
	// Upstream #848 reproduced underruns with the 85ms configuration even
	// with scheduling gaps below that nominal ring duration. Its restored
	// configuration is about 171ms. This checks the actual driver policy,
	// not a simulation of the physical underrun or an interruption pass.
	duration := time.Second * alsaBufferFrames / 48000
	if duration < 170*time.Millisecond {
		t.Fatalf("hardware scheduling margin regressed: %s", duration)
	}
	if duration >= 300*time.Millisecond {
		t.Fatalf("hardware buffer alone exhausts the interruption budget: %s", duration)
	}
}
