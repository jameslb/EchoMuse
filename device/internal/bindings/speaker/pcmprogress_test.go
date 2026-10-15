package speaker

import (
	"strings"
	"testing"
)

func TestPlaybackPositionIncludesDriverDelay(t *testing.T) {
	position, ok := pcmPlaybackPosition(runningStatus, 1094)
	if !ok || position.Written != 1570880 || position.Played != 1562816 {
		t.Fatal(position, ok)
	}
	withFIFO := strings.Replace(runningStatus, "delay       : 8064", "delay       : 8164", 1)
	position, ok = pcmPlaybackPosition(withFIFO, 1094)
	if !ok || position.Played != 1562716 {
		t.Fatal("FIFO was credited as played", position, ok)
	}
}

func TestPlaybackPositionRejectsAmbiguousHardwareState(t *testing.T) {
	for _, status := range []string{
		heldStatus, "closed", runningStatus + "delay: 8064\n",
		strings.Replace(runningStatus, "RUNNING", "XRUN", 1),
		strings.Replace(runningStatus, "owner_pid   : 1094", "owner_pid   : 1234", 1),
		strings.Replace(runningStatus, "delay       : 8064", "delay       : -1", 1),
		strings.Replace(runningStatus, "delay       : 8064", "delay       : 9999999", 1),
		strings.Replace(runningStatus, "delay       : 8064", "delay       : 1", 1),
		strings.Replace(runningStatus, "appl_ptr    : 1570880", "appl_ptr    : 100", 1),
		strings.Replace(runningStatus, "hw_ptr      : 1562816\n", "", 1),
		strings.Replace(runningStatus, "trigger_time: 1786260229.525790163", "trigger_time: 0.000000000", 1),
	} {
		if _, ok := pcmPlaybackPosition(status, 1094); ok {
			t.Fatal("accepted ambiguous status", status)
		}
	}
}

func TestPlaybackFenceRequiresMeasuredConsumption(t *testing.T) {
	p := pcmProgress{}
	if p.reached(100) {
		t.Fatal("uninitialized fence")
	}
	if !p.observe(pcmPosition{Owner: 1, Trigger: "session", Written: 200, Played: 80}) {
		t.Fatal("snapshot rejected")
	}
	if p.reached(100) {
		t.Fatal("queued audio counted as heard")
	}
	if !p.observe(pcmPosition{Owner: 1, Trigger: "session", Written: 250, Played: 100}) || !p.reached(100) {
		t.Fatal("measured fence missing")
	}
}

func TestPlaybackResetCannotCreditOldFence(t *testing.T) {
	for _, changed := range []pcmPosition{
		{Owner: 2, Trigger: "session", Written: 300, Played: 250},
		{Owner: 1, Trigger: "new session", Written: 300, Played: 250},
		{Owner: 1, Trigger: "session", Written: 99, Played: 80},
		{Owner: 1, Trigger: "session", Written: 300, Played: 79},
	} {
		p := pcmProgress{}
		p.observe(pcmPosition{Owner: 1, Trigger: "session", Written: 200, Played: 80})
		if p.observe(changed) || p.reached(50) {
			t.Fatal("reset allowed stale completion")
		}
		if p.observe(pcmPosition{Owner: 1, Trigger: "session", Written: 400, Played: 300}) {
			t.Fatal("failed tracker silently recovered")
		}
	}
}
