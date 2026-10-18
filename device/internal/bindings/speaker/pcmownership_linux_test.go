package speaker

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

// Sanitized exact counters and identities observed on the running Dot without
// recording or playback. The server's TGID was 571; ALSA's opening TID was 582.
const threadOwnedStatus = `state: RUNNING
owner_pid: 582
trigger_time: 1262304051.027770771
delay: 4064
hw_ptr: 158787696
appl_ptr: 158791760
`

func TestPlaybackThreadOwnerRegression(t *testing.T) {
	if _, ok := pcmPlaybackPosition(threadOwnedStatus, 571); ok {
		t.Fatal("negative control accepted process ID")
	}
	o := pcmOwnership{thread: 582}
	p, ok := o.position(threadOwnedStatus)
	if !ok || p.Owner != 582 || p.Played != 158787696 {
		t.Fatal("opening thread rejected", p, ok)
	}
	for _, owner := range []uint64{0, 571, 583} {
		o.thread = owner
		if _, ok := o.position(threadOwnedStatus); ok {
			t.Fatal("foreign/uninitialized owner accepted", owner)
		}
	}
}

func TestPlaybackOwnershipCapturedOnActualOpeningThread(t *testing.T) {
	var o pcmOwnership
	var actual uint64
	if err := o.open(func() error { actual = uint64(syscall.Gettid()); return nil }); err != nil {
		t.Fatal(err)
	}
	if actual == 0 || o.thread != actual {
		t.Fatal(o.thread, actual)
	}
	status := fmt.Sprintf("state: RUNNING\nowner_pid: %d\ntrigger_time: epoch\ndelay: 20\nhw_ptr: 80\nappl_ptr: 100\n", actual)
	if _, ok := o.position(status); !ok {
		t.Fatal("actual opened owner rejected")
	}
	if err := o.open(func() error { return errors.New("open failed") }); err == nil {
		t.Fatal("failure hidden")
	}
	if o.thread != 0 {
		t.Fatal("failed reopen retained old ownership")
	}
}
