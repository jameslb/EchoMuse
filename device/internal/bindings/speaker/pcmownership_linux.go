package speaker

import (
	"runtime"
	"syscall"
)

// ALSA owner_pid identifies the OS thread which opened the substream. Pin that
// thread across pcm_open and retain its identity for the lifetime of this PCM.
// A different thread in the same process is not accepted as a replacement owner.
type pcmOwnership struct{ thread uint64 }

func (o *pcmOwnership) open(open func() error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	o.thread = 0
	thread := uint64(syscall.Gettid())
	if err := open(); err != nil {
		return err
	}
	o.thread = thread
	return nil
}

func (o *pcmOwnership) position(status string) (pcmPosition, bool) {
	return pcmPlaybackPosition(status, o.thread)
}
