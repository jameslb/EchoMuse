package aec

import (
	"testing"
	"time"
)

func TestHardwareReferenceSpeakerDoesNotWaitForCancellationLock(t *testing.T) {
	c := New()
	c.SetHardwareRef(true)
	c.mu.Lock() // Represents a microphone batch doing cancellation.
	done := make(chan struct{})
	go func() { c.WriteFar(make([]byte, 2048*4)); close(done) }()
	blocked := false
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	c.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("speaker writer did not retire after test released lock")
	}
	if blocked {
		t.Fatal("hardware-reference speaker write waited for the cancellation lock")
	}
}

func TestSpeakerReferenceResumesAfterHardwareToSoftwareSwitch(t *testing.T) {
	c := New()
	c.SetParams(true, 0, 300)
	defer c.SetParams(false, 0, 300)
	c.SetHardwareRef(true)
	period := make([]byte, 2048*4)
	c.WriteFar(period)
	if c.count != 0 {
		t.Fatal("hardware mode populated unused software reference ring")
	}
	c.SetHardwareRef(false)
	before := c.count
	c.WriteFar(period)
	if c.count <= before {
		t.Fatal("switch back to software mode left the speaker reference disabled")
	}
}
