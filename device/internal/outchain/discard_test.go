package outchain

import "testing"

func TestDiscardRemovesBufferedSpeechButKeepsSettings(t *testing.T) {
	c := New(48000)
	p := DefaultParams()
	p.Bands[1] = 6
	c.SetParams(p)
	c.SetActive(true)
	b := make([]byte, 2048*4)
	for i := 0; i < len(b); i += 4 {
		b[i+1] = 32
		b[i+3] = 32
	}
	c.Process(b)
	if c.DelayFrames() != 239 {
		t.Fatal("missing limiter delay", c.DelayFrames())
	}
	c.DiscardHistory()
	quiet := make([]byte, len(b))
	c.Process(quiet)
	for _, v := range quiet {
		if v != 0 {
			t.Fatal("old speech survived flush")
		}
	}
	if c.params != p || !c.Active() {
		t.Fatal("flush changed processing settings")
	}
	c.SetActive(false)
	c.Process(quiet)
	if c.DelayFrames() != 0 {
		t.Fatal("inactive chain delay")
	}
}
