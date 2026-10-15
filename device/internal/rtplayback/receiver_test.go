package satellite

import (
	"encoding/binary"
	"sync"
	"testing"
)

var sid = [16]byte{1, 2, 3, 4}

func fresh(t *testing.T) *Receiver {
	t.Helper()
	r := &Receiver{}
	if err := r.Open(sid); err != nil {
		t.Fatal(err)
	}
	if r.Quiesced(sid, 0) == nil {
		t.Fatal("missing ready")
	}
	return r
}

func wire(turn, seq, offset, total, count uint32) []byte {
	b := make([]byte, HeaderSize+int(count)*2)
	b[0] = 9
	copy(b[1:17], sid[:])
	for i, n := range []uint32{turn, seq, 16000, offset, total} {
		binary.BigEndian.PutUint32(b[17+i*4:], n)
	}
	for i := HeaderSize; i < len(b); i++ {
		b[i] = byte(i)
	}
	return b
}

func TestOnlyConsumedAudioProducesReceipts(t *testing.T) {
	r := fresh(t)
	for _, offset := range []uint32{0, 320, 640} {
		if err := r.Push(wire(0, 1, offset, 960, 320)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		chunk, ok := r.Take()
		if !ok {
			t.Fatal("missing audio")
		}
		if len(r.segments) != 1 {
			t.Fatal("queue take credited playback")
		}
		receipts, err := r.Consumed(chunk.Token)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && (len(receipts) != 1 || receipts[0].Type != "rt_played") {
			t.Fatal(receipts)
		}
		if i == 1 && len(receipts) != 0 {
			t.Fatal(receipts)
		}
		if i == 2 && (len(receipts) != 1 || receipts[0].Type != "rt_done" || receipts[0].Frames != 960) {
			t.Fatal(receipts)
		}
	}
}

func TestWireBufferIsOwned(t *testing.T) {
	r := fresh(t)
	b := wire(0, 1, 0, 320, 320)
	first := b[HeaderSize]
	if err := r.Push(b); err != nil {
		t.Fatal(err)
	}
	b[HeaderSize] ^= 255
	c, _ := r.Take()
	if c.PCM[0] != first {
		t.Fatal("caller changed queued PCM")
	}
}

func TestFlushRejectsTakenTokenQueuedAudioAndNetworkRemainder(t *testing.T) {
	r := fresh(t)
	r.Push(wire(0, 1, 0, 640, 320))
	r.Push(wire(0, 1, 320, 640, 320))
	old, _ := r.Take()
	if !r.Flush(sid, 1) {
		t.Fatal("flush failed")
	}
	if _, ok := r.Take(); ok {
		t.Fatal("old audio survived")
	}
	if events, err := r.Consumed(old.Token); err != nil || len(events) != 0 {
		t.Fatal("old callback credited")
	}
	if err := r.Push(wire(0, 1, 320, 640, 320)); err != nil {
		t.Fatal(err)
	}
	if r.Quiesced(sid, 0) != nil {
		t.Fatal("stale fence acknowledged new epoch")
	}
	if r.Quiesced(sid, 1) == nil {
		t.Fatal("new fence missing")
	}
	if err := r.Push(wire(1, 1, 0, 320, 320)); err != nil {
		t.Fatal(err)
	}
	c, _ := r.Take()
	events, err := r.Consumed(c.Token)
	if err != nil || len(events) != 2 || events[1].Turn != 1 {
		t.Fatal(events, err)
	}
}

func TestBeforePhysicalFenceNoReplacementAudio(t *testing.T) {
	r := fresh(t)
	r.Flush(sid, 1)
	if err := r.Push(wire(1, 1, 0, 320, 320)); err == nil {
		t.Fatal("accepted before fence")
	}
	if r.Quiesced(sid, 1) != nil {
		t.Fatal("protocol failure reopened")
	}
}

func TestDuplicateOldFlushCannotStopReplacement(t *testing.T) {
	r := fresh(t)
	r.Flush(sid, 2)
	r.Quiesced(sid, 2)
	r.Push(wire(2, 1, 0, 320, 320))
	if r.Flush(sid, 1) || r.Flush(sid, 2) {
		t.Fatal("old flush accepted")
	}
	if _, ok := r.Take(); !ok {
		t.Fatal("new audio dropped")
	}
}

func TestDuplicatesGapsAndOversizeFailClosed(t *testing.T) {
	cases := map[string][]byte{
		"gap": wire(0, 1, 320, 640, 320), "skip_segment": wire(0, 2, 0, 320, 320),
		"future": wire(1, 1, 0, 320, 320), "huge": wire(0, 1, 0, 480001, 320),
		"tiny_nonfinal": wire(0, 1, 0, 640, 1), "large_packet": wire(0, 1, 0, 640, 640),
		"empty": wire(0, 1, 0, 320, 0),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			r := fresh(t)
			if err := r.Push(b); err == nil || !r.closed {
				t.Fatal("malformed input not closed")
			}
			if _, ok := r.Take(); ok {
				t.Fatal("malformed input emitted audio")
			}
		})
	}
	r := fresh(t)
	r.Push(wire(0, 1, 0, 640, 320))
	if err := r.Push(wire(0, 1, 0, 640, 320)); err == nil {
		t.Fatal("duplicate accepted")
	}
}

func TestTwoSegmentLookaheadAndThirtySecondLimit(t *testing.T) {
	r := fresh(t)
	r.Push(wire(0, 1, 0, 320, 320))
	r.Push(wire(0, 2, 0, 320, 320))
	if err := r.Push(wire(0, 3, 0, 320, 320)); err == nil {
		t.Fatal("third pending segment")
	}
	r = fresh(t)
	for offset := uint32(0); offset < 480000; offset += 320 {
		if err := r.Push(wire(0, 1, offset, 480000, 320)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Push(wire(0, 2, 0, 1, 1)); err == nil {
		t.Fatal("duration overflow")
	}
}

func TestWrongStreamAndClosedLinkDoNotPlay(t *testing.T) {
	r := fresh(t)
	b := wire(0, 1, 0, 320, 320)
	b[1] ^= 255
	if err := r.Push(b); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Take(); ok {
		t.Fatal("wrong link audio")
	}
	r.Close()
	if err := r.Open(sid); err == nil {
		t.Fatal("closed route reopened")
	}
	if err := r.Push(wire(0, 1, 0, 320, 320)); err == nil {
		t.Fatal("closed input accepted")
	}
}

func TestCompletionOrderAndDuplicates(t *testing.T) {
	r := fresh(t)
	r.Push(wire(0, 1, 0, 320, 320))
	r.Push(wire(0, 2, 0, 320, 320))
	a, _ := r.Take()
	b, _ := r.Take()
	if _, err := r.Consumed(b.Token); err == nil {
		t.Fatal("out of order credited")
	}
	r = fresh(t)
	r.Push(wire(0, 1, 0, 320, 320))
	a, _ = r.Take()
	first, _ := r.Consumed(a.Token)
	again, _ := r.Consumed(a.Token)
	if len(first) != 2 || len(again) != 0 {
		t.Fatal("duplicate receipts")
	}
}

func TestCannotConsumeFramesNeverTaken(t *testing.T) {
	r := fresh(t)
	r.Push(wire(0, 1, 0, 320, 320))
	_, err := r.Consumed(Token{Stream: sid, Seq: 1, Frames: 320})
	if err == nil {
		t.Fatal("unplayed frame accepted")
	}
}

func TestShortFinalBlockPreserved(t *testing.T) {
	r := fresh(t)
	r.Push(wire(0, 1, 0, 323, 320))
	r.Push(wire(0, 1, 320, 323, 3))
	a, _ := r.Take()
	r.Consumed(a.Token)
	b, _ := r.Take()
	events, err := r.Consumed(b.Token)
	if len(b.PCM) != 6 || err != nil || len(events) != 1 || events[0].Frames != 323 {
		t.Fatal(events, err)
	}
}

func TestConcurrentFlushAndDriverCompletion(t *testing.T) {
	for i := 0; i < 100; i++ {
		r := fresh(t)
		r.Push(wire(0, 1, 0, 320, 320))
		c, _ := r.Take()
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); r.Flush(sid, 1) }()
		go func() { defer wg.Done(); r.Consumed(c.Token) }()
		go func() { defer wg.Done(); r.Take() }()
		wg.Wait()
		if events, _ := r.Consumed(c.Token); len(events) != 0 {
			t.Fatal("late completion resurrected")
		}
		if _, ok := r.Take(); ok {
			t.Fatal("stale queue")
		}
	}
}
