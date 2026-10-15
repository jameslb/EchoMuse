package speaker

import (
	"encoding/binary"
	satellite "github.com/wilbowes/EchoMuse/internal/rtplayback"
	"sync"
	"testing"
	"time"
)

var rtID = [16]byte{1, 2, 3}
var rtNow = time.Unix(1000, 0)

func rtPos(written, played uint64) pcmPosition {
	return pcmPosition{Owner: 1, Trigger: "session", Written: written, Played: played}
}
func rtWire(turn, seq, offset, total, frames uint32) []byte {
	b := make([]byte, 37+frames*2)
	b[0] = 9
	copy(b[1:17], rtID[:])
	for i, n := range []uint32{turn, seq, 48000, offset, total} {
		binary.BigEndian.PutUint32(b[17+i*4:], n)
	}
	for i := 37; i < len(b); i += 2 {
		b[i] = 0x34
		b[i+1] = 0x12
	}
	return b
}
func rtFresh(t *testing.T) *realtimePlayback {
	t.Helper()
	r := &realtimePlayback{}
	if err := r.open(rtID, rtNow, rtNow.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !r.observe(rtPos(100, 100), rtNow) || len(r.events) != 1 {
		t.Fatal("no measured ready")
	}
	r.events = nil
	return r
}

func TestRealtimeOpenAndFlushWaitForActualDAC(t *testing.T) {
	r := &realtimePlayback{}
	r.open(rtID, rtNow, rtNow.Add(time.Second))
	r.observe(rtPos(400, 200), rtNow)
	if len(r.events) != 0 || !r.waiting {
		t.Fatal("ready while old audio queued")
	}
	r.observe(rtPos(500, 399), rtNow)
	if len(r.events) != 0 {
		t.Fatal("early ready")
	}
	r.observe(rtPos(600, 400), rtNow)
	if len(r.events) != 1 || r.events[0].Type != "rt_ready" {
		t.Fatal(r.events)
	}
	r.push(rtWire(0, 1, 0, 960, 960))
	r.render(100) // partial token must be thrown away
	if !r.flush(rtID, 1) {
		t.Fatal("flush refused")
	}
	r.observe(rtPos(700, 600), rtNow)
	if out, _ := r.render(2048); out != nil {
		t.Fatal("audio before fence")
	}
	r.observe(rtPos(800, 700), rtNow)
	if len(r.events) != 1 || r.events[0].Turn != 1 {
		t.Fatal(r.events)
	}
	if out, _ := r.render(2048); out != nil {
		t.Fatal("retired partial survived")
	}
}

func TestRealtimeSplitTokenExactStereoAndDSPDelay(t *testing.T) {
	r := rtFresh(t)
	if err := r.push(rtWire(0, 1, 0, 960, 960)); err != nil {
		t.Fatal(err)
	}
	a, marks := r.render(500)
	if len(a) != 2000 || len(marks) != 0 {
		t.Fatal("first partial", len(a), marks)
	}
	for i := 0; i < len(a); i += 4 {
		if a[i] != 0x34 || a[i+1] != 0x12 || a[i+2] != 0x34 || a[i+3] != 0x12 {
			t.Fatal("PCM corrupted")
		}
	}
	r.written(rtPos(100, 100), rtPos(600, 200), 500, 239, marks, rtNow)
	b, marks := r.render(500)
	if len(marks) != 1 || marks[0].end != 460 {
		t.Fatal("token mapping", marks)
	}
	for _, v := range b[460*4:] {
		if v != 0 {
			t.Fatal("non-silent padding")
		}
	}
	r.written(rtPos(600, 200), rtPos(1100, 900), 500, 239, marks, rtNow)
	r.observe(rtPos(1400, 1298), rtNow)
	if len(r.events) != 0 {
		t.Fatal("DSP delay or queued audio counted as played")
	}
	r.observe(rtPos(1400, 1299), rtNow)
	if len(r.events) != 2 || r.events[0].Type != "rt_played" || r.events[1].Type != "rt_done" || r.events[1].Frames != 960 {
		t.Fatal(r.events)
	}
}

func TestRealtimeFailedOrResetWriteNeverCompletes(t *testing.T) {
	for _, after := range []pcmPosition{rtPos(109, 109), {Owner: 2, Trigger: "session", Written: 110, Played: 110}, {Owner: 1, Trigger: "new", Written: 110, Played: 110}} {
		r := rtFresh(t)
		r.push(rtWire(0, 1, 0, 10, 10))
		_, marks := r.render(10)
		if r.written(rtPos(100, 100), after, 10, 0, marks, rtNow) || r.receiver != nil || len(r.events) != 0 {
			t.Fatal("bad write credited", after)
		}
	}
}

func TestRealtimeLeaseCannotBeExtendedByFlush(t *testing.T) {
	r := rtFresh(t)
	r.flush(rtID, 1)
	if r.observe(rtPos(100, 100), rtNow.Add(30*time.Second)) || r.receiver != nil {
		t.Fatal("expired output survived")
	}
	if err := r.push(rtWire(1, 1, 0, 10, 10)); err != nil {
		t.Fatal("retired in-flight packet tore down the link")
	}
	if audio, _ := r.render(2048); audio != nil {
		t.Fatal("closed route rendered retired packet")
	}
	if err := r.open(rtID, rtNow, rtNow.Add(31*time.Second)); err == nil {
		t.Fatal("unbounded lease")
	}
}

func TestRealtimeRateAndQueueBoundsFailClosed(t *testing.T) {
	r := rtFresh(t)
	b := rtWire(0, 1, 0, 10, 10)
	binary.BigEndian.PutUint32(b[25:29], 24000)
	if err := r.push(b); err == nil || r.receiver != nil {
		t.Fatal("wrong pitch rate accepted")
	}
	r = rtFresh(t)
	for i := 0; i < 65; i++ {
		r.emit(satellite.Receipt{Type: "rt_ready"})
	}
	if r.receiver != nil {
		t.Fatal("unbounded control backlog")
	}
}

func TestRealtimeFlushAndProducerAreRaceFree(t *testing.T) {
	for n := 0; n < 50; n++ {
		r := rtFresh(t)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); r.push(rtWire(0, 1, 0, 960, 960)) }()
		go func() { defer wg.Done(); r.flush(rtID, 1) }()
		wg.Wait()
		r.observe(rtPos(100, 100), rtNow)
		if out, _ := r.render(2048); out != nil {
			t.Fatal("stale audio survived flush")
		}
	}
}

func TestRealtimeCloseKeepsOwnershipUntilHardwareFence(t *testing.T) {
	r := rtFresh(t)
	r.push(rtWire(0, 1, 0, 960, 960))
	r.closeLocked()
	if r.closing == nil {
		t.Fatal("close lost hardware fence")
	}
	r.observe(rtPos(500, 400), rtNow)
	if len(r.events) != 0 || r.closing == nil {
		t.Fatal("early native handover")
	}
	if err := r.open(rtID, rtNow, rtNow.Add(time.Second)); err == nil {
		t.Fatal("new route before fence")
	}
	r.observe(rtPos(600, 500), rtNow)
	if r.closing != nil || len(r.events) != 1 || r.events[0].Type != "rt_closed" {
		t.Fatal(r.events)
	}
}

func TestExpiryCannotSkipDSPResetAtEndOfCycle(t *testing.T) {
	r := rtFresh(t)
	late := rtNow.Add(30 * time.Second)
	r.observe(rtPos(100, 100), late)
	if !r.reset {
		t.Fatal("missing DSP reset")
	}
	r.written(rtPos(100, 100), rtPos(200, 200), 100, 0, nil, late)
	if !r.reset || len(r.events) != 0 {
		t.Fatal("post-write acknowledged undiscarded DSP history")
	}
}
