package speaker

import (
	"testing"
	"time"
)

func diagnostics(r *realtimePlayback) []RealtimeDiagnostic {
	var result []RealtimeDiagnostic
	for {
		e, ok := r.diagnostics.take()
		if !ok {
			return result
		}
		result = append(result, e)
	}
}

func TestDiagnosticSeparatesFenceWaitFromReady(t *testing.T) {
	r := &realtimePlayback{}
	r.traceCommand(7, rtID, 0, "command_open")
	r.open(rtID, rtNow, rtNow.Add(time.Second))
	r.observe(rtPos(400, 200), rtNow)
	for _, e := range diagnostics(r) {
		if e.Event == "rt_ready_queued" {
			t.Fatal("untruthful ready diagnostic")
		}
		if e.Event == "command_open" && (e.Session != 7 || e.Stream != rtID) {
			t.Fatal(e)
		}
	}
	r.observe(rtPos(600, 400), rtNow.Add(100*time.Millisecond))
	found := false
	for _, e := range diagnostics(r) {
		if e.Event == "rt_ready_queued" {
			found = true
			if e.Fence != 400 || e.Played != 400 || e.Stream != rtID || e.Waiting {
				t.Fatal(e)
			}
		}
	}
	if !found || len(r.events) != 1 {
		t.Fatal("missing measured ready")
	}
}

func TestDiagnosticIdentifiesEpochFailureWithoutForgingReceipt(t *testing.T) {
	r := &realtimePlayback{}
	r.open(rtID, rtNow, rtNow.Add(time.Second))
	r.observe(rtPos(400, 200), rtNow)
	p := rtPos(500, 300)
	p.Trigger = "new epoch"
	r.observe(p, rtNow.Add(time.Millisecond))
	found := false
	for _, e := range diagnostics(r) {
		if e.Event == "progress_invalid" {
			found = true
			if !e.EpochChanged {
				t.Fatal(e)
			}
		}
	}
	if !found || r.receiver != nil || len(r.events) != 0 {
		t.Fatal("failure hidden or receipt forged")
	}
}

func TestDiagnosticQueueAndLifetimeBudgetAreFixed(t *testing.T) {
	r := &realtimePlayback{}
	for i := 0; i < 10000; i++ {
		r.traceCommand(7, rtID, 0, "command_open")
	}
	if r.diagnostics.count != diagnosticCapacity {
		t.Fatal(r.diagnostics.count)
	}
	total := len(diagnostics(r))
	for i := 0; i < 10000; i++ {
		r.traceCommand(7, rtID, 0, "command_open")
		total += len(diagnostics(r))
	}
	if total != diagnosticBudget {
		t.Fatal(total)
	}
	if err := r.open(rtID, rtNow, rtNow.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	r.observe(rtPos(100, 100), rtNow)
	if len(r.events) != 1 || len(diagnostics(r)) != 0 {
		t.Fatal("budget reset or receipt affected")
	}
}

func TestDiagnosticPositionSamplingIsBounded(t *testing.T) {
	r := &realtimePlayback{}
	r.open(rtID, rtNow, rtNow.Add(30*time.Second))
	for i := 0; i < 20000; i++ {
		r.observe(rtPos(100, 0), rtNow.Add(time.Duration(i)*time.Millisecond))
	}
	count := 0
	for _, e := range diagnostics(r) {
		if e.Event == "position" {
			count++
		}
	}
	if count != 12 || len(r.events) != 0 {
		t.Fatal(count, len(r.events))
	}
}

func TestDiagnosticProducerHasNoAllocations(t *testing.T) {
	r := &realtimePlayback{}
	if n := testing.AllocsPerRun(100, func() {
		r.diagnostics = realtimeDiagnostics{}
		r.trace("position", rtPos(400, 200))
	}); n != 0 {
		t.Fatal("audio-writer allocation", n)
	}
}

func TestDiagnosticCloseStillWaitsForHardware(t *testing.T) {
	r := rtFresh(t)
	diagnostics(r)
	r.closeLocked()
	r.observe(rtPos(400, 200), rtNow)
	for _, e := range diagnostics(r) {
		if e.Event == "rt_closed_queued" {
			t.Fatal("close reported before fence")
		}
	}
	r.observe(rtPos(500, 400), rtNow.Add(100*time.Millisecond))
	found := false
	for _, e := range diagnostics(r) {
		if e.Event == "rt_closed_queued" {
			found = true
			if e.Played < e.Fence {
				t.Fatal(e)
			}
		}
	}
	if !found {
		t.Fatal("missing actual close fence")
	}
}
