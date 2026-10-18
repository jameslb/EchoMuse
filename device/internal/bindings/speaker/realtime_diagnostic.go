package speaker

import "time"

// No PCM, text, configuration or raw procfs content. The producer runs under
// realtime.mu and never does I/O. The control worker drains this fixed queue.
// Budget is per speaker lifetime, not renewed by wakes or malformed commands.
const diagnosticBudget = 256
const diagnosticCapacity = 64

var diagnosticOrigin = time.Now()

type RealtimeDiagnostic struct {
	Event                         string
	Stream                        [16]byte
	Session, Turn                 uint32
	At                            time.Time
	MonoNS                        int64
	Owner, Written, Played, Fence uint64
	Waiting, Reset, Closing       bool
	EpochChanged                  bool
}

type realtimeDiagnostics struct {
	queue              [diagnosticCapacity]RealtimeDiagnostic
	head, count, total int
	lastSample         time.Time
	samples            int
}

func (d *realtimeDiagnostics) add(e RealtimeDiagnostic) {
	if d.total >= diagnosticBudget || d.count == len(d.queue) {
		return
	}
	d.queue[(d.head+d.count)%len(d.queue)] = e
	d.count++
	d.total++
}

func (d *realtimeDiagnostics) take() (RealtimeDiagnostic, bool) {
	if d.count == 0 {
		return RealtimeDiagnostic{}, false
	}
	e := d.queue[d.head]
	d.queue[d.head] = RealtimeDiagnostic{}
	d.head = (d.head + 1) % len(d.queue)
	d.count--
	return e, true
}

func (r *realtimePlayback) trace(event string, p pcmPosition) {
	if r.diagnostics.total >= diagnosticBudget {
		return
	}
	r.diagnostics.add(RealtimeDiagnostic{Event: event, Stream: r.stream, Turn: r.turn,
		At: time.Now(), MonoNS: time.Since(diagnosticOrigin).Nanoseconds(),
		Owner: p.Owner, Written: p.Written, Played: p.Played,
		Fence: r.fence, Waiting: r.waiting, Reset: r.reset, Closing: r.closing != nil,
		EpochChanged: r.progress.initialized && (p.Owner != r.progress.last.Owner || p.Trigger != r.progress.last.Trigger)})
}

func (r *realtimePlayback) tracePosition(p pcmPosition, now time.Time) {
	d := &r.diagnostics
	if d.samples >= 12 || (!d.lastSample.IsZero() && now.Sub(d.lastSample) < 100*time.Millisecond) {
		return
	}
	d.samples++
	d.lastSample = now
	r.trace("position", p)
}

func (r *realtimePlayback) traceCommand(session uint32, stream [16]byte, turn uint32, event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diagnostics.add(RealtimeDiagnostic{Event: event, Stream: stream, Session: session,
		Turn: turn, At: time.Now(), MonoNS: time.Since(diagnosticOrigin).Nanoseconds()})
}
