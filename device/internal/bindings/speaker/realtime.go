package speaker

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	satellite "github.com/wilbowes/EchoMuse/internal/rtplayback"
)

var errRealtime = errors.New("realtime playback closed or invalid")

type playbackMark struct {
	token satellite.Token
	end   uint64
}

// cycleMu serializes lifecycle commands with complete mix/write cycles so a
// flush cannot acknowledge a period that has not reached hardware. mu protects
// queue/state only: producers must remain free during a blocking ALSA write.
type realtimePlayback struct {
	cycleMu        sync.Mutex
	mu             sync.Mutex
	receiver       *satellite.Receiver
	stream         [16]byte
	turn           uint32
	expires        time.Time
	progress       pcmProgress
	fence          uint64
	waiting, reset bool
	partial        satellite.Chunk
	used           int
	marks          []playbackMark
	events         []satellite.Receipt
	closing        *satellite.Receipt
	diagnostics    realtimeDiagnostics
}

func (r *realtimePlayback) closeLocked() {
	if r.receiver != nil {
		r.trace("closing", r.progress.last)
		r.receiver.Close()
		r.closing = &satellite.Receipt{Type: "rt_closed", Stream: hex.EncodeToString(r.stream[:]), Turn: r.turn}
	}
	r.receiver = nil
	r.partial = satellite.Chunk{}
	r.used = 0
	r.marks = nil
	r.events = nil
	r.waiting = false
	r.reset = true // discard DSP delay/tails on the next ALSA cycle, too
}

func (r *realtimePlayback) open(stream [16]byte, now, expires time.Time) error {
	r.cycleMu.Lock()
	defer r.cycleMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.receiver != nil || r.closing != nil || !expires.After(now) || expires.Sub(now) > 30*time.Second {
		r.trace("open_rejected", r.progress.last)
		return errRealtime
	}
	q := &satellite.Receiver{}
	if err := q.Open(stream); err != nil {
		return err
	}
	r.receiver, r.stream, r.turn, r.expires = q, stream, 0, expires
	r.progress = pcmProgress{}
	r.partial, r.used, r.marks, r.events = satellite.Chunk{}, 0, nil, nil
	r.waiting, r.reset, r.fence = true, true, 0
	r.diagnostics.samples, r.diagnostics.lastSample = 0, time.Time{}
	r.trace("opened", pcmPosition{})
	return nil
}

func (r *realtimePlayback) flush(stream [16]byte, turn uint32) bool {
	r.cycleMu.Lock()
	defer r.cycleMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.receiver == nil || !r.receiver.Flush(stream, turn) {
		return false
	}
	r.turn = turn
	r.partial, r.used, r.marks, r.events = satellite.Chunk{}, 0, nil, nil
	r.waiting, r.reset, r.fence = true, true, 0
	r.trace("flush", r.progress.last)
	return true
}

func (r *realtimePlayback) beginCycle() {
	r.cycleMu.Lock()
	r.mu.Lock()
}

func (r *realtimePlayback) endCycle() {
	r.mu.Unlock()
	r.cycleMu.Unlock()
}

// Called with both locks held. Keep the lifecycle fence while letting network
// frames and receipt readers progress during a hardware-paced write. A push
// error can retire the route here; written() preserves its pending reset fence.
func (r *realtimePlayback) writePCM(write func() error) error {
	r.mu.Unlock()
	defer r.mu.Lock()
	return write()
}

func (r *realtimePlayback) push(wire []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// This binding is native 48 kHz. Conversion belongs to the controller;
	// accepting another rate would silently change voice pitch and cadence.
	if len(wire) < satellite.HeaderSize+2 || len(wire) > satellite.HeaderSize+1920 {
		r.closeLocked()
		return errRealtime
	}
	var stream [16]byte
	copy(stream[:], wire[1:17])
	turn := binary.BigEndian.Uint32(wire[17:21])
	if r.receiver == nil {
		// Control and data travel on separate sockets. Packets already sent
		// for this retired route can arrive after close; they must not tear
		// down the data link just as native playback is taking it back.
		if stream == r.stream && stream != [16]byte{} && turn <= r.turn {
			return nil
		}
		return errRealtime
	}
	if stream != r.stream || turn < r.turn {
		return nil
	}
	if binary.BigEndian.Uint32(wire[25:29]) != 48000 {
		r.closeLocked()
		return errRealtime
	}
	if err := r.receiver.Push(wire); err != nil {
		r.closeLocked()
		return err
	}
	return nil
}

func (r *realtimePlayback) emit(event satellite.Receipt) bool {
	if len(r.events) >= 64 {
		r.closeLocked()
		return false
	}
	r.events = append(r.events, event)
	if event.Type == "rt_ready" {
		r.trace("rt_ready_queued", r.progress.last)
	} else if event.Type == "rt_closed" {
		r.trace("rt_closed_queued", r.progress.last)
	}
	return true
}

// observe is called only by the ALSA writer, with the mutex held. reset means
// the caller has discarded the native voice queue AND output-chain history.
// The fence includes everything written before that discard, never a timer.
func (r *realtimePlayback) observe(p pcmPosition, now time.Time) bool {
	if r.receiver == nil && r.closing == nil {
		return false
	}
	if !r.progress.observe(p) {
		r.trace("progress_invalid", p)
		r.closeLocked()
		return false
	}
	if r.receiver != nil && !now.Before(r.expires) {
		r.trace("lease_expired", p)
		r.closeLocked()
		return false
	}
	if r.reset {
		r.fence, r.reset = p.Written, false
		r.trace("fence_set", p)
	}
	r.tracePosition(p, now)
	if r.closing != nil {
		if p.Played >= r.fence {
			event := *r.closing
			r.closing = nil
			r.emit(event)
		}
		return false
	}
	if r.waiting {
		if p.Played < r.fence {
			return true
		}
		r.waiting = false
		if event := r.receiver.Quiesced(r.stream, r.turn); event != nil {
			if !r.emit(*event) {
				return false
			}
		}
	}
	for len(r.marks) > 0 && p.Played >= r.marks[0].end {
		mark := r.marks[0]
		r.marks = r.marks[1:]
		events, err := r.receiver.Consumed(mark.token)
		if err != nil {
			r.closeLocked()
			return false
		}
		for _, event := range events {
			if !r.emit(event) {
				return false
			}
		}
	}
	return true
}

// render produces a native stereo period, carrying each completed source
// token's exact end offset. A token split across periods completes only once.
// The writer turns these offsets into measured absolute ALSA frame boundaries.
func (r *realtimePlayback) render(frames int) ([]byte, []playbackMark) {
	if r.receiver == nil || r.waiting {
		return nil, nil
	}
	var out []byte
	var marks []playbackMark
	for at := 0; at < frames; {
		if len(r.partial.PCM) == 0 {
			chunk, ok := r.receiver.Take()
			if !ok {
				break
			}
			r.partial, r.used = chunk, 0
		}
		if out == nil {
			out = make([]byte, frames*4)
		}
		count := min(frames-at, len(r.partial.PCM)/2-r.used)
		for i := 0; i < count; i++ {
			src, dst := (r.used+i)*2, (at+i)*4
			out[dst], out[dst+1] = r.partial.PCM[src], r.partial.PCM[src+1]
			out[dst+2], out[dst+3] = out[dst], out[dst+1]
		}
		at, r.used = at+count, r.used+count
		if r.used == len(r.partial.PCM)/2 {
			marks = append(marks, playbackMark{token: r.partial.Token, end: uint64(at)})
			r.partial, r.used = satellite.Chunk{}, 0
		}
	}
	return out, marks
}

func (r *realtimePlayback) written(before, after pcmPosition, frames, delay int, marks []playbackMark, now time.Time) bool {
	// A deadline may close the route during begin-cycle observation. Only
	// beginRealtimeCycle can discard DSP history; do not consume that reset
	// flag on this post-write sample and acknowledge still-buffered tails.
	if r.reset {
		return false
	}
	if r.receiver == nil && r.closing == nil {
		return false
	}
	if after.Written < before.Written || after.Written-before.Written != uint64(frames) || delay < 0 {
		r.trace("write_boundary_invalid", after)
		r.closeLocked()
		return false
	}
	for _, mark := range marks {
		mark.end += before.Written + uint64(delay)
		r.marks = append(r.marks, mark)
	}
	return r.observe(after, now)
}
