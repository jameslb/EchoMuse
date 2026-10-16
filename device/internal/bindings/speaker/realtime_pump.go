//go:build server

package speaker

import (
	"os"
	"time"

	satellite "github.com/wilbowes/EchoMuse/internal/rtplayback"
)

// The control/data binding enforces authenticated ownership, session expiry,
// physical mute and either-link loss before opting into this driver. It
// advertises the capability only when these hooks are actually installed.
func (p *PcmSpeaker) OpenRealtime(stream [16]byte, expires time.Time) error {
	return p.realtime.open(stream, time.Now(), expires)
}

func (p *PcmSpeaker) FlushRealtime(stream [16]byte, turn uint32) bool {
	return p.realtime.flush(stream, turn)
}

func (p *PcmSpeaker) PushRealtime(wire []byte) error { return p.realtime.push(wire) }

func (p *PcmSpeaker) CloseRealtime() {
	p.realtime.cycleMu.Lock()
	defer p.realtime.cycleMu.Unlock()
	p.realtime.mu.Lock()
	defer p.realtime.mu.Unlock()
	if p.realtime.receiver != nil {
		p.realtime.closeLocked()
	}
}

func (p *PcmSpeaker) CloseRealtimeFor(stream [16]byte, turn uint32) {
	p.realtime.cycleMu.Lock()
	defer p.realtime.cycleMu.Unlock()
	p.realtime.mu.Lock()
	defer p.realtime.mu.Unlock()
	if p.realtime.receiver != nil && p.realtime.stream == stream && p.realtime.turn == turn {
		p.realtime.closeLocked()
	}
}

func (p *PcmSpeaker) RealtimeOwned() bool {
	p.realtime.mu.Lock()
	defer p.realtime.mu.Unlock()
	return p.realtime.receiver != nil || p.realtime.closing != nil
}

// Drain on a control worker, never the ALSA writer. Queues are bounded; a slow
// link closes the route instead of blocking the speaker or leaking audio.
func (p *PcmSpeaker) RealtimeReceipt() (satellite.Receipt, bool) {
	p.realtime.mu.Lock()
	defer p.realtime.mu.Unlock()
	if len(p.realtime.events) == 0 {
		return satellite.Receipt{}, false
	}
	event := p.realtime.events[0]
	p.realtime.events = p.realtime.events[1:]
	return event, true
}

func (p *PcmSpeaker) realtimePosition() (pcmPosition, bool) {
	b, err := os.ReadFile(p.statusFile)
	if err != nil {
		return pcmPosition{}, false
	}
	return pcmPlaybackPosition(string(b), uint64(os.Getpid()))
}

// The lifecycle lock spans prepare, mix, AEC tap and both hardware writes. Thus a
// concurrently received flush retires the previous cycle as a unit. No socket
// write, callback, resampling or unbounded queue wait occurs under this lock.
func (p *PcmSpeaker) beginRealtimeCycle() (bool, pcmPosition) {
	r := &p.realtime
	r.beginCycle()
	owned := r.receiver != nil || r.closing != nil
	if r.reset {
		p.voice.flush()
		p.chain.DiscardHistory()
		if !owned {
			r.reset = false
		}
	}
	if !owned {
		return false, pcmPosition{}
	}
	// Discard any legacy voice packet racing exclusive-route acquisition.
	p.voice.flush()
	position, ok := p.realtimePosition()
	if !ok {
		r.closeLocked()
		return true, pcmPosition{}
	}
	r.observe(position, time.Now())
	return true, position
}

func (p *PcmSpeaker) endRealtimeCycle(before pcmPosition, marks []playbackMark) {
	r := &p.realtime
	defer r.endCycle()
	if r.receiver == nil && r.closing == nil {
		return
	}
	after, ok := p.realtimePosition()
	if !ok {
		r.closeLocked()
		return
	}
	r.written(before, after, periodSize, p.chain.DelayFrames(), marks, time.Now())
}
