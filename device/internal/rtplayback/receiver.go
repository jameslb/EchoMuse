// Package satellite implements the staged realtime_playback_v1 receiver.
// It has no socket, microphone, filesystem, authentication or hardware access.
// Firmware MUST NOT advertise this capability until its audio driver implements
// both barriers documented on Quiesced and Consumed. A successful queue write
// is never evidence that a person heard audio.
package satellite

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"
)

const HeaderSize = 37

var ErrProtocol = errors.New("invalid realtime playback frame or lifecycle")

type Receipt struct {
	Type   string `json:"type"`
	Stream string `json:"stream"`
	Turn   uint32 `json:"turn"`
	Seq    uint32 `json:"seq,omitempty"`
	Frames uint32 `json:"frames,omitempty"`
}

// Token survives queue removal. The driver must carry it to hardware completion
// so a flush between Take and the DAC cannot credit the replacement turn.
type Token struct {
	Stream                    [16]byte
	Turn, Seq, Offset, Frames uint32
}

type Chunk struct {
	Token Token
	Rate  uint32
	PCM   []byte
}

type segment struct {
	rate, total, received, taken, consumed uint32
}

type Receiver struct {
	mu                    sync.Mutex
	stream                [16]byte
	turn, lastSeq         uint32
	opened, closed, ready bool
	queue                 []Chunk
	segments              map[uint32]*segment
	order                 []uint32
}

// Open is called once per authenticated, exclusively owned device route.
// New routes use a fresh random stream ID; a closed instance cannot be reused.
func (r *Receiver) Open(stream [16]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.opened || r.closed || stream == [16]byte{} {
		return ErrProtocol
	}
	r.opened = true
	r.stream = stream
	r.segments = make(map[uint32]*segment)
	return nil
}

func (r *Receiver) clearLocked() {
	r.ready = false
	r.queue = nil
	r.segments = make(map[uint32]*segment)
	r.order = nil
	r.lastSeq = 0
}

// Flush retires the old epoch before discarding its buffers. The owner must
// also drop the downstream driver queue; Quiesced is a separate hardware fence.
func (r *Receiver) Flush(stream [16]byte, turn uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.opened || r.closed || stream != r.stream || turn <= r.turn {
		return false
	}
	r.turn = turn
	r.clearLocked()
	return true
}

// Quiesced may be called ONLY when no prior-epoch audio can still be emitted:
// downstream queues discarded AND the DAC play cursor passed their last frame
// (or a verified hardware stop completed). Do not call on a socket write, an
// unmeasured timer, software queue drain or ALSA enqueue. A stale fence is ignored.
func (r *Receiver) Quiesced(stream [16]byte, turn uint32) *Receipt {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.opened || r.closed || r.ready || stream != r.stream || turn != r.turn {
		return nil
	}
	r.ready = true
	return &Receipt{Type: "rt_ready", Stream: hex.EncodeToString(r.stream[:]), Turn: r.turn}
}

func validRate(rate uint32) bool {
	switch rate {
	case 8000, 16000, 22050, 24000, 44100, 48000:
		return true
	}
	return false
}

func (r *Receiver) invalidLocked() error {
	r.closed = true
	r.clearLocked()
	return ErrProtocol
}

// Push validates and owns a copy of one <=20ms S16LE mono block. Accepted
// segments reserve their full duration, capped at two segments / thirty seconds.
// A firmware driver that needs 48 kHz must resample with preserved token/frame
// mapping; it cannot simply reinterpret a 22.05 kHz buffer at 48 kHz.
func (r *Receiver) Push(wire []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.opened || r.closed {
		return ErrProtocol
	}
	if len(wire) < HeaderSize+2 || (len(wire)-HeaderSize)%2 != 0 || wire[0] != 9 {
		return r.invalidLocked()
	}
	var stream [16]byte
	copy(stream[:], wire[1:17])
	turn := binary.BigEndian.Uint32(wire[17:21])
	if stream != r.stream || turn < r.turn {
		return nil
	} // retired link/epoch
	if turn != r.turn || !r.ready {
		return r.invalidLocked()
	}
	seq := binary.BigEndian.Uint32(wire[21:25])
	rate := binary.BigEndian.Uint32(wire[25:29])
	offset := binary.BigEndian.Uint32(wire[29:33])
	total := binary.BigEndian.Uint32(wire[33:37])
	count := uint32((len(wire) - HeaderSize) / 2)
	if seq == 0 || !validRate(rate) || total == 0 || total > rate*30 ||
		count > rate/50 || offset >= total || count > total-offset ||
		(count != rate/50 && offset+count != total) {
		return r.invalidLocked()
	}
	s := r.segments[seq]
	if s == nil {
		if r.lastSeq == ^uint32(0) || seq != r.lastSeq+1 || offset != 0 || len(r.order) >= 2 {
			return r.invalidLocked()
		}
		seconds := float64(total) / float64(rate)
		for _, old := range r.segments {
			seconds += float64(old.total) / float64(old.rate)
		}
		if seconds > 30 {
			return r.invalidLocked()
		}
		if prev := r.segments[r.lastSeq]; prev != nil && prev.received != prev.total {
			return r.invalidLocked()
		}
		s = &segment{rate: rate, total: total}
		r.segments[seq] = s
		r.order = append(r.order, seq)
		r.lastSeq = seq
	}
	if seq != r.lastSeq || s.rate != rate || s.total != total || s.received != offset {
		return r.invalidLocked()
	}
	s.received += count
	r.queue = append(r.queue, Chunk{Token: Token{Stream: stream, Turn: turn, Seq: seq, Offset: offset, Frames: count},
		Rate: rate, PCM: append([]byte(nil), wire[HeaderSize:]...)})
	return nil
}

// Take is software queue removal, NOT a playback receipt.
func (r *Receiver) Take() (Chunk, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.ready || r.closed || len(r.queue) == 0 {
		return Chunk{}, false
	}
	chunk := r.queue[0]
	r.queue[0] = Chunk{}
	r.queue = r.queue[1:]
	r.segments[chunk.Token.Seq].taken += chunk.Token.Frames
	return chunk, true
}

// Consumed is called only after ALL source frames represented by the token
// crossed the measured DAC playback cursor. Resampling must map output cursor
// to source-token boundaries conservatively. Returns at most start+done receipts.
// A flushed token is ignored, including a late callback from a retired route.
func (r *Receiver) Consumed(token Token) ([]Receipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || !r.ready || token.Stream != r.stream || token.Turn != r.turn {
		return nil, nil
	}
	s := r.segments[token.Seq]
	if s == nil {
		return nil, nil
	}
	if token.Offset < s.consumed {
		return nil, nil
	} // duplicate completion callback
	if len(r.order) == 0 || r.order[0] != token.Seq || token.Frames == 0 || token.Offset != s.consumed ||
		token.Frames > s.taken-s.consumed {
		return nil, r.invalidLocked()
	}
	var receipts []Receipt
	base := Receipt{Stream: hex.EncodeToString(r.stream[:]), Turn: r.turn, Seq: token.Seq}
	if s.consumed == 0 {
		start := base
		start.Type = "rt_played"
		receipts = append(receipts, start)
	}
	s.consumed += token.Frames
	if s.consumed == s.total {
		done := base
		done.Type = "rt_done"
		done.Frames = s.total
		receipts = append(receipts, done)
		delete(r.segments, token.Seq)
		r.order = r.order[1:]
	}
	return receipts, nil
}

// Close is mandatory on either link loss, mute, route release or device expiry.
// The owner must additionally stop/discard the actual hardware output queue.
func (r *Receiver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.clearLocked()
}
