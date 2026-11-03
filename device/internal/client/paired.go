package client

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/gorilla/websocket"
	"github.com/wilbowes/EchoMuse/internal/aec"
	"github.com/wilbowes/EchoMuse/internal/aecdiag"
	"github.com/wilbowes/EchoMuse/internal/pairedinput"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

func pairedNow() int64 { return aecdiag.Stamp() }

// EnablePairedInput is intentionally never called by the default build wiring.
// It adds no microphone command/lease and uses the existing authenticated links.
func (d *DataClient) EnablePairedInput(ctx context.Context, c *ControlClient, muted func() bool) {
	if d.paired != nil {
		return
	} // construction-time opt-in, never spawn a second exporter
	s := pairedinput.New(func(session uint32) (time.Time, bool) {
		if muted == nil || muted() || !c.HasFeature(pairedinput.Capability) {
			return time.Time{}, false
		}
		return d.ListenLease(session)
	}, pairedNow)
	d.paired = s
	var bindingMu sync.Mutex
	var boundData *websocket.Conn
	var boundControl *websocket.Conn
	var boundControlSerial uint64
	var boundID pairedinput.Identity
	c.pairedClosed = s.ResetLink
	c.pairedCommand = func(raw []byte) {
		command, err := pairedinput.Decode(raw)
		if err != nil {
			return
		}
		c.connMu.Lock()
		control := c.conn
		controlSerial := c.pairedControlSerial.Load()
		c.connMu.Unlock()
		d.connMu.Lock()
		data := d.conn
		d.connMu.Unlock()
		secure := false
		if control != nil && data != nil {
			_, a := control.UnderlyingConn().(*tls.Conn)
			_, b := data.UnderlyingConn().(*tls.Conn)
			secure = a && b
		}
		_ = s.Control(command, time.Now(), secure, c.HasFeature(pairedinput.Capability), func() {
			d.pairedMicSerial.Store(atomic.LoadUint64(&d.diagMicSerial))
			bindingMu.Lock()
			boundData = data
			boundControl = control
			boundControlSerial = controlSerial
			boundID = command.Identity
			bindingMu.Unlock()
		})
	}
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		defer s.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if event, ok := s.Event(time.Now()); ok {
				bindingMu.Lock()
				expected, id := boundControl, boundID
				bindingMu.Unlock()
				if event.Identity != id {
					continue
				}
				if c.sendPairedEvent(event, expected) != nil {
					s.CloseIdentity(event.Identity)
					continue
				}
			}
			for i := 0; i < pairedinput.QueueLimit; i++ {
				frame, err := s.Poll(time.Now(), pairedNow())
				if err != nil || frame == nil {
					break
				}
				// Recheck the actual lease/mute immediately before each send. Poll does
				// not hold the sender mutex while waiting on the existing data socket.
				bindingMu.Lock()
				expected, id, controlSerial := boundData, boundID, boundControlSerial
				bindingMu.Unlock()
				packetID, decodeErr := pairedinput.FrameIdentity(frame)
				if decodeErr != nil || packetID != id {
					continue
				}
				if c.pairedControlSerial.Load() != controlSerial {
					s.CloseIdentity(packetID)
					continue
				}
				state := d.aec.DiagnosticState()
				filter := pairedinput.Filter{Generation: state.Generation, Domain: state.Domain, Status: state.LoadStatus, SHA256: state.LoadSHA256, Hardware: true}
				d.connMu.Lock()
				conn := d.conn
				if conn == nil || conn != expected {
					d.connMu.Unlock()
					s.CloseIdentity(packetID)
					break
				}
				if !s.SendAllowed(packetID, filter, time.Now()) {
					d.connMu.Unlock()
					continue
				}
				conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
				wire := append([]byte{pairedinput.FrameType}, frame...)
				err = conn.WriteMessage(2, wire)
				d.connMu.Unlock()
				if err != nil {
					s.CloseIdentity(packetID)
					conn.Close() // gorilla write errors retire this socket; never reuse it
					break
				}
			}
		}
	}()
}

func (c *ControlClient) sendPairedEvent(event pairedinput.Ready, expected *websocket.Conn) error {
	wire, err := json.Marshal(event)
	if err != nil {
		return err
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil || c.conn != expected {
		return errors.New("retired paired control link")
	}
	c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return c.conn.WriteMessage(websocket.TextMessage, wire)
}

// ClosePairedInputForStream leaves a different/newer stream untouched.
func (d *DataClient) ClosePairedInputForStream(stream [16]byte) {
	if d.paired != nil {
		d.paired.CloseStream(hex.EncodeToString(stream[:]))
	}
}

// offerPaired observes the unmodified post-AEC monitor. It never runs diagnostics
// or writes files. Observation time bounds queue age, not acoustic capture age.
func (d *DataClient) offerPaired(mono, ref []byte, gain float64, filter aec.FilterDiagnostic, hardware bool, sample uint64, observed int64, micSerial uint64) {
	if d.paired == nil {
		return
	}
	if micSerial != d.pairedMicSerial.Load() {
		return
	} // late callback from a retired mic goroutine
	if len(mono) != len(ref) || len(mono)%1024 != 0 || gain <= 0 || math.IsNaN(gain) || math.IsInf(gain, 0) {
		d.paired.Close()
		return
	}
	n := len(mono) / 2
	f := pairedinput.Filter{Generation: filter.Generation, Domain: filter.Domain, Status: filter.LoadStatus, SHA256: filter.LoadSHA256, Hardware: hardware}
	for off := 0; off < n; off += pairedinput.Samples {
		b, err := pairedinput.FromGainedPCM(mono[off*2:(off+pairedinput.Samples)*2], ref[off*2:(off+pairedinput.Samples)*2], gain, sample+uint64(off), observed)
		if err != nil {
			d.paired.Close()
			return
		}
		if d.paired.Capture(b, f, time.Now(), pairedNow()) != nil {
			return
		}
	}
}
