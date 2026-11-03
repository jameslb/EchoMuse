package client

import (
	"bytes"
	"encoding/binary"
	"github.com/gorilla/websocket"
	"github.com/wilbowes/EchoMuse/internal/aec"
	"github.com/wilbowes/EchoMuse/internal/pairedinput"
	"math"
	"strings"
	"testing"
	"time"
)

func TestPairedCapabilityIsOffUnlessHookRegistered(t *testing.T) {
	c := &ControlClient{}
	contains := func() bool {
		for _, x := range c.registerCapabilities() {
			if x == pairedinput.Capability {
				return true
			}
		}
		return false
	}
	if contains() {
		t.Fatal("default advertised")
	}
	c.pairedCommand = func([]byte) {}
	if !contains() {
		t.Fatal("explicit hook not advertised")
	}
}
func TestPairedRetiredControlNeverPretendsEventDelivery(t *testing.T) {
	c := &ControlClient{}
	event := pairedinput.Ready{Type: "pair_ready"}
	if c.sendPairedEvent(event, nil) == nil {
		t.Fatal("nil connection reported success")
	}
	c.conn = &websocket.Conn{}
	if c.sendPairedEvent(event, &websocket.Conn{}) == nil {
		t.Fatal("replacement link used")
	}
}
func TestPairedActualTapUsesMatchingMicEpochAndCurrentGain(t *testing.T) {
	now := time.Now()
	deadline := now.Add(time.Second)
	s := pairedinput.New(func(uint32) (time.Time, bool) { return deadline, true }, pairedNow)
	id := pairedinput.Identity{Stream: strings.Repeat("31", 16), Epoch: 7, Session: 9}
	if e := s.Control(pairedinput.Command{Type: "pair_open", Identity: id}, now, true, true); e != nil {
		t.Fatal(e)
	}
	d := &DataClient{paired: s}
	d.pairedMicSerial.Store(4)
	f := aec.FilterDiagnostic{Generation: 3, Domain: "unity_f32_s24", LoadStatus: "loaded", LoadSHA256: strings.Repeat("42", 32)}
	pcm := make([]byte, 1024)
	ref := make([]byte, 1024)
	binary.LittleEndian.PutUint16(pcm, 25)
	binary.LittleEndian.PutUint16(ref, 65413)
	original := append([]byte(nil), pcm...)
	d.offerPaired(pcm, ref, 10, f, true, 0, pairedNow(), 3)
	if _, ok := s.Event(time.Now()); ok {
		t.Fatal("retired mic callback entered new context")
	}
	d.offerPaired(pcm, ref, 10, f, true, 0, pairedNow(), 4)
	if _, ok := s.Event(time.Now()); !ok {
		t.Fatal("current tap not ready")
	}
	if e := s.Control(pairedinput.Command{Type: "pair_ack", Identity: id}, time.Now(), true, true); e != nil {
		t.Fatal(e)
	}
	for i, gain := range []float64{10, 20} {
		if i > 0 {
			d.offerPaired(pcm, ref, gain, f, true, 512, pairedNow(), 4)
		}
		frame, e := s.Poll(time.Now(), pairedNow())
		if e != nil || frame == nil {
			t.Fatal(e)
		}
		got := math.Float32frombits(binary.LittleEndian.Uint32(frame[56:]))
		if got != float32(25/gain) {
			t.Fatal("stale gain", got, gain)
		}
	}
	if !bytes.Equal(pcm, original) {
		t.Fatal("tap altered normal mono input")
	}
}
