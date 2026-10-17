//go:build server

package main

import (
	"context"
	"sync"
	"time"

	"github.com/wilbowes/EchoMuse/internal/aecdiag"
	"github.com/wilbowes/EchoMuse/internal/bindings/speaker"
	"github.com/wilbowes/EchoMuse/internal/client"
)

// Existing mutually authenticated control/data connections only. No listener,
// new credentials, mic_start or lease renewal. Hardware mute remains sovereign.
func wireRealtime(ctx context.Context, c *client.ControlClient, d *client.DataClient, p *speaker.PcmSpeaker, muted func() bool) {
	var mu sync.Mutex
	var session uint32
	allowed := func() bool {
		_, ok := d.ListenLease(session)
		return ok && !muted() && c.HasFeature(client.RealtimeCapability)
	}
	c.OnRealtime(func(command client.RealtimeCommand) {
		mu.Lock()
		defer mu.Unlock()
		if command.Type == "rt_close" {
			if p.CloseRealtimeFor(command.Stream, command.Turn) {
				d.AECProbe.StopFor(command.Stream)
			}
			return
		}
		if command.Type == "rt_open" {
			deadline, ok := d.ListenLease(command.Session)
			if !ok || muted() || !c.HasFeature(client.RealtimeNativeRate) {
				return
			}
			if p.OpenRealtime(command.Stream, deadline) == nil {
				session = command.Session
				// Missing/invalid/consumed permit leaves capture disabled.
				_ = d.AECProbe.Start(aecdiag.Root, session, command.Stream, deadline)
			}
			return
		}
		if !allowed() {
			p.CloseRealtime()
			d.AECProbe.Stop("privacy_closed")
			return
		}
		p.FlushRealtime(command.Stream, command.Turn)
	})
	d.OnRealtime(func(wire []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if !allowed() {
			p.CloseRealtime()
			d.AECProbe.Stop("privacy_closed")
			return nil
		}
		return p.PushRealtime(wire)
	}, func() { p.CloseRealtime(); d.AECProbe.Stop("disconnected") })
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				p.CloseRealtime()
				d.AECProbe.Stop("shutdown")
				return
			case <-ticker.C:
			}
			mu.Lock()
			if !allowed() {
				p.CloseRealtime()
				d.AECProbe.Stop("privacy_closed")
			}
			mu.Unlock()
			for {
				event, ok := p.RealtimeReceipt()
				if !ok {
					break
				}
				if err := c.SendRealtimeReceipt(event); err != nil {
					p.CloseRealtime()
					d.AECProbe.Stop("control_disconnected")
					break
				}
			}
		}
	}()
}
