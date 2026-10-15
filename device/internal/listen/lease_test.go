package listen

import (
	"testing"
	"time"
)

func TestPlaybackLeaseCannotOpenOrRenewMicrophone(t *testing.T) {
	g := New(0, 0, 0)
	now := time.Unix(1000, 0)
	if _, ok := g.Lease(1, now); ok {
		t.Fatal("closed lease accepted")
	}
	id, ok := g.Open(now, now)
	if !ok {
		t.Fatal("wake rejected")
	}
	deadline, ok := g.Lease(id, now.Add(time.Second))
	if !ok || !deadline.Equal(now.Add(DefaultMaxOpen)) {
		t.Fatal(deadline, ok)
	}
	if _, ok := g.Lease(id, now.Add(DefaultAckTimeout)); ok {
		t.Fatal("unacked lease extended")
	}
	g.Ack(id)
	deadline, ok = g.Lease(id, now.Add(20*time.Second))
	if !ok || !deadline.Equal(now.Add(DefaultMaxOpen)) {
		t.Fatal("ack renewed deadline")
	}
	if _, ok := g.Lease(id+1, now); ok {
		t.Fatal("wrong session")
	}
	if _, ok := g.Lease(id, now.Add(DefaultMaxOpen)); ok {
		t.Fatal("expired session")
	}
	g.Close(id)
	if _, ok := g.Lease(id, now); ok {
		t.Fatal("closed session revived")
	}
}
