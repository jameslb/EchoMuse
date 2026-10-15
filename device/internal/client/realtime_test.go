package client

import "testing"

func TestRealtimeCommandsRequireIdentityAndSession(t *testing.T) {
	valid := `{"type":"rt_open","stream":"01020300000000000000000000000000","turn":0,"session":7}`
	c, err := DecodeRealtimeCommand([]byte(valid))
	if err != nil || c.Session != 7 || c.Stream[0] != 1 {
		t.Fatal(c, err)
	}
	for _, raw := range []string{
		`{"type":"rt_open","stream":"01020300000000000000000000000000","turn":0}`,
		`{"type":"rt_open","stream":"01020300000000000000000000000000","turn":1,"session":7}`,
		`{"type":"rt_open","stream":"01020300000000000000000000000000","session":7}`,
		`{"type":"rt_open","stream":"00000000000000000000000000000000","turn":0,"session":7}`,
		`{"type":"rt_flush","stream":"01","turn":1}`,
		`{"type":"rt_flush","stream":"01020300000000000000000000000000","turn":-1}`,
		`{"type":"mic_start","stream":"01020300000000000000000000000000","turn":0}`,
	} {
		if _, err := DecodeRealtimeCommand([]byte(raw)); err == nil {
			t.Fatal("accepted", raw)
		}
	}
}

func TestRealtimeCapabilityRequiresWiredDriver(t *testing.T) {
	c := &ControlClient{}
	for _, cap := range c.registerCapabilities() {
		if cap == RealtimeCapability {
			t.Fatal("unwired capability")
		}
	}
	c.OnRealtime(func(RealtimeCommand) {})
	found := 0
	for _, cap := range c.registerCapabilities() {
		if cap == RealtimeCapability || cap == RealtimeNativeRate {
			found++
		}
	}
	if found != 2 {
		t.Fatal("missing rate/protocol negotiation")
	}
}
