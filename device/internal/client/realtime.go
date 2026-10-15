package client

import (
	"encoding/hex"
	"encoding/json"
	"errors"
)

const RealtimeCapability = "realtime_playback_v1"
const RealtimeNativeRate = "realtime_pcm_48000_v1"

type RealtimeCommand struct {
	Type          string
	Stream        [16]byte
	Turn, Session uint32
}

func DecodeRealtimeCommand(raw []byte) (RealtimeCommand, error) {
	var result RealtimeCommand
	var wire struct {
		Type    string  `json:"type"`
		Stream  string  `json:"stream"`
		Turn    *uint32 `json:"turn"`
		Session uint32  `json:"session"`
	}
	bad := errors.New("invalid realtime command")
	if len(raw) > 512 || json.Unmarshal(raw, &wire) != nil || wire.Turn == nil {
		return result, bad
	}
	id, err := hex.DecodeString(wire.Stream)
	if err != nil || len(id) != 16 {
		return result, bad
	}
	copy(result.Stream[:], id)
	if result.Stream == [16]byte{} {
		return RealtimeCommand{}, bad
	}
	switch wire.Type {
	case "rt_open":
		if *wire.Turn != 0 || wire.Session == 0 {
			return RealtimeCommand{}, bad
		}
	case "rt_flush", "rt_close":
	default:
		return RealtimeCommand{}, bad
	}
	result.Type, result.Turn, result.Session = wire.Type, *wire.Turn, wire.Session
	return result, nil
}

// Presence here means the server wired the measured driver and privacy hooks.
// An older controller never announces the feature, so receives native behavior.
func (c *ControlClient) registerCapabilities() []string {
	caps := capabilities()
	if c.realtimeCallback != nil {
		caps = append(caps, RealtimeCapability, RealtimeNativeRate)
	}
	return caps
}
func (c *ControlClient) OnRealtime(cb func(RealtimeCommand)) { c.realtimeCallback = cb }
func (c *ControlClient) SendRealtimeReceipt(event any) error { return c.writeJSON(event) }
