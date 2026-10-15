package speaker

import (
	"strconv"
	"strings"
)

// pcmPosition is a measured playback fence, not an estimate from elapsed time.
// The candidate tagged speaker's single ALSA writer reads status after its write and
// carry the resulting Written boundary alongside the source segment token.
type pcmPosition struct {
	Owner           uint64
	Trigger         string
	Written, Played uint64
}

// pcmPlaybackPosition uses appl_ptr - delay, not just hw_ptr: delay also
// includes frames in the driver's FIFO. Counter wrap, XRUN, a foreign owner,
// missing/duplicate fields and inconsistent snapshots fail closed. The caller
// must resolve the playback substream by board name, as PcmSpeaker already does.
func pcmPlaybackPosition(status string, owner uint64) (pcmPosition, bool) {
	var result pcmPosition
	fields := make(map[string]string)
	for _, line := range strings.Split(status, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		switch key {
		case "state", "owner_pid", "trigger_time", "delay", "hw_ptr", "appl_ptr":
			if _, duplicate := fields[key]; duplicate {
				return result, false
			}
			fields[key] = value
		}
	}
	if fields["state"] != "RUNNING" || owner == 0 {
		return result, false
	}
	parse := func(key string) (uint64, bool) {
		value, err := strconv.ParseUint(fields[key], 10, 64)
		return value, err == nil
	}
	pid, pidOK := parse("owner_pid")
	written, writtenOK := parse("appl_ptr")
	hardware, hardwareOK := parse("hw_ptr")
	delay, delayOK := parse("delay")
	trigger := fields["trigger_time"]
	if !pidOK || pid != owner || !writtenOK || !hardwareOK || !delayOK ||
		trigger == "" || trigger == "0.000000000" || hardware > written ||
		delay > written || delay < written-hardware {
		return result, false
	}
	result = pcmPosition{Owner: pid, Trigger: trigger, Written: written, Played: written - delay}
	return result, true
}

// pcmProgress rejects resets and regressing samples. A new PCM session needs a
// new tracker; an old segment token must never be acknowledged on a new device
// epoch just because its numerical frame boundary happens to be smaller.
type pcmProgress struct {
	last                pcmPosition
	initialized, failed bool
}

func (p *pcmProgress) observe(position pcmPosition) bool {
	if p.failed || position.Played > position.Written || position.Owner == 0 || position.Trigger == "" {
		p.failed = true
		return false
	}
	if p.initialized && (position.Owner != p.last.Owner || position.Trigger != p.last.Trigger ||
		position.Written < p.last.Written || position.Played < p.last.Played) {
		p.failed = true
		return false
	}
	p.last, p.initialized = position, true
	return true
}

func (p *pcmProgress) reached(boundary uint64) bool {
	return p.initialized && !p.failed && boundary != 0 && p.last.Played >= boundary
}
