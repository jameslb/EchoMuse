# Local realtime voice candidate

This branch is an implementation candidate, not an installed release. Existing
devices and controller deployments are unchanged. It introduces no new listener,
credentials, firmware download, microphone recording, or default route.

## Device path

The server wires the bounded `internal/rtplayback` receiver into `PcmSpeaker`.
Control/data links negotiate both `realtime_playback_v1` and
`realtime_pcm_48000_v1`. `rt_open` must name an already-open private microphone
session; it cannot open or renew that session. The original three-second ack and
thirty-second absolute device deadlines, hardware mute and link-loss closures
remain authoritative.

Tagged 0x09 mono S16LE packets are 48 kHz, at most 20 ms each. Two segments and
thirty seconds of source audio are the maximum reservation. The ALSA writer
carries source-token boundaries through stereo mixing and the existing output
chain. Completion includes the chain's lookahead delay and is based on
`appl_ptr - delay` from the board-resolved playback status, not elapsed time or
socket writes. The AEC reference still sees the actual mixed output.

Flush retires the old turn immediately, discards queued speech and DSP history,
and acknowledges `rt_ready` only when the previous write boundary has passed the
measured DAC cursor. Close similarly emits `rt_closed` only after that fence.
Native voice cannot acquire the speaker before close completes. Missing,
inconsistent or reset hardware counters fail closed; they never fabricate a
playback receipt. Hardware acceptance must confirm these counters on the Dot.

## Controller path

`em_realtime.register(device_id, route_factory)` is an explicit, in-process
extension point. Nothing registers by default. Factory arguments are
`(send_control, send_data, capabilities=..., private_state='local', exclusive=True)`.
The factory returns an object supporting asynchronous `wake`, `frame`, `control`,
`close` and `wait_closed` methods. The sibling prototype's `EchoRoute` implements
this interface when supplied its `create_conversation` callback.

Only registered devices with authenticated TLS and both capabilities attach.
Wake validation and cross-device arbitration use the existing controller code;
disabled wakes, mute, native busy state and announcements retain their native
owner. Wake setup runs separately from the control reader so waiting for a
hardware acknowledgment cannot deadlock the very reader that delivers it.
Mic frames have one consumer. Device control receipts remain independent of
outgoing PCM backpressure.

The binding pins both WebSocket objects. Send failures propagate and replacement
sockets cannot silently inherit old audio. A native announcement or button turn
closes the candidate route and waits for `rt_closed` before sending legacy voice.
After this handover a new user wake or its private-session frame may attach a
fresh route only once native playback is idle and the same authenticated links
remain. This preserves data-before-control buffering. Unmute, timers and socket
replacement cannot rearm it or reopen a microphone.

## Host validation

Use the approved, pinned Go/compiler images with networking disabled and sources
read-only. Host scope is `go test -mod=readonly -race -count=1 ./internal/... ./pkg/...`.
The parent repository's `Build-EchoMuseCandidate.ps1` performs the offline Android
ARM build and writes source/dependency/artifact hashes. It does not install it.

Controller binding checks need only Python's standard library:

```
python -m unittest discover -s controller/tests -p test_realtime_binding.py -v
```

The sibling prototype adds real route/controller composition tests, local
resampler tests, privacy/transport contracts and a synthetic speech replay.
Those tests explicitly simulate the DAC; they do not certify physical AEC.

## Remaining integration gates

1. Host the backend and explicitly register the device factory over a reviewed,
   authenticated bridge. The extension point alone is not a deployed backend.
2. Confirm native announcement, alarm and button handover/recovery on hardware.
3. Verify driver status/owner/counters and lease expiry on actual hardware, then
   test physical mute, either-link loss, old-frame rejection and ordinary-speech
   interruption with the real AEC reference. Installation requires separate approval.
4. Implement and validate the independent Voice PE transport and standard voice.
5. Retain the existing production pipeline until capability parity passes.

No GoTinyAlsa modification or submodule revision change is required by this
candidate. A new microphone policy beyond thirty seconds is not implied.
