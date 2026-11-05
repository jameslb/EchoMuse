# Experimental paired-input hooks

Implemented for offline validation; **OFF by default and not a deployment**.
`pairedInputExperimental` is false. Existing route registration defaults to
`paired_input=False`; no bootstrap selects the paired mode. Ordinary mono audio,
VAD, speaker flush, physical fences and completion receipts remain unchanged.

The capability `phase5_paired_aec_input_v1` must be present on both authenticated
TLS links. An explicitly opted-in controller route must retain strict wake
ownership. Negotiation uses `pair_open`, `pair_ready`, `pair_ack`, `pair_close`
and `pair_closed`, bound to stream, connection epoch and existing listen session.
Nothing opens a microphone or creates/renews a lease. There is no data export
before ACK. Missing/refused/nonfloat/nonhardware filter state is rejected rather
than reported ready. A loaded bank is provenance, not proof of acoustic quality.

The device tap observes the existing pre-NS/AGC post-AEC S16 monitor and divides
by the actual gain snapshot for that batch. `encoding` explicitly reports
`unity_f32_from_gained_s16_v1`: these are not recovered ADC float samples, and
upstream clipping is not repaired. Filter metadata is read atomically with AEC
processing. Late callbacks from retired microphone goroutines are ignored.

Frame type `0x0a` contains the `P5I1` payload: stream16, epoch64, session32,
sequence32, sample64, timestamp64, filter-generation32 (network byte order), then
512 float32 residual and512 S16 hardware-reference samples (little endian).
Payload is3128bytes plus the outer type byte; source reference is frame aligned.
Unknown/unselected paired data is never passed to native STT.

The source clock is the existing monotonic microphone **observation** clock,
declared as `mic_observation_v1`, not a hardware/acoustic timestamp. The controller
uses the bounded request/ready interval to conservatively map queue age. It does
not infer physical latency from it. Negotiation allows one native160ms input
batch within a250ms host bound; device READY-to-ACK remains100ms. Device pending
preroll/egress is at most one five-period batch. Unpublished preroll may roll
before ACK; once active, gaps/overflow close and purge instead of splicing.
Controller buffering remains at most four periods, with250ms maximum queue age
and inactivity bounds. Existing stricter fences/freshness requirements still
apply; none of these bounds certifies physical interruption latency.

Every source send rechecks lease/mute, packet identity, filter state and exact
data-link ownership. A packet taken before retirement cannot borrow a new
negotiation's authority. Source monotonic time is read under its sender lock.
Control/data replacement, microphone stop, mute, lease expiry, generation change,
gap, overflow or input loss revoke input. Matching closure is explicit; absent
control links cannot report successful event delivery. Failed data sockets close
normally. A controller restart may use a fresh epoch only after actual link
retirement and a valid existing/new user session, never by automatic rearming.
The controller can explicitly reset its endpoint only after winning a newly
validated user wake with a new session; unmute/receipts/provider retries cannot.

Offline evidence:30 controller tests;26 device/listen race tests;3 actual client
hook race tests;37 existing realtime/fence/flush/receipt race tests. Full changed
server compiles for Android/ARM32 using existing read-only caches. Go-produced
retained input traverses actual controller negotiation/decoding, existing stage2
model and actual VAD: warm-D echo-only clean, normal/quiet6.60s, near16.96s,
zero clipping. This proves software compatibility, not physical acceptance.

Cold-filter speech still fails in the retained independent control. Refusing
that state is not a speech-quality fix or a waived acceptance criterion. Target
model runtime, live DSP consumption/fault policy and physical qualification are
not supplied by these hooks. No model files, recordings, private config or
runtime dependencies are included here. No downloads, installation, service
restart or room audio ran for this implementation.
# Cached-runtime consumer integration — 2026-11-04

The companion home-voice-stack staged route now consumes paired input via a
bounded child-process DSP worker, with explicit pre-wake preparation. The device
gate and controller registration defaults remain OFF; no bootstrap selects it.
No firmware/device code changed in this integration.

`PairedInput.pop_with_time()` preserves conservative host observation time for
post-inference checks; `checkpoint()` rechecks real privacy/lease authority.
`wait_available()` wakes a consumer on accepted input or retirement. The data
dispatcher yields after an accepted paired frame: otherwise five already-buffered
native periods fill the four-period queue before the consumer can run. A regression
first rejected the fifth period, then passed with one in DSP/four queued, without
increasing queue capacity. Overflow, gap and age rejection stay intact.

The companion implementation rejects output after ownership/lease/privacy loss,
preserves the oldest observation age across partial20ms packets, never feeds
original mono in parallel, and closes input/output on DSP failure. It does not
manufacture completion receipts or silently retry/rearm a context.

Offline proof used the existing CPython3.12 controller image and a CPython3.11
child copied only from the already-cached approved runtime image, with networking
disabled. Four retained-D cases match previous hardware-region PCM exactly;
native bursts produce1000 correctly sized Conversation.audio input packets per20s case. All children
were reaped and disposable test containers removed. This is not a cold-speech,
physical latency, ARM-runtime or deployment qualification.
