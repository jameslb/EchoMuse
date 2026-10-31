# Phase 5 audio backports — October 30, 2026

Review branch: `codex/phase5-upstream-review`, based on the retained diagnostic
baseline `0500d7332ccf3bb80bb93c1226172865309b7297`. Not deployed.

Compared stable firmware v2.18.1 (`b18a0befb3492f335ac0a71cdd9cd15390b6e9e8`)
and fetched the controller-v2.26.3 source ref
(`89f3f717460f7d11a9421edc3861455fb44d2a96`) for provenance. This is a selective
backport of two firmware fixes, not a full controller/dependency rebase.

- Upstream #848: restore ALSA 4×2048 at 48 kHz (~170.7 ms), from 4×1024
  (~85.3 ms). Mix/wire cadence remains 2048 samples. Constants are in
  `pcmbuffer.go` so the actual server policy can be checked without opening ALSA.
- Upstream #870: an atomic hardware-reference mirror lets `WriteFar` return
  before acquiring the microphone cancellation mutex. The software-reference
  path retains the locked check/ring logic. A mode switch still resets the ring;
  the atomic fast path can discard one racing period as the upstream design does.

Before these changes, a regression holding the cancellation mutex demonstrated
the speaker callback blocking for its 200 ms test window, and the driver-policy
contract rejected the 85.3 ms configuration. Both pass after the backports; a
hardware-to-software transition regression verifies software reference resumes.
These are code/contract regressions, not physical underrun or latency evidence.

AEC, speaker, aecdiag, client and rtplayback suites pass with race detection using
synthetic inputs and cached offline tooling. The actual Android/ARM server also
compiles. The proof artifact is labeled `offline-upstream-review`; no deployment
package, upload or room test was performed.

Full-precision input/provenance, separate permit scope, filter persistence/domain
handling, controller export/authentication, PCM transport, owner checks, epoch/
stale-frame rejection, interruption, flush and completion receipts are retained.
No model, gain, VAD, tail, deadline or queue-size changes. Larger hardware buffering
still requires physical interruption/latency qualification; it is not permission
to relax the Phase 5 targets. No causal claim about the saved room failure.
