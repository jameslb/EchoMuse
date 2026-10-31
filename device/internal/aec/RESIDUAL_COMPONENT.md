# SpeexDSP residual-echo component

Source: official SpeexDSP 1.2.1 release, BSD notices retained in every source file.
Archive: https://downloads.xiph.org/releases/speex/speexdsp-1.2.1.tar.gz
SHA-256: 8c777343e4a6399569c72abc38a95b24db56882c83dbdb6c6424a5f4aeb54d3d

Added only preprocess.c, filterbank.c/filterbank.h and speex_preprocess.h from
that archive. No configure/install scripts or new runtime dependency are used.

Local adaptations:
- A float input/output adapter shares the upstream algorithm with its S16 API.
  It uses S24 counts internally (input times256, output divided by256), and
  scales the coupled residual echo power by65536. This preserves fractional
  S24 microphone data and keeps upstream absolute power floors in their domain.
- Floating residual PSD is retained in mdf.c instead of truncating to int32.
- Noise attenuation, AGC, Speex VAD and dereverb are off (noise floor0dB,
  gain stage remains enabled because disabling it disables echo suppression too).
  Stock echo suppression settings remain -40dB/-15dB active; no parameter sweep.
- One processor per mic is owned/freed alongside its echo state. Processing
  carries one frame of overlap latency (512samples/32ms).
- Idle microphone gaps discard queued post-filter history. The regression
  previously replayed an old frame at RMS699 when selecting that mic again;
  the correction passes without changing the learned echo path.

The component is experimental, OFF by default and unexported. Device startup
does not enable it. Saved-capture tests still find one false interruption and
delayed quiet-overlap detection; it is NOT ready to deploy or qualify Phase5.
The controller VAD, interruption/flush, queues and receipts are unchanged.

Private recorded speech/banks are deliberately excluded from this repository.
Public tests cover precision, default behavior, learned-filter noninterference
and lifecycle. Saved-audio/Conversation outcomes are recorded in the parent
home-voice-stack Phase5 handoff.

On the earlier independent capture, echo-only onsets fall from five (including
the genuine prompt) to just that prompt; normal/quiet overlap remain detected.
On the newer capture, one of three false onsets remains (10.64s), and quiet
overlap is detected at6.84s for an injected start at6.40s. Those failures remain
required gates; no additional parameter sweep or physical replay is justified.
