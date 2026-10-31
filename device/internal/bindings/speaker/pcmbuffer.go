package speaker

// Hardware buffering covers local writer scheduling, not network jitter.
// Keep this policy independent of ALSA initialization so it can be checked
// without opening a real device. The mix/wire period remains unchanged.
// Upstream v2.18.1 (#848) restored 4x2048 after 4x1024 underruns on hardware
// despite measured scheduling gaps below the nominal smaller ring duration.
// This restores ~171ms of hardware buffering at 48kHz; physical interruption
// latency still requires measurement, and the realtime flush path is unchanged.
const alsaPeriodSize = 2048
const alsaPeriodCount = 4
const alsaBufferFrames = alsaPeriodSize * alsaPeriodCount
