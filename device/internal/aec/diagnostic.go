package aec

// FilterDiagnostic describes provenance, not a dump of adaptive weights or
// signal history. Reading it never resets, imports or otherwise changes DSP.
type FilterDiagnostic struct {
	Generation     uint64
	HardwareFrames uint64
	Domain         string
	LoadStatus     string
	LoadSHA256     string
	LoadBytes      int
}

func (c *Canceller) DiagnosticState() FilterDiagnostic {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.diagnosticStateLocked()
}

func (c *Canceller) diagnosticStateLocked() FilterDiagnostic {
	state := c.diagnostic
	state.Domain = "gained_s16"
	if c.floatMic {
		state.Domain = "unity_f32_s24"
	}
	if !c.enabled || c.st == nil {
		state.LoadStatus = "inactive"
		state.LoadSHA256 = ""
		state.LoadBytes = 0
	}
	return state
}

func (c *Canceller) ProcessDiagnostic(mono []byte) ([]byte, FilterDiagnostic) {
	var state FilterDiagnostic
	out := c.processObserved(mono, nil, &state)
	return out, state
}

func (c *Canceller) ProcessMicsFloatDiagnostic(mics [][]float32, active int, ref []byte, gain float64) ([]byte, FilterDiagnostic) {
	var state FilterDiagnostic
	out := c.processMicsFloat(mics, active, ref, gain, &state)
	return out, state
}
