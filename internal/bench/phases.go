package bench

import (
	"time"

	"github.com/dmunish/mikyu/internal/bench/result"
)

// Phase is one named stretch of a run.
type Phase string

const (
	// PhasePopulate writes the backlog a consume run measures against.
	PhasePopulate Phase = "populate"
	// PhaseWarmup drives the path without recording so caches and pools settle.
	PhaseWarmup Phase = "warmup"
	// PhaseMeasure records the observations the envelope reports.
	PhaseMeasure Phase = "measure"
	// PhaseDrain lets in-flight work finish after the bound fires.
	PhaseDrain Phase = "drain"
	// PhaseAbort records a run cut short before its bound.
	PhaseAbort Phase = "abort"
)

// phaseSequence returns the phases a mode runs, in order.
func phaseSequence(m Mode, preload int64) []Phase {
	switch m {
	case ModeConsume:
		if preload > 0 {
			return []Phase{PhasePopulate, PhaseWarmup, PhaseMeasure, PhaseDrain}
		}
		return []Phase{PhaseWarmup, PhaseMeasure, PhaseDrain}
	case ModeRecovery:
		return []Phase{PhasePopulate, PhaseAbort}
	default:
		return []Phase{PhaseWarmup, PhaseMeasure, PhaseDrain}
	}
}

// boundary tracks the phase in progress and the bytes it has moved.
type boundary struct {
	phase Phase
	start time.Time
	bytes int64
}

// begin starts a new phase at now with no bytes yet.
func (b *boundary) begin(phase Phase, now time.Time) {
	b.phase = phase
	b.start = now
	b.bytes = 0
}

// add accumulates bytes moved in the current phase.
func (b *boundary) add(n int64) { b.bytes += n }

// close renders the current phase's view.
func (b boundary) close(now time.Time) result.PhaseView {
	return result.PhaseView{
		Name:     string(b.phase),
		Duration: now.Sub(b.start).Round(time.Microsecond).String(),
		Bytes:    b.bytes,
	}
}

// emitProgress forwards a progress event when the caller supplied a callback.
func emitProgress(opts Options, e ProgressEvent) {
	if opts.Progress != nil {
		opts.Progress(e)
	}
}
