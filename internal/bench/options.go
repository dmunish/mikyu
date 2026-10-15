package bench

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// Mode is what a run does; the phase sequence and the bound that stops it follow from it.
type Mode string

const (
	// ModeProduce appends to a queue and measures the write path.
	ModeProduce Mode = "produce"
	// ModeConsume reads from a queue and measures the read path.
	ModeConsume Mode = "consume"
	// ModeMix runs producers and consumers at once over one set of queues.
	ModeMix Mode = "mix"
	// ModeRecovery kills a writer mid-stream and measures how fast the queue reopens.
	ModeRecovery Mode = "recovery"
)

// Access is how the run reaches mikyu: in-process, over a spawned daemon, or an existing target.
type Access string

const (
	// AccessEmbedded runs the engine in the bench process.
	AccessEmbedded Access = "embedded"
	// AccessDaemon spawns a daemon subprocess this run owns.
	AccessDaemon Access = "daemon"
	// AccessRemote dials a target somebody else serves.
	AccessRemote Access = "remote"
)

// Modes lists every run mode in table order.
var Modes = []Mode{ModeProduce, ModeConsume, ModeMix, ModeRecovery}

// DefaultMode is the mode a run measures when the caller names none.
const DefaultMode = ModeProduce

// ModeDoc describes the run mode on every surface.
const ModeDoc = "What the run measures: produce, consume, mix, or recovery"

// ParseMode converts flag, scenario, or environment text into a Mode.
func ParseMode(text string) (Mode, error) {
	mode := Mode(strings.TrimSpace(text))
	if slices.Contains(Modes, mode) {
		return mode, nil
	}
	names := make([]string, len(Modes))
	for i, m := range Modes {
		names[i] = string(m)
	}
	return "", fmt.Errorf("%w: mode must be one of %s", ErrUsage, strings.Join(names, "|"))
}

// Options is the fully resolved configuration for one run.
type Options struct {
	Mode               Mode
	Access             Access
	Target             string
	Dir                string
	KeyFile            string
	Out                string
	RunID              string
	Persist            bool
	NoCleanupStale     bool
	Quiet              bool
	Verify             bool
	Cold               bool
	CPU                int
	Metrics            []string
	Workload           Workload
	Spec               spec.Spec
	Progress           func(ProgressEvent)
	Stderr             io.Writer
	DisableObservation bool
}

// ProgressKind classifies a progress event.
type ProgressKind int

const (
	// ProgressStart marks a phase beginning.
	ProgressStart ProgressKind = iota
	// ProgressTick reports a running phase.
	ProgressTick
	// ProgressEnd marks a phase finishing.
	ProgressEnd
)

// ProgressEvent is emitted by the runner as phases progress.
type ProgressEvent struct {
	Kind    ProgressKind
	Phase   Phase
	Elapsed time.Duration
	Bytes   int64
	Total   int64
}
