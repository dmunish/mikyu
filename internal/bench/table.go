package bench

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
)

// Queue kinds a run may target; the kind is a directory, so it is declared, never inferred.
const (
	KindLog  = "log"
	KindTask = "task"
)

// Param is one row of the workload table: how a knob is named on every surface and which field it sets.
type Param struct {
	Key      string
	Kind     spec.Kind
	Modes    []Mode
	EnvName  string
	FlagName string
	Doc      string
	Default  string
	Apply    func(*Workload, spec.Value)
}

// Workload is the resolved, validated description of what a run puts through a queue.
type Workload struct {
	Kind            string
	Queues          int
	MsgSize         int
	Bytes           int64
	Duration        time.Duration
	Rate            uint64
	RateSchedule    []RatePoint
	Workers         int
	Batch           int
	Keys            int
	Preload         int64
	Warmup          time.Duration
	AckRate         float64
	CrashAfterBytes int64
	Seed            uint32
}

// RatePoint is one step of a time-varying offered rate.
type RatePoint struct {
	At   time.Duration
	Rate uint64
}

// Table is the canonical workload parameter list, in help order.
var Table = buildTable()

// buildTable derives every surface name from the key, so a row cannot drift out of the naming rule.
func buildTable() []Param {
	table := []Param{
		{Key: "kind", Kind: spec.KindString, Default: KindLog,
			Doc:   "Queue kind: log or task",
			Apply: func(w *Workload, v spec.Value) { w.Kind = v.Text() }},
		{Key: "queues", Kind: spec.KindInt, Default: "1",
			Doc:   "Number of queues to spread load across",
			Apply: func(w *Workload, v spec.Value) { w.Queues = v.Int() }},
		{Key: "msg_size", Kind: spec.KindBytes, Default: "1024",
			Doc:   "Payload size in bytes",
			Apply: func(w *Workload, v spec.Value) { w.MsgSize = int(v.Bytes()) }},
		{Key: "bytes", Kind: spec.KindBytes,
			Doc:   "Total bytes to write before stopping",
			Apply: func(w *Workload, v spec.Value) { w.Bytes = v.Bytes() }},
		{Key: "duration", Kind: spec.KindDuration,
			Doc:   "Total measure-phase duration",
			Apply: func(w *Workload, v spec.Value) { w.Duration = v.Duration() }},
		{Key: "rate", Kind: spec.KindUint32, Modes: []Mode{ModeProduce, ModeMix}, Default: "0",
			Doc:   "Target messages/s; 0 = unthrottled",
			Apply: func(w *Workload, v spec.Value) { w.Rate = uint64(v.Uint32()) }},
		{Key: "rate_schedule", Kind: spec.KindString, Modes: []Mode{ModeProduce, ModeMix},
			Doc:   "Rate schedule: '0:0,30s:100k,60s:10k'",
			Apply: func(w *Workload, v spec.Value) { w.RateSchedule, _ = ParseRateSchedule(v.Text()) }},
		{Key: "workers", Kind: spec.KindInt, Default: "4",
			Doc:   "Concurrent client workers",
			Apply: func(w *Workload, v spec.Value) { w.Workers = v.Int() }},
		{Key: "batch", Kind: spec.KindInt, Default: "64",
			Doc:   "Messages per API call",
			Apply: func(w *Workload, v spec.Value) { w.Batch = v.Int() }},
		{Key: "keys", Kind: spec.KindInt, Modes: []Mode{ModeProduce, ModeMix}, Default: "0",
			Doc:   "Distinct key cardinality; 0 = unkeyed",
			Apply: func(w *Workload, v spec.Value) { w.Keys = v.Int() }},
		{Key: "preload", Kind: spec.KindBytes, Modes: []Mode{ModeConsume}, Default: "0",
			Doc:   "Bytes written before measurement (populate phase)",
			Apply: func(w *Workload, v spec.Value) { w.Preload = v.Bytes() }},
		{Key: "warmup", Kind: spec.KindDuration, Default: "5s",
			Doc:   "Warmup duration excluded from measurement",
			Apply: func(w *Workload, v spec.Value) { w.Warmup = v.Duration() }},
		{Key: "ack_rate", Kind: spec.KindFloat, Modes: []Mode{ModeConsume, ModeMix}, Default: "0.9",
			Doc:   "Fraction of task dequeues that Ack (rest Nack)",
			Apply: func(w *Workload, v spec.Value) { w.AckRate = v.Float() }},
		{Key: "crash_after_bytes", Kind: spec.KindBytes, Modes: []Mode{ModeRecovery},
			Doc:   "Bytes written before simulated crash",
			Apply: func(w *Workload, v spec.Value) { w.CrashAfterBytes = v.Bytes() }},
		{Key: "seed", Kind: spec.KindUint32, Default: "0",
			Doc:   "PRNG seed for payload generation and ack decisions",
			Apply: func(w *Workload, v spec.Value) { w.Seed = v.Uint32() }},
	}
	for i := range table {
		table[i].EnvName = "MIKYU_BENCH_" + strings.ToUpper(table[i].Key)
		table[i].FlagName = strings.ReplaceAll(table[i].Key, "_", "-")
	}
	return table
}

// Lookup finds a workload parameter by key.
func Lookup(key string) (Param, bool) {
	for _, p := range Table {
		if p.Key == key {
			return p, true
		}
	}
	return Param{}, false
}

// AppliesTo reports whether the row is meaningful in mode; an unscoped row applies everywhere.
func (p Param) AppliesTo(mode Mode) bool { return len(p.Modes) == 0 || slices.Contains(p.Modes, mode) }

// ScopeNames names the modes a row applies to, in table order.
func (p Param) ScopeNames() []string {
	names := make([]string, len(p.Modes))
	for i, m := range p.Modes {
		names[i] = string(m)
	}
	return names
}

// Parse converts flag or env text into a Value of the row's kind.
func (p Param) Parse(text string) (spec.Value, error) {
	v, err := spec.ParseValue(p.Kind, text)
	if err != nil {
		return spec.Value{}, fmt.Errorf("%w: %s: %w", ErrUsage, p.Key, err)
	}
	return v, nil
}

// DefaultWorkload is the workload every row's Default describes; rows without one stay zero.
func DefaultWorkload() Workload {
	var w Workload
	for _, p := range Table {
		if p.Default == "" {
			continue
		}
		v, err := p.Parse(p.Default)
		if err != nil {
			panic("bench: unparseable default for " + p.Key)
		}
		p.Apply(&w, v)
	}
	return w
}

// DefaultDuration is the measure window a run gets when it names no bound of its own.
const DefaultDuration = time.Minute

// EnsureBound installs DefaultDuration for a run that named no bound; recovery has no measure window to fill.
func (w *Workload) EnsureBound(mode Mode) {
	if mode == ModeRecovery || w.Bytes > 0 || w.Duration > 0 || w.CrashAfterBytes > 0 {
		return
	}
	w.Duration = DefaultDuration
}

// Validate checks the workload in isolation; the runner checks the rules that span it and the access.
func (w Workload) Validate() error {
	switch {
	case w.Kind != KindLog && w.Kind != KindTask:
		return errRule("kind", "one of log|task", w.Kind)
	case w.Queues < 1:
		return errRule("queues", "1 or greater", w.Queues)
	case w.MsgSize < 1:
		return errRule("msg_size", "1 or greater", w.MsgSize)
	case w.Workers < 1:
		return errRule("workers", "1 or greater", w.Workers)
	case w.Batch < 1:
		return errRule("batch", "1 or greater", w.Batch)
	case w.Warmup < 0:
		return errRule("warmup", "0 or greater", w.Warmup)
	case w.Keys < 0:
		return errRule("keys", "0 or greater", w.Keys)
	case w.Keys > 0 && w.Kind != KindLog:
		return errRule("keys", "0 for a task queue", w.Keys)
	case !(w.AckRate >= 0 && w.AckRate <= 1):
		return errRule("ack_rate", "between 0 and 1", w.AckRate)
	case w.Rate > 0 && len(w.RateSchedule) > 0:
		return fmt.Errorf("%w: set either rate or rate_schedule, not both", ErrUsage)
	}
	if err := w.validateSchedule(); err != nil {
		return err
	}
	return w.validateBound()
}

// validateBound enforces one stopping condition: a byte budget, a duration, or a crash point.
func (w Workload) validateBound() error {
	if w.CrashAfterBytes > 0 {
		if w.Bytes > 0 || w.Duration > 0 {
			return fmt.Errorf("%w: crash_after_bytes replaces bytes and duration", ErrUsage)
		}
		return nil
	}
	switch {
	case w.Bytes > 0 && w.Duration > 0:
		return fmt.Errorf("%w: set exactly one of bytes or duration", ErrUsage)
	case w.Bytes <= 0 && w.Duration <= 0:
		return fmt.Errorf("%w: set exactly one of bytes or duration", ErrUsage)
	default:
		return nil
	}
}

// validateSchedule requires a schedule anchored at zero with strictly rising timestamps.
func (w Workload) validateSchedule() error {
	for i, p := range w.RateSchedule {
		switch {
		case i == 0 && p.At != 0:
			return errRule("rate_schedule", "anchored at 0s", p.At)
		case i > 0 && p.At <= w.RateSchedule[i-1].At:
			return errRule("rate_schedule", "strictly increasing timestamps", p.At)
		}
	}
	return nil
}

// View renders the workload into the envelope's shape.
func (w Workload) View() result.WorkloadView {
	view := result.WorkloadView{
		Kind:         w.Kind,
		Queues:       w.Queues,
		MsgSize:      w.MsgSize,
		Bytes:        w.Bytes,
		Duration:     w.Duration.String(),
		Rate:         w.Rate,
		RateSchedule: rateScheduleView(w.RateSchedule),
		Workers:      w.Workers,
		Batch:        w.Batch,
		Keys:         w.Keys,
		Preload:      w.Preload,
		Warmup:       w.Warmup.String(),
		AckRate:      w.AckRate,
		CrashAfter:   w.CrashAfterBytes,
		Seed:         w.Seed,
	}
	if w.Duration == 0 {
		view.Duration = ""
	}
	return view
}

func rateScheduleView(points []RatePoint) []result.RatePoint {
	if len(points) == 0 {
		return nil
	}
	out := make([]result.RatePoint, len(points))
	for i, p := range points {
		out[i] = result.RatePoint{At: p.At.String(), Rate: p.Rate}
	}
	return out
}
