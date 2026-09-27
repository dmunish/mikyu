package metrics

import (
	"math"
	"time"
)

// Health is the derived operational state of a node or queue, not a UI concern.
type Health int

const (
	// HealthOK means every signal present in the snapshot is within bounds.
	HealthOK Health = iota
	// HealthWarn means a signal crossed its warn threshold.
	HealthWarn
	// HealthCritical means a signal crossed its critical threshold.
	HealthCritical
)

// Queue kind labels as they appear in QueueView.Kind.
const (
	kindLog      = "log"
	kindTask     = "task"
	kindInternal = "internal"
)

// Storage gauge names the engine publishes into QueueView.Gauges. Task occupancy
// is routed to QueueView.Tasks instead, so none of these names collide with it.
const (
	gaugeMessages   = "messages"
	gaugeSegments   = "segments"
	gaugeSizeBytes  = "size_bytes"
	gaugeLastOffset = "last_offset"
	gaugeMinOffset  = "min_offset"
)

// Thresholds backing the two health tables below. Ratios are fractions of a
// whole; percent and millisecond thresholds carry their natural units. The
// inclusive (>=) rows treat the boundary as a violation; the strict (>) rows
// leave an exact boundary healthy.
//
//	Queue signals — worst one wins.
//
//	Signal                                Warn      Critical
//	backpressure.wal_channel_fill         >= 0.5    >= 0.9
//	backpressure.wal_byte_fill            >= 0.5    >= 0.9
//	backpressure.queue_full_events        > 0
//	backpressure.retry_budget_drops       > 0
//	producers.blocked_on_wal              > 0
//	log queue: lag / high_water (groups)   > 0.1     > 0.5
//	task queue: dead_letters               > 0
//	task queue: in_flight / max_in_flight  >= 0.8    >= 0.95
//
//	Node signals — worst one wins over the worst queue's state.
//
//	Signal                                Warn     Critical
//	storage.cache_bytes / cache_limit     >= 0.95
//	gc.pause.p99_ms                       > 10     > 50
//	process.cpu_percent                   > 80     > 95
//
// A signal sitting at zero reads as absent rather than as a violation, so a
// quiet queue is always healthy and an unconfigured bound never trips a ratio.
const (
	walFillWarn  = 0.5
	walFillCrit  = 0.9
	lagWarn      = 0.1
	lagCrit      = 0.5
	inFlightWarn = 0.8
	inFlightCrit = 0.95
	cacheWarn    = 0.95
	pauseWarnMs  = 10
	pauseCritMs  = 50
	cpuWarnPct   = 80
	cpuCritPct   = 95
)

// String names the state the way the UI speaks it.
func (h Health) String() string {
	switch h {
	case HealthWarn:
		return "degraded"
	case HealthCritical:
		return "critical"
	default:
		return "healthy"
	}
}

// IsLog reports whether the queue is a log queue.
func (q QueueView) IsLog() bool { return q.Kind == kindLog }

// IsTask reports whether the queue is a task queue.
func (q QueueView) IsTask() bool { return q.Kind == kindTask }

// IsInternal reports whether the view is a non-queue instrument scope, not a queue.
func (q QueueView) IsInternal() bool { return q.Kind == kindInternal }

// Messages is the queue's published record count.
func (q QueueView) Messages() int64 { return q.Gauges[gaugeMessages] }

// SizeBytes is the queue's on-disk footprint.
func (q QueueView) SizeBytes() int64 { return q.Gauges[gaugeSizeBytes] }

// Segments is the queue's segment count.
func (q QueueView) Segments() int64 { return q.Gauges[gaugeSegments] }

// LastOffset is the next offset the queue will hand out.
func (q QueueView) LastOffset() int64 { return q.Gauges[gaugeLastOffset] }

// MinOffset is the oldest offset the queue still retains.
func (q QueueView) MinOffset() int64 { return q.Gauges[gaugeMinOffset] }

// MaxInFlight is the task queue's concurrency bound, or 0 when unset.
func (q QueueView) MaxInFlight() int64 { return uint32OrZero(q.Effective.Task.MaxInFlight) }

// MaxDeadLetters is the task queue's dead-letter bound, or 0 when unset.
func (q QueueView) MaxDeadLetters() int64 { return uint32OrZero(q.Effective.Task.MaxDeadLetters) }

// MaxAttempts is the task queue's attempt bound, or 0 when unset.
func (q QueueView) MaxAttempts() int64 { return uint32OrZero(q.Effective.Task.MaxAttempts) }

// Lease is the task queue's visibility timeout, or 0 when unset.
func (q QueueView) Lease() time.Duration {
	if q.Effective.Task.Lease == nil {
		return 0
	}
	return q.Effective.Task.Lease.Duration()
}

// LagRatio is the worst consumer-group lag as a fraction of its high-water mark.
func (q QueueView) LagRatio() float64 {
	worst := 0.0
	for _, g := range q.Groups {
		if r := fraction(float64(g.Lag), float64(g.HighWater)); r > worst {
			worst = r
		}
	}
	return worst
}

// MaxLag is the largest absolute consumer-group lag across the queue's groups.
func (q QueueView) MaxLag() uint64 {
	var max uint64
	for _, g := range q.Groups {
		if g.Lag > max {
			max = g.Lag
		}
	}
	return max
}

// GroupHealth derives one consumer group's state from its lag against its high-water mark.
func GroupHealth(g GroupView) Health {
	return above(fraction(float64(g.Lag), float64(g.HighWater)), lagWarn, lagCrit)
}

// QueueHealth derives one queue's state from a single snapshot, with no history and no wall clock.
func QueueHealth(q QueueView) Health {
	h := atOrAbove(q.Backpressure.WALChannelFill, walFillWarn, walFillCrit)
	h = worst(h, atOrAbove(q.Backpressure.WALByteFill, walFillWarn, walFillCrit))
	h = worst(h, present(q.Backpressure.QueueFullEvents))
	h = worst(h, present(q.Backpressure.RetryBudgetDrops))
	h = worst(h, present(q.Producers.BlockedOnWAL))
	if q.IsTask() {
		h = worst(h, present(q.Tasks.DeadLetters))
		return worst(h, atOrAbove(
			fraction(float64(q.Tasks.InFlight), float64(q.MaxInFlight())), inFlightWarn, inFlightCrit))
	}
	for _, g := range q.Groups {
		h = worst(h, GroupHealth(g))
	}
	return h
}

// NodeHealth derives the node's state from its own signals, then from its worst queue's.
func NodeHealth(s Snapshot) Health {
	h := atLeast(fraction(float64(s.Storage.CacheBytes), float64(s.Storage.CacheLimit)), cacheWarn)
	h = worst(h, above(s.GC.Pause.P99Ms, pauseWarnMs, pauseCritMs))
	h = worst(h, above(s.Process.CPUPercent, cpuWarnPct, cpuCritPct))
	for _, q := range s.Queues {
		h = worst(h, QueueHealth(q))
	}
	return h
}

// worst returns the more severe of two states.
func worst(a, b Health) Health {
	if b > a {
		return b
	}
	return a
}

// atOrAbove grades against inclusive thresholds, so a boundary itself trips.
func atOrAbove(value, warn, crit float64) Health {
	switch {
	case finite(value) && value >= crit:
		return HealthCritical
	case finite(value) && value >= warn:
		return HealthWarn
	default:
		return HealthOK
	}
}

// above grades against strict thresholds, so an exact boundary stays healthy.
func above(value, warn, crit float64) Health {
	switch {
	case finite(value) && value > crit:
		return HealthCritical
	case finite(value) && value > warn:
		return HealthWarn
	default:
		return HealthOK
	}
}

// atLeast grades against a single inclusive threshold with no critical step.
func atLeast(value, warn float64) Health {
	if finite(value) && value >= warn {
		return HealthWarn
	}
	return HealthOK
}

// present warns once an event counter has fired at least once.
func present(events int64) Health {
	if events > 0 {
		return HealthWarn
	}
	return HealthOK
}

// finite rejects a non-finite reading, so a broken instrument reads as absent rather than as an outage.
func finite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// fraction divides part by whole, treating an absent whole as no signal.
func fraction(part, whole float64) float64 {
	if whole <= 0 {
		return 0
	}
	return part / whole
}

// uint32OrZero dereferences an optional spec field.
func uint32OrZero(v *uint32) int64 {
	if v == nil {
		return 0
	}
	return int64(*v)
}
