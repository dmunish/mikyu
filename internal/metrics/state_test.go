package metrics

import (
	"math"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logQueue builds a calm log queue that individual tests degrade one signal at a time.
func logQueue(mutate ...func(*QueueView)) QueueView {
	q := QueueView{
		Name: "events",
		Kind: kindLog,
		Gauges: map[string]int64{
			gaugeMessages: 1000, gaugeSegments: 3, gaugeSizeBytes: 4096,
			gaugeLastOffset: 1000, gaugeMinOffset: 0,
		},
		Groups: []GroupView{{Name: "workers", Committed: 900, HighWater: 1000, Lag: 0}},
	}
	for _, m := range mutate {
		m(&q)
	}
	return q
}

// taskQueue builds a calm task queue with a 100 in-flight bound.
func taskQueue(mutate ...func(*QueueView)) QueueView {
	max := uint32(100)
	q := QueueView{
		Name:      "jobs",
		Kind:      kindTask,
		Gauges:    map[string]int64{gaugeMessages: 500, gaugeSegments: 2, gaugeSizeBytes: 2048},
		Tasks:     TaskView{Active: 40, InFlight: 10, Scheduled: 30},
		Effective: spec.Spec{Task: spec.TaskSpec{MaxInFlight: &max}},
	}
	for _, m := range mutate {
		m(&q)
	}
	return q
}

func TestQueueHealthSignals(t *testing.T) {
	tests := []struct {
		name  string
		queue QueueView
		want  Health
	}{
		// wal_channel_fill and wal_byte_fill grade inclusively.
		{"wal channel fill below warn", logQueue(func(q *QueueView) { q.Backpressure.WALChannelFill = 0.49 }), HealthOK},
		{"wal channel fill at warn", logQueue(func(q *QueueView) { q.Backpressure.WALChannelFill = 0.5 }), HealthWarn},
		{"wal channel fill between", logQueue(func(q *QueueView) { q.Backpressure.WALChannelFill = 0.89 }), HealthWarn},
		{"wal channel fill at crit", logQueue(func(q *QueueView) { q.Backpressure.WALChannelFill = 0.9 }), HealthCritical},
		{"wal channel fill above crit", logQueue(func(q *QueueView) { q.Backpressure.WALChannelFill = 1 }), HealthCritical},
		{"wal byte fill below warn", logQueue(func(q *QueueView) { q.Backpressure.WALByteFill = 0.49 }), HealthOK},
		{"wal byte fill at warn", logQueue(func(q *QueueView) { q.Backpressure.WALByteFill = 0.5 }), HealthWarn},
		{"wal byte fill at crit", logQueue(func(q *QueueView) { q.Backpressure.WALByteFill = 0.9 }), HealthCritical},

		// Event counters warn once, and stay warned.
		{"queue full events clear", logQueue(func(q *QueueView) { q.Backpressure.QueueFullEvents = 0 }), HealthOK},
		{"queue full events fired", logQueue(func(q *QueueView) { q.Backpressure.QueueFullEvents = 1 }), HealthWarn},
		{"retry budget drops fired", logQueue(func(q *QueueView) { q.Backpressure.RetryBudgetDrops = 1 }), HealthWarn},
		{"blocked producers", logQueue(func(q *QueueView) { q.Producers.BlockedOnWAL = 3 }), HealthWarn},

		// Group lag grades strictly, so exactly 10% is still healthy.
		{"lag at warn boundary stays ok", logQueue(withLag(100, 1000)), HealthOK},
		{"lag just over warn", logQueue(withLag(101, 1000)), HealthWarn},
		{"lag at crit boundary stays warn", logQueue(withLag(500, 1000)), HealthWarn},
		{"lag just over crit", logQueue(withLag(501, 1000)), HealthCritical},
		{"worst group wins", logQueue(
			withLag(1, 1000),
			func(q *QueueView) { q.Groups = append(q.Groups, GroupView{Name: "slow", Lag: 600, HighWater: 1000}) },
		), HealthCritical},
		{"zero high water is no signal", logQueue(withLag(50, 0)), HealthOK},

		// Task-only signals.
		{"task dead lettered", taskQueue(func(q *QueueView) { q.Tasks.DeadLetters = 1 }), HealthWarn},
		{"task in flight below warn", taskQueue(func(q *QueueView) { q.Tasks.InFlight = 79 }), HealthOK},
		{"task in flight at warn", taskQueue(func(q *QueueView) { q.Tasks.InFlight = 80 }), HealthWarn},
		{"task in flight at crit", taskQueue(func(q *QueueView) { q.Tasks.InFlight = 95 }), HealthCritical},
		{"task in flight above crit", taskQueue(func(q *QueueView) { q.Tasks.InFlight = 100 }), HealthCritical},
		{"task without bound ignores ratio", taskQueue(
			func(q *QueueView) { q.Tasks.InFlight = 5000; q.Effective = spec.Spec{} },
		), HealthOK},
		{"task dead letters beat in flight", taskQueue(
			func(q *QueueView) { q.Tasks.DeadLetters = 4; q.Tasks.InFlight = 99 },
		), HealthCritical},

		// A zero value and a quiet snapshot are both healthy.
		{"zero queue view", QueueView{}, HealthOK},
		{"quiet log queue", logQueue(), HealthOK},
		{"quiet task queue", taskQueue(), HealthOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, QueueHealth(tt.queue))
		})
	}
}

// withLag sets the first consumer group's lag and high-water mark.
func withLag(lag, highWater uint64) func(*QueueView) {
	return func(q *QueueView) { q.Groups[0].Lag, q.Groups[0].HighWater = lag, highWater }
}

func TestNodeHealthSignals(t *testing.T) {
	tests := []struct {
		name  string
		build func(*Snapshot)
		want  Health
	}{
		{"quiet node", func(*Snapshot) {}, HealthOK},
		{"cache below warn", func(s *Snapshot) { s.Storage.CacheBytes, s.Storage.CacheLimit = 949, 1000 }, HealthOK},
		{"cache at warn", func(s *Snapshot) { s.Storage.CacheBytes, s.Storage.CacheLimit = 950, 1000 }, HealthWarn},
		{"cache full", func(s *Snapshot) { s.Storage.CacheBytes, s.Storage.CacheLimit = 1000, 1000 }, HealthWarn},
		{"no cache limit is no signal", func(s *Snapshot) { s.Storage.CacheBytes, s.Storage.CacheLimit = 1<<40, 0 }, HealthOK},
		{"gc pause at warn boundary stays ok", func(s *Snapshot) { s.GC.Pause.P99Ms = 10 }, HealthOK},
		{"gc pause over warn", func(s *Snapshot) { s.GC.Pause.P99Ms = 10.1 }, HealthWarn},
		{"gc pause at crit boundary stays warn", func(s *Snapshot) { s.GC.Pause.P99Ms = 50 }, HealthWarn},
		{"gc pause over crit", func(s *Snapshot) { s.GC.Pause.P99Ms = 50.1 }, HealthCritical},
		{"cpu at warn boundary stays ok", func(s *Snapshot) { s.Process.CPUPercent = 80 }, HealthOK},
		{"cpu over warn", func(s *Snapshot) { s.Process.CPUPercent = 80.1 }, HealthWarn},
		{"cpu at crit boundary stays warn", func(s *Snapshot) { s.Process.CPUPercent = 95 }, HealthWarn},
		{"cpu over crit", func(s *Snapshot) { s.Process.CPUPercent = 95.1 }, HealthCritical},
		{"worst signal wins", func(s *Snapshot) {
			s.Process.CPUPercent = 96
			s.GC.Pause.P99Ms = 60
		}, HealthCritical},
		{"degraded queue degrades node", func(s *Snapshot) {
			s.Queues = []QueueView{logQueue(func(q *QueueView) { q.Backpressure.QueueFullEvents = 1 })}
		}, HealthWarn},
		{"calm queues leave node calm", func(s *Snapshot) {
			s.Queues = []QueueView{logQueue(), taskQueue()}
		}, HealthOK},
		{"node beats calm queue on gc", func(s *Snapshot) {
			s.GC.Pause.P99Ms = 51
			s.Queues = []QueueView{logQueue()}
		}, HealthCritical},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var snap Snapshot
			tt.build(&snap)
			assert.Equal(t, tt.want, NodeHealth(snap))
		})
	}
}

// TestQueueHealthIgnoresNonFinite: a broken instrument reads as absent, not as an outage.
func TestQueueHealthIgnoresNonFinite(t *testing.T) {
	for _, fill := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		q := logQueue(func(q *QueueView) { q.Backpressure.WALChannelFill = fill })
		assert.Equal(t, HealthOK, QueueHealth(q), "fill %v", fill)
	}
	snap := Snapshot{GC: GCView{Pause: PauseView{P99Ms: math.NaN()}}, Process: ProcessView{CPUPercent: math.Inf(1)}}
	assert.Equal(t, HealthOK, NodeHealth(snap))
}

func TestHealthString(t *testing.T) {
	assert.Equal(t, "healthy", HealthOK.String())
	assert.Equal(t, "degraded", HealthWarn.String())
	assert.Equal(t, "critical", HealthCritical.String())
	assert.Equal(t, "healthy", Health(42).String())
}

func TestQueueAccessors(t *testing.T) {
	q := logQueue()
	assert.True(t, q.IsLog())
	assert.False(t, q.IsTask())
	assert.False(t, q.IsInternal())
	assert.True(t, QueueView{Kind: kindInternal}.IsInternal())
	assert.Equal(t, int64(1000), q.Messages())
	assert.Equal(t, int64(4096), q.SizeBytes())
	assert.Equal(t, int64(3), q.Segments())
	assert.Equal(t, int64(1000), q.LastOffset())
	assert.Zero(t, q.MinOffset())

	// Optional spec fields read as zero rather than panicking or lying.
	assert.Zero(t, QueueView{}.MaxInFlight())
	assert.Zero(t, QueueView{}.MaxDeadLetters())
	assert.Zero(t, QueueView{}.MaxAttempts())
	assert.Zero(t, QueueView{}.Lease())

	lease, attempts, dlq := spec.Duration(30*time.Second), uint32(5), uint32(100000)
	task := QueueView{Kind: kindTask, Effective: spec.Spec{
		Task: spec.TaskSpec{Lease: &lease, MaxAttempts: &attempts, MaxDeadLetters: &dlq},
	}}
	assert.True(t, task.IsTask())
	assert.Equal(t, 30*time.Second, task.Lease())
	assert.Equal(t, int64(5), task.MaxAttempts())
	assert.Equal(t, int64(100000), task.MaxDeadLetters())
}

func TestLagRatio(t *testing.T) {
	require.Equal(t, 0.0, QueueView{}.LagRatio())
	require.Equal(t, 0.0, logQueue().LagRatio())
	assert.InDelta(t, 0.25, logQueue(withLag(250, 1000)).LagRatio(), 1e-9)
	assert.InDelta(t, 0.9, logQueue(withLag(900, 1000)).LagRatio(), 1e-9)
	assert.Zero(t, logQueue(withLag(10, 0)).LagRatio(), "absent high water yields no ratio")
}

func TestMaxLag(t *testing.T) {
	assert.Zero(t, QueueView{}.MaxLag())
	assert.Equal(t, uint64(42), logQueue(withLag(42, 1000)).MaxLag())
	assert.Equal(t, uint64(900), logQueue(
		withLag(42, 1000),
		func(q *QueueView) { q.Groups = append(q.Groups, GroupView{Lag: 900, HighWater: 1000}) },
	).MaxLag(), "the largest absolute lag wins, not the largest ratio")
}
