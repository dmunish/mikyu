package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartPreservesSpec(t *testing.T) {
	dir := t.TempDir()

	n1, release1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	s := spec.Spec{
		Engine: spec.EngineSpec{
			BatchSize: new(7),
			Fsync:     new(false),
		},
	}
	require.NoError(t, n1.CreateQueue(t.Context(), "q", s))
	require.NoError(t, release1())

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	d, err := n2.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Engine.BatchSize)
	assert.Equal(t, 7, *d.Spec.Engine.BatchSize)
	require.NotNil(t, d.Spec.Engine.Fsync)
	assert.False(t, *d.Spec.Engine.Fsync)
}

func TestRegistryCorruptRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	require.NoError(t, n.CreateQueue(t.Context(), "q", spec.Spec{}))
	require.NoError(t, release())

	require.NoError(t, os.WriteFile(filepath.Join(dir, registryFileName), []byte("{garbage"), 0o644))

	_, _, err = Acquire(dir, spec.Spec{})
	require.ErrorIs(t, err, ErrRegistryCorrupt)
}

func TestAdoptOrphanAndPurgeOrphan(t *testing.T) {
	dir := t.TempDir()
	n1, release1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	require.NoError(t, n1.CreateQueue(t.Context(), "adopted", spec.Spec{}))
	require.NoError(t, release1())

	// Overwrite the registry to include only a queue with no directory.
	r := &registry{Version: 1, Queues: map[string]spec.Spec{"missing": {}}}
	require.NoError(t, saveRegistry(dir, r))

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	// "adopted" survived on disk.
	_, err = n2.Manager().GetQueue("adopted")
	require.NoError(t, err)

	// "missing" was purged from the registry.
	r2, err := loadRegistry(dir)
	require.NoError(t, err)
	_, ok := r2.Queues["missing"]
	assert.False(t, ok)
}

func TestDescribeAfterCreateWithFlags(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Builtin())
	require.NoError(t, err)
	defer func() { _ = release() }()

	s := spec.Spec{Engine: spec.EngineSpec{BatchSize: new(7)}}
	require.NoError(t, n.CreateQueue(t.Context(), "q", s))

	d, err := n.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Engine.BatchSize)
	assert.Equal(t, 7, *d.Spec.Engine.BatchSize)
	require.NotNil(t, d.Effective.Engine.BatchSize)
	require.NotNil(t, d.Effective.Engine.SegmentSize)
	require.NotNil(t, d.Defaults.Engine.SegmentSize)
}

func TestUpdateQueueRejectsCreateOnly(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	require.NoError(t, n.CreateQueue(t.Context(), "q", spec.Spec{}))
	patch := spec.Spec{Engine: spec.EngineSpec{SegmentSize: new(int64(1024))}}
	err = n.UpdateQueue(t.Context(), "q", patch)
	require.ErrorIs(t, err, spec.ErrImmutableField)
}

func TestUpdateQueueMutable(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	require.NoError(t, n.CreateQueue(t.Context(), "q", spec.Spec{}))
	require.NoError(t, n.UpdateQueue(t.Context(), "q", spec.Spec{Engine: spec.EngineSpec{BatchSize: new(42)}}))

	d, err := n.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Engine.BatchSize)
	assert.Equal(t, 42, *d.Spec.Engine.BatchSize)
}

func TestRetentionShrinkTriggersImmediateReconcile(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	ctx := t.Context()

	segSize := int64(256)
	maxMsg := int64(128)
	hour := spec.Duration(time.Hour)
	require.NoError(t, n.CreateQueue(ctx, "q", spec.Spec{
		Engine:    spec.EngineSpec{SegmentSize: &segSize, MaxMessageBytes: &maxMsg},
		Retention: spec.RetentionSpec{Age: &hour},
	}))
	lq, err := n.LogQueue("q")
	require.NoError(t, err)

	// 12 records roll three 256B segments.
	for range 12 {
		_, err = lq.Append(ctx, []byte("retention-shrink-payload-1234"))
		require.NoError(t, err)
	}
	// Age every record past the new, tighter bound.
	time.Sleep(20 * time.Millisecond)

	millis := spec.Duration(10 * time.Millisecond)
	require.NoError(t, n.UpdateQueue(ctx, "q", spec.Spec{Retention: spec.RetentionSpec{Age: &millis}}))

	// The shrink triggers an immediate reconcile; the 30s tick is never
	// waited for.
	require.Eventually(t, func() bool {
		minOff, err := lq.MinOffset(ctx)
		return err == nil && minOff > 0
	}, 2*time.Second, 5*time.Millisecond)
}

func TestQueueDefaultUpdateAffectsFutureEnqueues(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	ctx := t.Context()

	minute := spec.Duration(time.Minute)
	require.NoError(t, n.CreateQueue(ctx, "q", spec.Spec{Task: spec.TaskSpec{DefaultVisibility: &minute}}))
	tq, err := n.TaskQueue("q")
	require.NoError(t, err)

	// The materialized handle carries the spec default (1m), not the built-in.
	// Visibility is the lease granted at dequeue time, so dequeue to observe it.
	_, err = tq.Enqueue(ctx, []byte("first"))
	require.NoError(t, err)
	first, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	assert.InDelta(t, time.Minute.Seconds(), time.Until(first.Visibility).Seconds(), 5)
	assert.Equal(t, uint32(5), first.MaxAttempts)

	two := spec.Duration(2 * time.Minute)
	require.NoError(t, n.UpdateQueue(ctx, "q", spec.Spec{Task: spec.TaskSpec{DefaultVisibility: &two}}))

	// Future enqueues observe the new default; the earlier task keeps its own.
	_, err = tq.Enqueue(ctx, []byte("second"))
	require.NoError(t, err)
	second, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	assert.InDelta(t, (2 * time.Minute).Seconds(), time.Until(second.Visibility).Seconds(), 5)
	assert.InDelta(t, time.Minute.Seconds(), time.Until(first.Visibility).Seconds(), 5)
}

func TestQueueDefaultUpdateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	n1, r1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	minute := spec.Duration(time.Minute)
	require.NoError(t, n1.CreateQueue(t.Context(), "q", spec.Spec{Task: spec.TaskSpec{DefaultVisibility: &minute}}))
	require.NoError(t, r1())

	n2, r2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = r2() }()
	ctx := t.Context()

	tq, err := n2.TaskQueue("q")
	require.NoError(t, err)
	_, err = tq.Enqueue(ctx, []byte("across-restart"))
	require.NoError(t, err)
	first, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	assert.InDelta(t, time.Minute.Seconds(), time.Until(first.Visibility).Seconds(), 5)

	two := spec.Duration(2 * time.Minute)
	require.NoError(t, n2.UpdateQueue(ctx, "q", spec.Spec{Task: spec.TaskSpec{DefaultVisibility: &two}}))
	_, err = tq.Enqueue(ctx, []byte("after-update"))
	require.NoError(t, err)
	second, err := tq.Dequeue(ctx)
	require.NoError(t, err)
	assert.InDelta(t, (2 * time.Minute).Seconds(), time.Until(second.Visibility).Seconds(), 5)
}

func TestNodeCollectorLifecycle(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{}, WithMetrics(MetricsConfig{
		Enabled:  true,
		Interval: 5 * time.Millisecond,
		History:  8,
	}))
	require.NoError(t, err)

	ch, cancel := n.Collector().Subscribe()
	defer cancel()

	// Wait for at least one collector tick.
	require.Eventually(t, func() bool {
		return len(n.Collector().History()) > 0
	}, time.Second, time.Millisecond)

	snap, err := n.Metrics(t.Context())
	require.NoError(t, err)
	require.False(t, snap.Disabled)
	require.Greater(t, snap.Runtime.Goroutines, 0)
	require.Greater(t, snap.Runtime.HeapAlloc, int64(0))

	require.NoError(t, release())
	// Stop joins the ticker goroutine and closes every subscriber channel. Drain
	// any buffered snapshot, then the channel must read as closed.
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("collector subscriber channel did not close after node release")
		}
	}
}

func TestNodeSnapshotIncludesQueueInstruments(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{}, WithMetrics(MetricsConfig{
		Enabled:    true,
		Interval:   time.Hour,
		History:    4,
		Histograms: true,
	}))
	require.NoError(t, err)
	defer func() { _ = release() }()

	ctx := t.Context()
	require.NoError(t, n.CreateQueue(ctx, "q", spec.Spec{}))

	lq, err := n.LogQueue("q")
	require.NoError(t, err)
	_, err = lq.Append(ctx, []byte("hello"))
	require.NoError(t, err)

	tq, err := n.TaskQueue("q")
	require.NoError(t, err)
	_, err = tq.Enqueue(ctx, []byte("task"))
	require.NoError(t, err)

	snap, err := n.Metrics(ctx)
	require.NoError(t, err)
	qv, ok := snap.Queue("q")
	require.True(t, ok)
	assert.Positive(t, qv.Gauges["messages"])
	assert.Positive(t, qv.Counters["appends"])
	assert.Positive(t, qv.Counters["enqueues"])
	assert.Contains(t, qv.Histograms, "append_duration")
}

func TestNodeCollectorDisabled(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{}) // no WithMetrics -> disabled
	require.NoError(t, err)
	defer func() { _ = release() }()

	snap, err := n.Metrics(t.Context())
	require.NoError(t, err)
	require.True(t, snap.Disabled)

	ch, cancel := n.Collector().Subscribe()
	defer cancel()
	_, ok := <-ch
	require.False(t, ok, "disabled collector must return a closed channel")
}

// TestCreateQueueRejectsInheritedConflict: the proposal is valid alone but
// conflicts with the session default it inherits.
func TestCreateQueueRejectsInheritedConflict(t *testing.T) {
	seg := int64(1 << 20)
	n, release, err := Acquire(t.TempDir(), spec.Spec{Engine: spec.EngineSpec{SegmentSize: &seg}})
	require.NoError(t, err)
	defer func() { _ = release() }()

	msg := int64(2 << 20)
	err = n.CreateQueue(t.Context(), "q", spec.Spec{Engine: spec.EngineSpec{MaxMessageBytes: &msg}})
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "max_message_bytes")
	assert.Contains(t, err.Error(), "segment_size")
}

// TestUpdateQueueRejectsInheritedConflict: reopening a directory under smaller
// session defaults makes a stored create-only field conflict with what it now
// inherits, so even an unrelated update must revalidate the merged result.
func TestUpdateQueueRejectsInheritedConflict(t *testing.T) {
	dir := t.TempDir()
	msg := int64(512 << 10)
	roomy := int64(1 << 20)

	n1, r1, err := Acquire(dir, spec.Spec{Engine: spec.EngineSpec{SegmentSize: &roomy}})
	require.NoError(t, err)
	require.NoError(t, n1.CreateQueue(t.Context(), "q", spec.Spec{Engine: spec.EngineSpec{MaxMessageBytes: &msg}}))
	require.NoError(t, r1())

	small := int64(256 << 10)
	n2, r2, err := Acquire(dir, spec.Spec{Engine: spec.EngineSpec{SegmentSize: &small}})
	require.NoError(t, err)
	defer func() { _ = r2() }()

	err = n2.UpdateQueue(t.Context(), "q", spec.Spec{Engine: spec.EngineSpec{BatchSize: new(7)}})
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "max_message_bytes")
	assert.Contains(t, err.Error(), "segment_size")
}

// TestCreateQueueRejectsInvalidSessionDefault: a bad session default makes
// every create fail, even for an empty proposal.
func TestCreateQueueRejectsInvalidSessionDefault(t *testing.T) {
	zero := 0
	n, release, err := Acquire(t.TempDir(), spec.Spec{Engine: spec.EngineSpec{BatchSize: &zero}})
	require.NoError(t, err)
	defer func() { _ = release() }()

	err = n.CreateQueue(t.Context(), "q", spec.Spec{})
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "batch_size")
}
