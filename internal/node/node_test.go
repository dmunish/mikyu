package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createLog(t *testing.T, n *Node, name string, s spec.Spec) {
	t.Helper()
	_, err := n.CreateLogQueue(t.Context(), name, s)
	require.NoError(t, err)
}

func createTask(t *testing.T, n *Node, name string, s spec.Spec) {
	t.Helper()
	_, err := n.CreateTaskQueue(t.Context(), name, s)
	require.NoError(t, err)
}

func TestRestartPreservesSpec(t *testing.T) {
	dir := t.TempDir()

	n1, release1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	s := spec.Spec{
		Flush: spec.FlushSpec{
			MaxBytes: new(int64(7)),
		},
	}
	createLog(t, n1, "q", s)
	require.NoError(t, release1())

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	d, err := n2.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Flush.MaxBytes)
	assert.Equal(t, int64(7), *d.Spec.Flush.MaxBytes)
}

func TestRegistryCorruptRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	createLog(t, n, "q", spec.Spec{})
	require.NoError(t, release())

	require.NoError(t, os.WriteFile(filepath.Join(dir, registryFileName), []byte("{garbage"), 0o644))

	_, _, err = Acquire(dir, spec.Spec{})
	require.ErrorIs(t, err, ErrRegistryCorrupt)
}

func TestAdoptOrphanAndPurgeOrphan(t *testing.T) {
	dir := t.TempDir()
	n1, release1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	createLog(t, n1, "adopted", spec.Spec{})
	require.NoError(t, release1())

	// Overwrite the registry to include only a queue with no directory.
	r := &registry{Version: registryVersion, Queues: map[string]QueueEntry{
		registryKey(manager.KindLog, "missing"): {Name: "missing", Kind: manager.KindLog},
	}}
	require.NoError(t, saveRegistry(dir, r))

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	// "adopted" survived on disk.
	_, _, err = n2.Manager().Get("adopted")
	require.NoError(t, err)

	// "missing" was purged from the registry.
	r2, err := loadRegistry(dir)
	require.NoError(t, err)
	_, ok := r2.Queues[registryKey(manager.KindLog, "missing")]
	assert.False(t, ok)
}

func TestDescribeAfterCreateWithFlags(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Builtin())
	require.NoError(t, err)
	defer func() { _ = release() }()

	s := spec.Spec{Flush: spec.FlushSpec{MaxBytes: new(int64(7))}}
	createLog(t, n, "q", s)

	d, err := n.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Flush.MaxBytes)
	assert.Equal(t, int64(7), *d.Spec.Flush.MaxBytes)
	require.NotNil(t, d.Effective.Storage.MaxSegment)
	require.NotNil(t, d.Defaults.Storage.MaxSegment)
}

func TestUpdateQueueRejectsCreateOnly(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	createLog(t, n, "q", spec.Spec{})
	patch := spec.Spec{Storage: spec.StorageSpec{MaxSegment: new(int64(1024))}}
	err = n.UpdateQueue(t.Context(), "q", patch)
	require.ErrorIs(t, err, spec.ErrImmutableField)
}

func TestUpdateQueueMutable(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	createLog(t, n, "q", spec.Spec{})
	require.NoError(t, n.UpdateQueue(t.Context(), "q", spec.Spec{Flush: spec.FlushSpec{MaxBytes: new(int64(42))}}))

	d, err := n.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Flush.MaxBytes)
	assert.Equal(t, int64(42), *d.Spec.Flush.MaxBytes)
}

func TestRetentionShrinkTriggersImmediateReconcile(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	ctx := t.Context()

	segSize := int64(256)
	segAge := spec.Duration(time.Hour)
	hour := spec.Duration(time.Hour)
	createLog(t, n, "q", spec.Spec{
		Storage:   spec.StorageSpec{MaxSegment: &segSize, MaxSegmentAge: &segAge},
		Retention: spec.RetentionSpec{MaxAge: &hour},
	})
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
	require.NoError(t, n.UpdateQueue(ctx, "q", spec.Spec{Retention: spec.RetentionSpec{MaxAge: &millis}}))

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
	createTask(t, n, "q", spec.Spec{Task: spec.TaskSpec{Lease: &minute}})
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
	require.NoError(t, n.UpdateQueue(ctx, "q", spec.Spec{Task: spec.TaskSpec{Lease: &two}}))

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
	createTask(t, n1, "q", spec.Spec{Task: spec.TaskSpec{Lease: &minute}})
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
	require.NoError(t, n2.UpdateQueue(ctx, "q", spec.Spec{Task: spec.TaskSpec{Lease: &two}}))
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
	createLog(t, n, "logs", spec.Spec{})
	createTask(t, n, "tasks", spec.Spec{})

	lq, err := n.LogQueue("logs")
	require.NoError(t, err)
	_, err = lq.Append(ctx, []byte("hello"))
	require.NoError(t, err)

	tq, err := n.TaskQueue("tasks")
	require.NoError(t, err)
	_, err = tq.Enqueue(ctx, []byte("task"))
	require.NoError(t, err)

	snap, err := n.Metrics(ctx)
	require.NoError(t, err)
	qv, ok := snap.Queue("logs")
	require.True(t, ok)
	assert.Positive(t, qv.Gauges["messages"])
	assert.Positive(t, qv.Counters["appends"])
	assert.Contains(t, qv.Histograms, "append_duration")
	tqv, ok := snap.Queue("tasks")
	require.True(t, ok)
	assert.Positive(t, tqv.Counters["enqueues"])
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

// TestCreateQueueRejectsExplicitPairConflict: a proposal setting two fields
// that conflict with each other is rejected, naming both keys.
func TestCreateQueueRejectsExplicitPairConflict(t *testing.T) {
	n, release, err := Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	segment := int64(1 << 20)
	payload := int64(2 << 20)
	_, err = n.CreateLogQueue(t.Context(), "q", spec.Spec{
		Storage: spec.StorageSpec{MaxSegment: &segment, MaxPayload: &payload},
	})
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "max_payload")
	assert.Contains(t, err.Error(), "max_segment")
}

// TestUpdateQueueRejectsMergedConflict: an unrelated patch revalidates the
// merged result, so a patch that conflicts with a stored field is caught.
func TestUpdateQueueRejectsMergedConflict(t *testing.T) {
	dir := t.TempDir()
	segment := int64(1 << 20)

	n1, r1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	createLog(t, n1, "q", spec.Spec{Storage: spec.StorageSpec{MaxSegment: &segment}})
	require.NoError(t, r1())

	n2, r2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = r2() }()

	cache := int64(256 << 10)
	err = n2.UpdateQueue(t.Context(), "q", spec.Spec{Storage: spec.StorageSpec{MaxCache: &cache}})
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "max_cache")
	assert.Contains(t, err.Error(), "max_segment")
}

// TestAcquireRejectsInvalidSessionDefault: a bad session default makes the
// whole node refuse to open.
func TestAcquireRejectsInvalidSessionDefault(t *testing.T) {
	zero := spec.Duration(0)
	_, _, err := Acquire(t.TempDir(), spec.Spec{Task: spec.TaskSpec{Lease: &zero}})
	require.ErrorIs(t, err, spec.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "lease")
}

// TestWrongKindRefused: a task queue addressed as a log queue is refused by name.
func TestWrongKindRefused(t *testing.T) {
	n, release, err := Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	createTask(t, n, "jobs", spec.Spec{})
	_, err = n.LogQueue("jobs")
	require.ErrorIs(t, err, ErrWrongKind)
	assert.Contains(t, err.Error(), "jobs")

	createLog(t, n, "events", spec.Spec{})
	_, err = n.TaskQueue("events")
	require.ErrorIs(t, err, ErrWrongKind)
}

// TestListCarriesKind: the filesystem is the source of truth for kind.
func TestListCarriesKind(t *testing.T) {
	n, release, err := Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	ctx := t.Context()

	createLog(t, n, "events", spec.Spec{})
	createTask(t, n, "events", spec.Spec{})
	createTask(t, n, "jobs", spec.Spec{})

	infos, err := n.ListQueues(ctx)
	require.NoError(t, err)
	require.Len(t, infos, 3)
	assert.Equal(t, QueueInfo{Name: "events", Kind: KindLog}, infos[0])
	assert.Equal(t, QueueInfo{Name: "events", Kind: KindTask}, infos[1])
	assert.Equal(t, QueueInfo{Name: "jobs", Kind: KindTask}, infos[2])
}

// TestCreateReturnsHandle: Create* returns a handle usable on the same call.
func TestCreateReturnsHandle(t *testing.T) {
	n, release, err := Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	ctx := t.Context()

	lq, err := n.CreateLogQueue(ctx, "events", spec.Spec{})
	require.NoError(t, err)
	_, err = lq.Append(ctx, []byte("x"))
	require.NoError(t, err)

	tq, err := n.CreateTaskQueue(ctx, "jobs", spec.Spec{})
	require.NoError(t, err)
	_, err = tq.Enqueue(ctx, []byte("y"))
	require.NoError(t, err)
}

// TestMoveQueueDirectoryFailsLoad: a log queue moved under task/ is a user
// error; the task driver reports the corrupt record naming the directory.
func TestMoveQueueDirectoryFailsLoad(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	createLog(t, n, "events", spec.Spec{})
	lq, err := n.LogQueue("events")
	require.NoError(t, err)
	_, err = lq.Append(t.Context(), []byte("an ordinary log record"))
	require.NoError(t, err)
	require.NoError(t, release())

	require.NoError(t, os.Rename(
		filepath.Join(dir, "log", "events"),
		filepath.Join(dir, "task", "events"),
	))

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	_, err = n2.TaskQueue("events")
	require.ErrorIs(t, err, engine.ErrCorruptRecord)
	assert.Contains(t, err.Error(), "events")
}