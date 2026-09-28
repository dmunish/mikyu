package node

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatalogCompleteness: every queue on disk is installed at open and resolves.
func TestCatalogCompleteness(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	createLog(t, n, "logs", spec.Spec{})
	createTask(t, n, "tasks", spec.Spec{})
	require.NoError(t, release())

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	infos, err := n2.ListQueues(t.Context())
	require.NoError(t, err)
	require.Len(t, infos, 2)
	for _, info := range infos {
		switch info.Kind {
		case manager.KindLog:
			e, err := n2.logEntryFor(info.Name)
			require.NoError(t, err)
			assert.NotNil(t, e.q)
		case manager.KindTask:
			e, err := n2.taskEntryFor(info.Name)
			require.NoError(t, err)
			assert.NotNil(t, e.q)
		}
	}
	assert.Len(t, n2.logCatalog, 1)
	assert.Len(t, n2.taskCatalog, 1)
}

// TestLookupIsPure: repeated lookups return the same driver and spawn nothing.
func TestLookupIsPure(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	createLog(t, n, "logs", spec.Spec{})
	createTask(t, n, "tasks", spec.Spec{})

	first, err := n.TaskQueue("tasks")
	require.NoError(t, err)
	before := runtime.NumGoroutine()
	for range 1000 {
		again, err := n.TaskQueue("tasks")
		require.NoError(t, err)
		assert.Same(t, first, again)
		_, err = n.LogQueue("logs")
		require.NoError(t, err)
	}
	assert.Equal(t, before, runtime.NumGoroutine())
	assert.Len(t, n.taskCatalog, 1)
}

// TestConcurrentInstall: racing lookups on an installed queue share one driver.
func TestConcurrentInstall(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	createTask(t, n, "jobs", spec.Spec{})

	want, err := n.TaskQueue("jobs")
	require.NoError(t, err)

	handles := make([]taskqueue.Queue, 32)
	var wg sync.WaitGroup
	wg.Add(len(handles))
	for i := range handles {
		go func() {
			defer wg.Done()
			got, err := n.TaskQueue("jobs")
			assert.NoError(t, err)
			handles[i] = got
		}()
	}
	wg.Wait()
	for _, got := range handles {
		assert.Same(t, want, got)
	}
}

// TestFaultedTaskQueueDoesNotFailOpen: one bad queue faults itself, not the node.
func TestFaultedTaskQueueDoesNotFailOpen(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	createLog(t, n, "logs", spec.Spec{})
	createTask(t, n, "jobs", spec.Spec{})

	// A record the task driver cannot interpret marks the queue faulted.
	storage, err := n.mgr.GetOf("jobs", manager.KindTask)
	require.NoError(t, err)
	_, err = storage.Append(t.Context(), []byte{0xff, 0x00, 0x00})
	require.NoError(t, err)
	require.NoError(t, release())

	n2, release2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release2() }()

	_, err = n2.TaskQueue("jobs")
	require.ErrorIs(t, err, engine.ErrCorruptRecord)
	assert.Contains(t, err.Error(), "jobs")

	// A healthy queue in the same directory is unaffected.
	lq, err := n2.LogQueue("logs")
	require.NoError(t, err)
	_, err = lq.Append(t.Context(), []byte("still works"))
	require.NoError(t, err)

	snap, err := n2.Metrics(t.Context())
	require.NoError(t, err)
	faulted, ok := snap.Queue("jobs")
	require.True(t, ok)
	assert.EqualValues(t, 1, faulted.Counters["queue_faults"])
	healthy, ok := snap.Queue("logs")
	require.True(t, ok)
	assert.Zero(t, healthy.Counters["queue_faults"])
}

// TestUntouchedQueueRetention: retention runs for a queue no client ever opened.
func TestUntouchedQueueRetention(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	tiny := spec.Duration(time.Nanosecond)
	small := int64(4096)
	createLog(t, n, "logs", spec.Spec{
		Storage:   spec.StorageSpec{MaxSegment: &small},
		Flush:     spec.FlushSpec{MaxBytes: &small},
		Retention: spec.RetentionSpec{MaxAge: &tiny},
	})
	lq, err := n.LogQueue("logs")
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("x"), 1024)
	for range 16 {
		_, err = lq.Append(t.Context(), payload)
		require.NoError(t, err)
	}

	storage, err := n.mgr.GetOf("logs", manager.KindLog)
	require.NoError(t, err)
	require.Zero(t, storage.MinOffset())

	reclaimed := n.retention.ReconcileAll(t.Context())
	assert.Positive(t, reclaimed["logs"])
	assert.Positive(t, storage.MinOffset())
}

// TestUntouchedTaskQueueRetention: a task queue's policy is set even without a client.
func TestUntouchedTaskQueueRetention(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	tiny := spec.Duration(time.Nanosecond)
	small := int64(4096)
	createTask(t, n, "jobs", spec.Spec{
		Storage:   spec.StorageSpec{MaxSegment: &small},
		Flush:     spec.FlushSpec{MaxBytes: &small},
		Retention: spec.RetentionSpec{MaxAge: &tiny},
	})
	tq, err := n.TaskQueue("jobs")
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("y"), 1024)
	for range 16 {
		_, err = tq.Enqueue(t.Context(), payload)
		require.NoError(t, err)
	}
	// Drain the queue: a live task anchors the truncation floor.
	for range 16 {
		task, err := tq.TryDequeue(t.Context())
		require.NoError(t, err)
		require.NoError(t, tq.Ack(t.Context(), task))
	}
	n.sweepCheckpoints()

	storage, err := n.mgr.GetOf("jobs", manager.KindTask)
	require.NoError(t, err)
	require.Zero(t, storage.MinOffset())

	reclaimed := n.retention.ReconcileAll(t.Context())
	assert.Positive(t, reclaimed["jobs"])
	assert.Positive(t, storage.MinOffset())
}

// TestSweepCheckpointsDirtyQueues: one sweep checkpoints every dirty queue.
func TestSweepCheckpointsDirtyQueues(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	names := []string{"a", "b", "c"}
	for _, name := range names {
		createTask(t, n, name, spec.Spec{})
		tq, err := n.TaskQueue(name)
		require.NoError(t, err)
		_, err = tq.Enqueue(t.Context(), []byte("work"))
		require.NoError(t, err)
	}
	for _, name := range names {
		assert.Positive(t, n.gauge(name, "dirty"), name)
	}

	n.sweepCheckpoints()
	for _, name := range names {
		assert.Zero(t, n.gauge(name, "dirty"), name)
		assert.Positive(t, n.gauge(name, "checkpoint_lsn"), name)
	}
}

// TestSweepSkipsCleanQueues: a clean queue is not rewritten.
func TestSweepSkipsCleanQueues(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	createTask(t, n, "jobs", spec.Spec{})
	tq, err := n.TaskQueue("jobs")
	require.NoError(t, err)
	_, err = tq.Enqueue(t.Context(), []byte("work"))
	require.NoError(t, err)

	n.sweepCheckpoints()
	first := n.gauge("jobs", "checkpoint_lsn")
	require.Positive(t, first)

	n.sweepCheckpoints()
	assert.Equal(t, first, n.gauge("jobs", "checkpoint_lsn"))
}

// TestSweepSkipsUninstalledQueues: an in-flight install is skipped, not awaited.
func TestSweepSkipsUninstalledQueues(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	n.mu.Lock()
	n.taskCatalog["pending"] = &taskEntry{ready: make(chan struct{})}
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		n.sweepCheckpoints()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep blocked on an in-flight install")
	}
}

// TestGoroutineStabilityAcrossScrapes: scraping never creates a goroutine.
func TestGoroutineStabilityAcrossScrapes(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{}, WithMetrics(MetricsConfig{Enabled: true, Interval: time.Hour, History: 8}))
	require.NoError(t, err)
	defer func() { _ = release() }()
	createTask(t, n, "jobs", spec.Spec{})
	tq, err := n.TaskQueue("jobs")
	require.NoError(t, err)
	_, err = tq.Enqueue(t.Context(), []byte("work"))
	require.NoError(t, err)

	before := runtime.NumGoroutine()
	for range 100 {
		_, err := n.Metrics(t.Context())
		require.NoError(t, err)
	}
	assert.Equal(t, before, runtime.NumGoroutine())
}

// gauge reads one published instrument for a queue.
func (n *Node) gauge(name, instrument string) int64 {
	return n.metrics.Registry().Scope(name).Gauge(instrument).Load()
}

// TestNodeLockObserverRecordsContention: a writer holding Node.mu charges the read row.
func TestNodeLockObserverRecordsContention(t *testing.T) {
	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	createTask(t, n, "jobs", spec.Spec{})

	before := n.mu.ReadView()
	n.mu.Lock()
	looked := make(chan struct{})
	go func() {
		defer close(looked)
		_, err := n.TaskQueue("jobs")
		assert.NoError(t, err)
	}()
	time.Sleep(50 * time.Millisecond)
	n.mu.Unlock()
	<-looked

	write, read := n.mu.WriteView(), n.mu.ReadView()
	assert.Equal(t, "node.mu", write.Name)
	assert.Equal(t, "node.mu.read", read.Name)
	assert.Equal(t, before.Waits+1, read.Waits)
	assert.Positive(t, read.MaxWaitMs)
	assert.Less(t, read.MaxWaitMs, float64(5*time.Second/time.Millisecond), "reads must not starve behind a writer")
}