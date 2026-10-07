package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineAppendCounters(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{Histograms: true})
	mgr, err := OpenManager(dir, testConfig(), nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)

	ctx := t.Context()
	for range 100 {
		_, err := storage.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}

	scope := reg.Scope("q")
	assert.EqualValues(t, 100, scope.Counter("appends").Load())
	assert.EqualValues(t, 700, scope.Counter("bytes_in").Load())
	assert.EqualValues(t, 100, scope.Histogram("append_duration", nil).Snapshot().Total)
}

func TestEngineReadCounters(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{Histograms: true})
	mgr, err := OpenManager(dir, testConfig(), nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)

	ctx := t.Context()
	for i := range 100 {
		_, err := storage.Append(ctx, []byte("payload"))
		require.NoError(t, err)
		_, err = storage.Read(ctx, uint64(i))
		require.NoError(t, err)
	}

	scope := reg.Scope("q")
	assert.EqualValues(t, 100, scope.Counter("reads").Load())
	assert.EqualValues(t, 700, scope.Counter("bytes_out").Load())
	assert.EqualValues(t, 100, scope.Histogram("read_duration", nil).Snapshot().Total)
}

func TestWALFlushCounter(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{})
	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	mgr, err := OpenManager(dir, cfg, nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)
	_, err = storage.Append(t.Context(), []byte("x"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return reg.Scope("q").Counter("wal_flushes").Load() > 0
	}, time.Second, time.Millisecond)
}

func TestManagerCollectShape(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir, testConfig(), nil)
	require.NoError(t, err)
	defer mgr.Close()

	a, err := mgr.Create("a")
	require.NoError(t, err)
	b, err := mgr.Create("b")
	require.NoError(t, err)
	ctx := t.Context()
	_, err = a.Append(ctx, []byte("hello"))
	require.NoError(t, err)
	_, err = b.Append(ctx, []byte("world"))
	require.NoError(t, err)

	var snap metrics.Snapshot
	mgr.Collect(ctx, &snap)
	require.Len(t, snap.Queues, 2)
	for _, qv := range snap.Queues {
		assert.Equal(t, "internal", qv.Kind)
		assert.EqualValues(t, 1, qv.Gauges[GaugeRecords])
		assert.EqualValues(t, 1, qv.Gauges[GaugeSegments])
		assert.Positive(t, qv.Gauges[GaugeSizeBytes])
		assert.EqualValues(t, 1, qv.Gauges[GaugeLastOffset])
	}
	assert.Equal(t, 2, snap.Storage.Queues)
	assert.EqualValues(t, 2, snap.Storage.Segments)
	assert.Positive(t, snap.Storage.SizeBytes)
	assert.Positive(t, snap.Storage.CacheLimit)
}

func TestCacheHitMissCounters(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushWait = time.Millisecond
	reg := metrics.NewRegistry(metrics.Config{})
	mgr, err := OpenManager(dir, cfg, nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)
	q := storage.(*Queue)
	ctx := t.Context()

	for range 10 {
		_, err := storage.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}

	_, err = storage.ReadBatch(ctx, 0, 10)
	require.NoError(t, err)
	require.EqualValues(t, 10, q.m.cacheHits.Load())
	require.Zero(t, q.m.cacheMisses.Load())

	// Wait for the async flusher to land the records, then drop the cache so
	// the same reads fall through to disk.
	require.Eventually(t, func() bool {
		q.w.mu.Lock()
		next := q.w.asyncNext
		q.w.mu.Unlock()
		return next >= 10
	}, time.Second, time.Millisecond)
	q.cache.SetMaxBytes(0)

	_, err = storage.ReadBatch(ctx, 0, 10)
	require.NoError(t, err)
	require.EqualValues(t, 10, q.m.cacheHits.Load())
	require.EqualValues(t, 10, q.m.cacheMisses.Load())
}

func TestInFlightAppends(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	cfg.FlushLinger = 0
	reg := metrics.NewRegistry(metrics.Config{})
	mgr, err := OpenManager(dir, cfg, nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)
	q := storage.(*Queue)

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	entered := make(chan struct{}, 1)
	writerHook = func() {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	defer func() { writerHook = nil }()
	defer releaseAll()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			if _, err := storage.Append(t.Context(), []byte("payload")); err != nil {
				t.Error(err)
			}
		}()
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("WAL flush never reached the writer hook")
	}
	require.Eventually(t, func() bool {
		return q.m.inFlightApp.Load() == int64(n)
	}, 2*time.Second, time.Millisecond)

	var snap metrics.Snapshot
	mgr.Collect(t.Context(), &snap)
	qv, ok := snap.Queue("q")
	require.True(t, ok)
	require.EqualValues(t, n, qv.Producers.InFlight)

	releaseAll()
	wg.Wait()
	require.Eventually(t, func() bool {
		return q.m.inFlightApp.Load() == 0
	}, time.Second, time.Millisecond)
}

func TestInFlightAppendsResetOnError(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	reg := metrics.NewRegistry(metrics.Config{})
	mgr, err := OpenManager(dir, cfg, nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)
	q := storage.(*Queue)

	_, err = storage.AppendBatch(t.Context(), [][]byte{[]byte("ok"), make([]byte, cfg.MaxPayload+1)})
	require.ErrorIs(t, err, ErrMessageTooLarge)
	require.Zero(t, q.m.inFlightApp.Load())
}

func TestWALBlockEvents(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushBytes = 4096
	w := newWAL(&segment{}, cfg, 0, nil, reg.Scope("w"))
	scope := reg.Scope("w")

	for range cap(w.asyncChan) {
		require.NoError(t, w.pushAsync(t.Context(), asyncEntry{rec: []byte("x")}))
	}
	require.Zero(t, scope.Counter("wal_block_events").Load())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.pushAsync(ctx, asyncEntry{rec: []byte("y")}) }()

	require.Eventually(t, func() bool {
		return scope.Counter("wal_block_events").Load() == 1
	}, time.Second, time.Millisecond)
	require.EqualValues(t, 1, scope.Gauge("wal_blocked_producers").Load())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool {
		return scope.Gauge("wal_blocked_producers").Load() == 0
	}, time.Second, time.Millisecond)
}

func TestWALBlockedGaugeDecreases(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushBytes = 4096
	w := newWAL(&segment{}, cfg, 0, nil, reg.Scope("w"))
	scope := reg.Scope("w")

	for range cap(w.asyncChan) {
		require.NoError(t, w.pushAsync(t.Context(), asyncEntry{rec: []byte("x")}))
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = w.pushAsync(ctx, asyncEntry{rec: []byte("y")}) }()
	require.Eventually(t, func() bool {
		return scope.Gauge("wal_blocked_producers").Load() == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		return scope.Gauge("wal_blocked_producers").Load() == 0
	}, time.Second, time.Millisecond)
}

func TestWALBlockedCanceledContext(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushBytes = 4096
	w := newWAL(&segment{}, cfg, 0, nil, reg.Scope("w"))
	scope := reg.Scope("w")

	for range cap(w.asyncChan) {
		require.NoError(t, w.pushAsync(t.Context(), asyncEntry{rec: []byte("x")}))
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, w.pushAsync(ctx, asyncEntry{rec: []byte("y")}), context.Canceled)
	require.Zero(t, scope.Counter("wal_block_events").Load())
}

func TestWALFillRatioBounded(t *testing.T) {
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushBytes = 8192
	w := newWAL(&segment{}, cfg, 0, nil)

	_, _, err := w.pushBatch([][]byte{make([]byte, 512)}, []int64{0}, -1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, w.byteFill(), 0.0)
	require.LessOrEqual(t, w.byteFill(), 1.0)

	for range cap(w.asyncChan) {
		require.NoError(t, w.pushAsync(t.Context(), asyncEntry{rec: []byte("x")}))
	}
	require.GreaterOrEqual(t, w.channelFill(), 0.0)
	require.LessOrEqual(t, w.channelFill(), 1.0)
}

func TestStorageSnapshotShape(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	mgr, err := OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)
	ctx := t.Context()
	for range 10 {
		_, err := storage.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}
	_, err = storage.Read(ctx, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var snap metrics.Snapshot
		mgr.Collect(ctx, &snap)
		return snap.Storage.PendingBytes == 0 && snap.Storage.FlushedBytes > 0
	}, time.Second, time.Millisecond)

	var snap metrics.Snapshot
	mgr.Collect(ctx, &snap)
	st := snap.Storage
	require.Equal(t, 1, st.Queues)
	require.Positive(t, st.Segments)
	require.Positive(t, st.SizeBytes)
	require.Positive(t, st.CacheLimit)
	require.GreaterOrEqual(t, st.CommittedBytes, st.FlushedBytes)
	require.GreaterOrEqual(t, st.FlushedBytes, st.PendingBytes)
	require.Zero(t, st.PendingBytes)
	require.GreaterOrEqual(t, st.OffsetStoreBytes, int64(0))
}

func TestFetchParkEvents(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{})
	mgr, err := OpenManager(dir, testConfig(), nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)
	ctx := t.Context()

	done := make(chan error, 1)
	go func() {
		_, err := storage.Fetch(ctx, 0, 10, 200*time.Millisecond)
		done <- err
	}()

	require.Eventually(t, func() bool {
		return reg.Scope("q").Counter("fetch_park_events").Load() == 1
	}, time.Second, time.Millisecond)
	require.EqualValues(t, 1, reg.Scope("q").Gauge("parked_fetch").Load())

	require.ErrorIs(t, <-done, ErrNoData)
	require.Eventually(t, func() bool {
		return reg.Scope("q").Gauge("parked_fetch").Load() == 0
	}, time.Second, time.Millisecond)
}

func BenchmarkQueueAppendWithMetrics(b *testing.B)    { benchmarkQueueAppend(b, true) }
func BenchmarkQueueAppendWithoutMetrics(b *testing.B) { benchmarkQueueAppend(b, false) }

func benchmarkQueueAppend(b *testing.B, withMetrics bool) {
	dir := b.TempDir()
	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	var (
		mgr *Manager
		err error
	)
	if withMetrics {
		mgr, err = OpenManager(dir, cfg, nil, metrics.NewRegistry(metrics.Config{}))
	} else {
		mgr, err = OpenManager(dir, cfg, nil)
	}
	require.NoError(b, err)
	defer mgr.Close()

	storage, err := mgr.Create("bench")
	require.NoError(b, err)
	payload := []byte("payload")
	ctx := b.Context()
	for b.Loop() {
		if _, err := storage.Append(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
}
