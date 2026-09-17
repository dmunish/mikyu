package engine

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineAppendCounters(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{Histograms: true})
	mgr, err := OpenManager(dir, Config{}, nil, reg)
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
	mgr, err := OpenManager(dir, Config{}, nil, reg)
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
	mgr, err := OpenManager(dir, Config{BatchSize: 1, MaxInterval: time.Millisecond}, nil, reg)
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
	mgr, err := OpenManager(dir, Config{}, nil)
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
		assert.Equal(t, "shared", qv.Kind)
		assert.EqualValues(t, 1, qv.Gauges[GaugeRecords])
		assert.EqualValues(t, 1, qv.Gauges[GaugeSegments])
		assert.Positive(t, qv.Gauges[GaugeSizeBytes])
		assert.EqualValues(t, 1, qv.Gauges[GaugeLastOffset])
	}
}

func BenchmarkQueueAppendWithMetrics(b *testing.B)    { benchmarkQueueAppend(b, true) }
func BenchmarkQueueAppendWithoutMetrics(b *testing.B) { benchmarkQueueAppend(b, false) }

func benchmarkQueueAppend(b *testing.B, withMetrics bool) {
	dir := b.TempDir()
	cfg := Config{BatchSize: 256, MaxInterval: time.Millisecond}
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
