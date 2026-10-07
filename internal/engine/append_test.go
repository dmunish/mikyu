package engine

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/metrics"
)

// TestSyncAppendWakeDrivesFlushCadence pins the sync wake contract: appends
// wake the flusher immediately, so the flush cadence tracks fsync latency
// rather than the MaxFlushWait ticker.
func TestSyncAppendWakeDrivesFlushCadence(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()
	cfg := testConfig()
	cfg.MaxFlushWait = time.Second // the ticker must never drive the cadence
	reg := metrics.NewRegistry(metrics.Config{})
	q, err := openQueue(dir, "wake", cfg, store, nil, reg.Scope("wake"))
	require.NoError(t, err)
	defer q.Close()

	payloads := make([][]byte, 8)
	for i := range payloads {
		payloads[i] = make([]byte, 1024)
	}

	const window = 1500 * time.Millisecond
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := q.AppendBatch(t.Context(), payloads); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	time.Sleep(window)
	close(stop)
	wg.Wait()

	require.Greater(t, q.w.m.flushes.Load(), int64(100),
		"flushes must be wake-driven, not ticker-driven")
}

// TestCommitSyncBatchesIndexWrites pins the sync commit path to write a
// staged batch's index entries in one syscall.
func TestCommitSyncBatchesIndexWrites(t *testing.T) {
	type span struct {
		off, n int64
	}
	dir := t.TempDir()
	mgr, err := OpenManager(dir, testConfig(), nil)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("q")
	require.NoError(t, err)

	var mu sync.Mutex
	var writes []span
	indexWriteHook = func(off int64, n int) {
		mu.Lock()
		defer mu.Unlock()
		writes = append(writes, span{off, int64(n)})
	}
	t.Cleanup(func() { indexWriteHook = nil })

	const count = 1000
	payloads := make([][]byte, count)
	for i := range payloads {
		payloads[i] = []byte("payload")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = storage.AppendBatch(ctx, payloads)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.LessOrEqual(t, len(writes), 4, "1000 sync index entries must coalesce into few writes")

	sort.Slice(writes, func(i, j int) bool { return writes[i].off < writes[j].off })
	var want int64
	for _, s := range writes {
		require.Equal(t, want, s.off, "index writes must tile the index area with no gaps")
		want += s.n
	}
	require.Equal(t, int64(count*rawIndexEntrySize), want, "every index slot must be written exactly once")
}
