package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// goroutineID parses the current goroutine's id out of its stack header.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}

// TestSingleWriter drives concurrent producers and asserts one goroutine wrote the WAL file.
func TestSingleWriter(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "single.log"))
	require.NoError(t, err)
	defer f.Close()

	w := newWAL(f, Config{BatchSize: 32, MaxInterval: time.Millisecond}, 0)
	w.start()
	defer w.stop()

	var mu sync.Mutex
	writers := map[uint64]struct{}{}
	writerHook = func() {
		mu.Lock()
		writers[goroutineID()] = struct{}{}
		mu.Unlock()
	}
	t.Cleanup(func() { writerHook = nil })

	const goroutines, perGoroutine = 8, 40
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				seq, _, err := w.push([]byte("payload"), int64(g*perGoroutine+i)*7)
				if err != nil {
					t.Error(err)
					return
				}
				if err := w.wait(seq); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, writers, "expected at least one WAL write")
	require.Len(t, writers, 1, "WAL file written by %d distinct goroutines", len(writers))
}

// TestBackpressureBlocksProducer fills the bounded async channel and asserts the next push blocks until canceled.
func TestBackpressureBlocksProducer(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "backpressure.log"))
	require.NoError(t, err)
	defer f.Close()

	// The WAL is never started, so nothing drains the channel and BatchSize is the exact capacity.
	w := newWAL(f, Config{BatchSize: 2}, 0)
	ctx, cancel := context.WithCancel(context.Background())
	entry := asyncEntry{seg: &segment{}, rec: []byte("x"), pos: 0, recLen: 1, offset: 0}

	require.NoError(t, w.pushAsync(ctx, entry))
	require.NoError(t, w.pushAsync(ctx, entry))

	blocked := make(chan error, 1)
	go func() { blocked <- w.pushAsync(ctx, entry) }()
	select {
	case err := <-blocked:
		t.Fatalf("pushAsync returned %v; expected it to block on a full channel", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-blocked:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("pushAsync did not honor cancellation")
	}
}

// TestShutdownWakesParkedFetch parks a Fetch and asserts Close wakes it with ErrQueueClosed.
func TestShutdownWakesParkedFetch(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()
	q, err := openQueue(dir, "fetch-park", Config{}, store)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err := q.Fetch(context.Background(), 0, 10, time.Hour)
		errCh <- err
	}()

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, q.Close())

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrQueueClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("parked Fetch did not wake on Close")
	}
}

// TestDirectoryFsyncSites asserts each engine-side durability site syncs its directory during a scripted workload.
func TestDirectoryFsyncSites(t *testing.T) {
	var count atomic.Int64
	SyncDirHook = func() { count.Add(1) }
	t.Cleanup(func() { SyncDirHook = nil })

	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Site 1: creating a new segment file syncs its directory.
	count.Store(0)
	q, err := openQueue(dir, "durable", Config{
		SegmentSize:     256,
		MaxMessageBytes: 128,
		BatchSize:       4,
		MaxInterval:     time.Millisecond,
		Fsync:           true,
	}, store)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count.Load(), int64(1), "creating a segment must fsync its directory")

	// An append that stays within the active segment does not touch the directory.
	count.Store(0)
	_, err = q.Append(t.Context(), []byte("small"))
	require.NoError(t, err)
	assert.Zero(t, count.Load(), "an append that does not roll must not fsync the directory")

	// Site 1 again: a roll creates a new segment file and syncs its directory.
	count.Store(0)
	payload := bytes.Repeat([]byte("x"), 100)
	for range 8 {
		if len(q.Segments()) >= 2 {
			break
		}
		_, err = q.Append(t.Context(), payload)
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, len(q.Segments()), 2)
	assert.GreaterOrEqual(t, count.Load(), int64(1), "rolling to a new segment must fsync its directory")

	// Site 2: offset-store compaction syncs the directory after its rename.
	count.Store(0)
	require.NoError(t, store.Commit("durable", "group", 1))
	require.NoError(t, store.compactLocked())
	assert.Equal(t, int64(1), count.Load(), "offset compaction must fsync the directory")

	// Site 4: closing the active segment truncates it to its written size and syncs the directory.
	count.Store(0)
	require.NoError(t, q.Close())
	assert.GreaterOrEqual(t, count.Load(), int64(1), "closing the active segment must fsync its directory")
}