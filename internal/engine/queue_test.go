package engine

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/dmunish/mikyu/internal/spec"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestQueueProduceAndRead(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "test", testConfig(), store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	offsets := make([]uint64, 0, len(payloads))
	for i, payload := range payloads {
		off, err := q.Append(ctx, payload)
		require.NoError(t, err)
		assert.Equal(t, uint64(i), off)
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		msg, err := q.Read(ctx, off)
		require.NoError(t, err)
		assert.Equal(t, payloads[i], msg.Payload)
	}
}

func TestQueueProduceBatch(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()

	q, err := openQueue(dir, "batch", testConfig(), store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	payloads := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	offsets, err := q.AppendBatch(ctx, payloads)
	require.NoError(t, err)
	assert.Len(t, offsets, 3)
	for i, off := range offsets {
		assert.Equal(t, uint64(i), off)
	}

	for i, off := range offsets {
		msg, err := q.Read(ctx, off)
		require.NoError(t, err)
		assert.Equal(t, payloads[i], msg.Payload)
	}
}

func TestQueueProduceConcurrentBatch(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()

	q, err := openQueue(dir, "concurrent-batch", testConfig(), store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	const goroutines = 10
	const batches = 5
	const msgsPerBatch = 2
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*batches)

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for b := range batches {
				payloads := make([][]byte, msgsPerBatch)
				for i := range msgsPerBatch {
					payloads[i] = fmt.Appendf(nil, "g%d-b%d-m%d", g, b, i)
				}
				offsets, err := q.AppendBatch(ctx, payloads)
				if err != nil {
					errCh <- err
					return
				}
				if len(offsets) != msgsPerBatch {
					errCh <- fmt.Errorf("expected %d offsets, got %d", msgsPerBatch, len(offsets))
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	total := int(q.LastOffset())
	assert.Equal(t, goroutines*batches*msgsPerBatch, total)
	for i := range total {
		_, err := q.Read(ctx, uint64(i))
		require.NoError(t, err)
	}
}

func TestQueueReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, _ := openQueue(dir, "range", testConfig(), store)
	defer q.Close()
	_, err := q.Read(t.Context(), 10)
	assert.True(t, errors.Is(err, ErrOffsetNotFound))
}

func TestQueueRolling(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	cfg := testConfig()
	cfg.MaxSegment = 128
	q, err := openQueue(dir, "roll", cfg, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	for i := range 20 {
		_, err := q.Append(ctx, fmt.Appendf(nil, "message-%d", i))
		require.NoError(t, err)
	}
	assert.GreaterOrEqual(t, len(q.segments), 2)
	for i := range 20 {
		msg, err := q.Read(ctx, uint64(i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("message-%d", i), string(msg.Payload))
	}
}

func TestQueueProduceConcurrentBatchExactOffsets(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, err := openQueue(dir, "concurrent-batch-exact", testConfig(), store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	const goroutines = 8
	const batches = 5
	const msgsPerBatch = 4

	var mu sync.Mutex
	expectedOffsets := make(map[uint64]string)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*batches)

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for b := range batches {
				payloads := make([][]byte, msgsPerBatch)
				for i := range msgsPerBatch {
					payload := fmt.Sprintf("g%d-b%d-m%d", g, b, i)
					payloads[i] = []byte(payload)
				}
				offsets, err := q.AppendBatch(ctx, payloads)
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				for i, off := range offsets {
					expectedOffsets[off] = string(payloads[i])
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	for off, payload := range expectedOffsets {
		msg, err := q.Read(ctx, off)
		require.NoError(t, err)
		assert.Equal(t, payload, string(msg.Payload), "offset %d has wrong payload", off)
	}

	assert.Equal(t, goroutines*batches*msgsPerBatch, len(expectedOffsets))
}

func TestQueueAsyncWriteFailureStopsSubsequentAppends(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushWait = time.Millisecond
	q, err := openQueue(dir, "async-error", cfg, store)
	require.NoError(t, err)
	defer q.Close()
	q.mu.RLock()
	file := q.active.logFile
	q.mu.RUnlock()
	require.NoError(t, file.Close())

	_, err = q.Append(t.Context(), []byte("first"))
	require.NoError(t, err, "buffered mode accepts the write before the async failure is observed")

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := q.w.currentError(); err != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Error(t, q.w.currentError())
	_, err = q.Append(t.Context(), []byte("second"))
	assert.Error(t, err)
}

func TestQueueOperationsAfterCloseReturnErrQueueClosed(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "closed", testConfig(), store)
	require.NoError(t, err)
	require.NoError(t, q.Close())

	_, err = q.Append(t.Context(), []byte("x"))
	assert.ErrorIs(t, err, ErrQueueClosed)
	_, err = q.Read(t.Context(), 0)
	assert.ErrorIs(t, err, ErrQueueClosed)
}

func TestQueueAppendBatchAcrossSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.MaxSegment = 256
	cfg.MaxPayload = 128
	cfg.MaxFlushWait = time.Millisecond
	q, err := openQueue(dir, "batch-roll", cfg, store)
	require.NoError(t, err)

	payloads := make([][]byte, 20)
	for i := range payloads {
		payloads[i] = fmt.Appendf(nil, "payload-%02d-xxxxxxxxxxxxxxxxxxxxxxxx", i)
	}
	offsets, err := q.AppendBatch(t.Context(), payloads)
	require.NoError(t, err)
	assert.Len(t, offsets, len(payloads))
	assert.GreaterOrEqual(t, len(q.Segments()), 2)
	for i, off := range offsets {
		msg, err := q.Read(t.Context(), off)
		require.NoError(t, err)
		assert.Equal(t, payloads[i], msg.Payload)
	}
	require.NoError(t, q.Close())

	store2, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store2.Close()
	q2, err := openQueue(dir, "batch-roll", testConfig(), store2)
	require.NoError(t, err)
	defer q2.Close()
	for i, off := range offsets {
		msg, err := q2.Read(t.Context(), off)
		require.NoError(t, err)
		assert.Equal(t, payloads[i], msg.Payload)
	}
}

func TestQueueProduceMessageTooLarge(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	cfg := testConfig()
	cfg.MaxPayload = 10
	q, _ := openQueue(dir, "large", cfg, store)
	defer q.Close()

	_, err := q.Append(t.Context(), make([]byte, 11))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrMessageTooLarge))
}

func TestQueueProduceBatchRecordExceedsSegmentSize(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	cfg := testConfig()
	cfg.MaxSegment = 64
	cfg.MaxPayload = 1024
	q, err := openQueue(dir, "oversized-batch", cfg, store)
	require.NoError(t, err)
	defer q.Close()

	payloads := [][]byte{make([]byte, 100), []byte("ok")}
	_, err = q.AppendBatch(t.Context(), payloads)
	assert.ErrorIs(t, err, ErrRecordTooLarge)
}

func TestQueueCommitOffset(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, _ := openQueue(dir, "offset", testConfig(), store)
	defer q.Close()

	ctx := t.Context()
	err := q.CommitOffset(ctx, "group1", 42)
	require.NoError(t, err)
	off, err := q.Committed(ctx, "group1")
	require.NoError(t, err)
	assert.Equal(t, uint64(42), off)
}

func TestQueueUngracefulCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)

	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	q, err := openQueue(dir, "crash-queue", cfg, store)
	require.NoError(t, err)

	ctx := t.Context()
	for i := range 5 {
		_, err := q.Append(ctx, fmt.Appendf(nil, "msg-%d", i))
		require.NoError(t, err)
	}

	_ = q.Close()
	_ = store.Close()

	store2, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	recoveredQueue, err := openQueue(dir, "crash-queue", testConfig(), store2)
	require.NoError(t, err)
	defer recoveredQueue.Close()

	assert.Equal(t, uint64(5), recoveredQueue.LastOffset())
	for i := range 5 {
		msg, err := recoveredQueue.Read(ctx, uint64(i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("msg-%d", i), string(msg.Payload))
	}
}

// TestQueueRealCrashRecovery runs a child process that performs durable appends,
// then kills the process without allowing Queue.Close to run. The parent opens
// the same queue and verifies every append that had already completed before
// the crash remains readable in order.
func TestQueueRealCrashRecovery(t *testing.T) {
	if os.Getenv("MIKYU_CRASH_HELPER") == "1" {
		t.Helper()
		dir := os.Getenv("MIKYU_CRASH_DIR")
		marker := os.Getenv("MIKYU_CRASH_MARKER")
		syncsFile := os.Getenv("MIKYU_CRASH_SYNCS")
		var syncs atomic.Int64
		SyncDirHook = func() { syncs.Add(1) }
		store, err := openOffsetStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		cfg := testConfig()
		cfg.MaxSegment = 1024
		cfg.MaxPayload = 64
		cfg.MaxFlushWait = 10 * time.Millisecond
		q, err := openQueue(dir, "real-crash", cfg, store)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		defer func() { _ = q.Close(); _ = store.Close() }()
		for i := range 2_000 {
			payload := fmt.Appendf(nil, "msg-%06d", i)
			if _, err := q.Append(t.Context(), payload); err != nil {
				t.Fatal(err)
			}
			if (i+1)%100 == 0 {
				// Publish the fsync count first so the parent never observes a marker without it.
				if err := os.WriteFile(syncsFile, []byte(strconv.FormatInt(syncs.Load(), 10)), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(marker, []byte(strconv.Itoa(i+1)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
		}
		return
	}

	dir := t.TempDir()
	marker := filepath.Join(dir, "progress")
	syncsFile := filepath.Join(dir, "syncs")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestQueueRealCrashRecovery$")
	cmd.Env = append(os.Environ(),
		"MIKYU_CRASH_HELPER=1",
		"MIKYU_CRASH_DIR="+dir,
		"MIKYU_CRASH_MARKER="+marker,
		"MIKYU_CRASH_SYNCS="+syncsFile,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// 30s: the durable path blocks on fsyncs, and the child competes with
	// the rest of the suite for disk under parallel runs.
	deadline := time.Now().Add(30 * time.Second)
	var completed int
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(marker)
		if err == nil {
			completed, _ = strconv.Atoi(strings.TrimSpace(string(data)))
			if completed >= 500 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if completed < 500 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child did not reach durable progress marker; completed=%d", completed)
	}

	// The durable path must have fsynced segment directories while appending.
	syncsData, err := os.ReadFile(syncsFile)
	require.NoError(t, err)
	syncs, _ := strconv.Atoi(strings.TrimSpace(string(syncsData)))
	require.Greater(t, syncs, 0, "durable appends must fsync segment directories")

	// Kill the child abruptly: no Queue.Close, WAL stop, segment trim, or
	// manager cleanup is allowed to run.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()
	cfg := testConfig()
	cfg.MaxSegment = 128
	cfg.MaxPayload = 64
	cfg.MaxFlushWait = 2 * time.Millisecond
	q, err := openQueue(dir, "real-crash", cfg, store)
	require.NoError(t, err)
	defer q.Close()

	require.GreaterOrEqual(t, q.LastOffset(), uint64(completed), "all completed durable appends must survive the crash")
	for i := 0; i < completed; i++ {
		msg, err := q.Read(t.Context(), uint64(i))
		require.NoError(t, err, "offset %d after crash recovery", i)
		require.Equal(t, fmt.Sprintf("msg-%06d", i), string(msg.Payload))
	}
}

func TestQueueConcurrentProduceDuringRoll(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()

	cfg := testConfig()
	cfg.MaxSegment = 256
	q, err := openQueue(dir, "roll-race", cfg, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	const goroutines = 10
	const msgsPerRoutine = 20
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range msgsPerRoutine {
				_, err := q.Append(ctx, fmt.Appendf(nil, "g%d-m%d", g, i))
				assert.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()

	totalMsgs := uint64(goroutines * msgsPerRoutine)
	assert.Equal(t, totalMsgs, q.LastOffset())
	assert.GreaterOrEqual(t, len(q.segments), 2)

	for i := range totalMsgs {
		_, err := q.Read(ctx, i)
		require.NoError(t, err, "offset %d failed to read after concurrent roll", i)
	}
}

func TestQueueReadUsesMMapOnRolledSegments(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()

	cfg := testConfig()
	cfg.MaxSegment = 100
	q, err := openQueue(dir, "mmap-verify", cfg, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()
	payload := []byte("0123456789")
	for range 3 {
		_, err := q.Append(ctx, payload)
		require.NoError(t, err)
	}

	require.GreaterOrEqual(t, len(q.segments), 2)

	q.mu.RLock()
	closedSeg := q.segments[0]
	q.mu.RUnlock()

	assert.NotNil(t, closedSeg.mmapData, "closed segment payload must have mmap enabled for zero-copy reads")
	assert.NotNil(t, closedSeg.idxMMap, "closed segment index must have mmap enabled for zero-copy index reads")
}

func TestQueueConcurrentIndexDiskOrdering(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	q, err := openQueue(dir, "disk-index-order", cfg, store)
	require.NoError(t, err)

	ctx := t.Context()
	const goroutines = 16
	const msgsPerRoutine = 50
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for range msgsPerRoutine {
				_, err := q.Append(ctx, []byte("payload"))
				assert.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, q.Close())

	// Re-open active segment raw index file and verify positions are strictly monotonic
	seg, err := openSegment(filepath.Join(dir, "disk-index-order"), 0, minSegmentPrealloc)
	require.NoError(t, err)
	defer seg.Close()

	entries, err := seg.recover()
	require.NoError(t, err)
	assert.Len(t, entries, goroutines*msgsPerRoutine)

	var lastPos int64 = -1
	for i, e := range entries {
		assert.Greater(t, e.pos, lastPos, "index entry %d on disk is out of order!", i)
		lastPos = e.pos
	}
}

func TestQueueVelocityPreallocationScaling(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// MaxSegment set to 64 MiB so it doesn't clamp minSegmentPrealloc (8 MiB)
	q, err := openQueue(dir, "velocity-test", testConfig(), store)
	require.NoError(t, err)
	defer q.Close()

	ctx := t.Context()

	// Initial pre-allocation target starts at minSegmentPrealloc (8 MB)
	assert.Equal(t, int64(minSegmentPrealloc), q.nextPreallocBytes)

	// 1. Fast fill (< 10s): Trigger roll and verify target doubles to 16 MB
	_, err = q.Append(ctx, []byte("large-payload-to-force-roll-1"))
	require.NoError(t, err)

	q.mu.Lock()
	err = q.roll()
	q.mu.Unlock()
	require.NoError(t, err)

	assert.Equal(t, int64(16*1024*1024), q.nextPreallocBytes)

	// 2. Slow fill (> 5m): Artificially age active segment's createdAt time
	q.mu.Lock()
	q.active.createdAt = time.Now().Add(-6 * time.Minute)
	err = q.roll()
	q.mu.Unlock()
	require.NoError(t, err)

	// Target should decay back down to the 8 MB floor
	assert.Equal(t, int64(minSegmentPrealloc), q.nextPreallocBytes)
}

func TestSegmentPreallocationTruncatedOnClose(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "trunc-test", testConfig(), store)
	require.NoError(t, err)

	ctx := t.Context()
	_, err = q.Append(ctx, []byte("hello world"))
	require.NoError(t, err)

	// Active segment file initially has full pre-allocated capacity on disk.
	// Segment handles are opened through an os.Root, so Name() is relative
	// to the queue directory; rebuild the full path for the external stat.
	activePath := filepath.Join(dir, "trunc-test", q.active.logName)
	info, err := os.Stat(activePath)
	require.NoError(t, err)
	assert.Equal(t, int64(minSegmentPrealloc), info.Size())

	// Closing queue must truncate file down to actual written length (s.pos)
	writtenPos := q.active.pos
	require.NoError(t, q.Close())

	info, err = os.Stat(activePath)
	require.NoError(t, err)
	assert.Equal(t, writtenPos, info.Size())
	assert.Less(t, info.Size(), int64(minSegmentPrealloc))
}
