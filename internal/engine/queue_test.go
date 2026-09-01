package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestQueueProduceAndRead(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "test", Config{MaxMessageBytes: 1024}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	offsets := make([]uint64, 0, len(payloads))
	for i, payload := range payloads {
		off, err := q.Produce(ctx, payload)
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

	q, err := openQueue(dir, "batch", Config{}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	payloads := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	offsets, err := q.ProduceBatch(ctx, payloads)
	require.NoError(t, err)
	assert.Len(t, offsets, 3)
	for i, off := range offsets {
		assert.Equal(t, uint64(i), off)
	}
	// Read back
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

	q, err := openQueue(dir, "concurrent-batch", Config{}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	const goroutines = 10
	const batches = 5
	const msgsPerBatch = 2
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*batches)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				payloads := make([][]byte, msgsPerBatch)
				for i := 0; i < msgsPerBatch; i++ {
					payloads[i] = fmt.Appendf(nil, "g%d-b%d-m%d", g, b, i)
				}
				offsets, err := q.ProduceBatch(ctx, payloads)
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

	// Verify total count and that all messages are readable
	total := int(q.LastOffset())
	assert.Equal(t, goroutines*batches*msgsPerBatch, total)
	for i := 0; i < total; i++ {
		_, err := q.Read(ctx, uint64(i))
		require.NoError(t, err)
	}
}

func TestQueueReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, _ := openQueue(dir, "range", Config{}, store)
	defer q.Close()
	_, err := q.Read(context.Background(), 10)
	assert.True(t, errors.Is(err, ErrOffsetNotFound))
}

func TestQueueRolling(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, err := openQueue(dir, "roll", Config{MaxSegmentBytes: 128}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := q.Produce(ctx, fmt.Appendf(nil, "message-%d", i))
		require.NoError(t, err)
	}
	assert.GreaterOrEqual(t, len(q.segments), 2)
	for i := 0; i < 20; i++ {
		msg, err := q.Read(ctx, uint64(i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("message-%d", i), string(msg.Payload))
	}
}

func TestQueueProduceConcurrentBatchExactOffsets(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, err := openQueue(dir, "concurrent-batch-exact", Config{BatchSize: 10}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	const goroutines = 8
	const batches = 5
	const msgsPerBatch = 4

	var mu sync.Mutex
	expectedOffsets := make(map[uint64]string)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*batches)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				payloads := make([][]byte, msgsPerBatch)
				for i := 0; i < msgsPerBatch; i++ {
					payload := fmt.Sprintf("g%d-b%d-m%d", g, b, i)
					payloads[i] = []byte(payload)
				}
				offsets, err := q.ProduceBatch(ctx, payloads)
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

func TestQueueProduceMessageTooLarge(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, _ := openQueue(dir, "large", Config{MaxMessageBytes: 10}, store)
	defer q.Close()

	_, err := q.Produce(context.Background(), make([]byte, 11))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrMessageTooLarge))
}

func TestQueueProduceBatchMessageExceedsSegmentSize(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, err := openQueue(dir, "oversized-batch", Config{MaxSegmentBytes: 64, MaxMessageBytes: 1024}, store)
	require.NoError(t, err)
	defer q.Close()

	payloads := [][]byte{make([]byte, 100), []byte("ok")} // first is too large
	_, err = q.ProduceBatch(context.Background(), payloads)
	assert.ErrorIs(t, err, ErrRecordTooLarge)
}

func TestQueueCommitOffset(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()
	q, _ := openQueue(dir, "offset", Config{}, store)
	defer q.Close()

	ctx := context.Background()
	err := q.CommitOffset(ctx, "group1", 42)
	require.NoError(t, err)
	off, err := q.FetchOffset(ctx, "group1")
	require.NoError(t, err)
	assert.Equal(t, uint64(42), off)
}

func TestQueueUngracefulCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)

	q, err := openQueue(dir, "crash-queue", Config{BatchSize: 1, Interval: time.Millisecond}, store)
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := q.Produce(ctx, fmt.Appendf(nil, "msg-%d", i))
		require.NoError(t, err)
	}

	// Close queue to release OS file locks (required for Windows TempDir cleanup)
	_ = q.Close()
	_ = store.Close()

	// Re-open queue from disk to test index recovery from log files
	store2, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	recoveredQueue, err := openQueue(dir, "crash-queue", Config{}, store2)
	require.NoError(t, err)
	defer recoveredQueue.Close()

	assert.Equal(t, uint64(5), recoveredQueue.LastOffset())
	for i := 0; i < 5; i++ {
		msg, err := recoveredQueue.Read(ctx, uint64(i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("msg-%d", i), string(msg.Payload))
	}
}

func TestQueueConcurrentProduceDuringRoll(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()

	// MaxSegmentBytes set small enough to force frequent segment rolls
	q, err := openQueue(dir, "roll-race", Config{MaxSegmentBytes: 256, BatchSize: 10}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	const goroutines = 10
	const msgsPerRoutine = 20
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < msgsPerRoutine; i++ {
				_, err := q.Produce(ctx, fmt.Appendf(nil, "g%d-m%d", g, i))
				assert.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()

	totalMsgs := uint64(goroutines * msgsPerRoutine)
	assert.Equal(t, totalMsgs, q.LastOffset())
	assert.GreaterOrEqual(t, len(q.segments), 2)

	for i := uint64(0); i < totalMsgs; i++ {
		_, err := q.Read(ctx, i)
		require.NoError(t, err, "offset %d failed to read after concurrent roll", i)
	}
}

func TestQueueReadUsesMMapOnRolledSegments(t *testing.T) {
	dir := t.TempDir()
	store, _ := openOffsetStore(dir)
	defer store.Close()

	// 10B payload + 34B header = 44B record.
	// MaxSegmentBytes = 100 allows 2 records per segment before rolling.
	q, err := openQueue(dir, "mmap-verify", Config{MaxSegmentBytes: 100}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()
	payload := []byte("0123456789") // 10 bytes
	for i := 0; i < 3; i++ {
		_, err := q.Produce(ctx, payload)
		require.NoError(t, err)
	}

	require.GreaterOrEqual(t, len(q.segments), 2)

	q.mu.RLock()
	entry := q.index[0]
	q.mu.RUnlock()

	assert.NotNil(t, entry.seg.mmapData, "closed segment must have mmap enabled for zero-copy reads")
}
