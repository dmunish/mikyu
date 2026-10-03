package engine

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testWithTimeout ensures a test does not hang forever.
func testWithTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		select {
		case <-done:
		case <-time.After(timeout):
			panic("test timed out")
		}
	}()
}

func TestWALPushAndWait(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, err := os.Create(logPath)
	require.NoError(t, err)
	defer f.Close()

	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	seq, wake, err := w.push([]byte("record"), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(6), seq, "seq is the byte offset after the record")
	assert.False(t, wake, "a small push stays under the byte budget")

	time.Sleep(5 * time.Millisecond)
	require.NoError(t, w.wait(seq))

	seq2, _, err := w.push([]byte("record2"), int64(len("record")))
	require.NoError(t, err)
	require.NoError(t, w.wait(seq2))
}

func TestWALBatch(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, _ := os.Create(logPath)
	defer f.Close()
	cfg := testConfig()
	cfg.MaxFlushBytes = 3 // bytes, so three one-byte records hit the budget
	cfg.MaxFlushWait = time.Hour
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	recs := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	positions := []int64{0, 1, 2}
	seq, wake, err := w.pushBatch(recs, positions)
	require.NoError(t, err)
	assert.Equal(t, int64(3), seq)
	assert.True(t, wake, "the byte budget was reached")

	// Signal the flusher to run immediately (byte threshold was reached)
	if wake {
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}

	require.NoError(t, w.wait(seq))
	assert.Equal(t, int64(3), w.publishedBytes)
}

func TestWALErrorPropagation(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, _ := os.Create(logPath)
	f.Close() // force write error
	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	_, _, err := w.push([]byte("x"), 0)
	require.NoError(t, err)
	err = w.wait(int64(len("x")))
	assert.Error(t, err)
}

func TestWALStopFlushesPending(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, _ := os.Create(logPath)
	defer f.Close()
	cfg := testConfig()
	cfg.MaxFlushWait = time.Hour
	w := newWAL(f, cfg, 0)
	w.start()
	_, _, _ = w.push([]byte("data"), 0)
	stopErr := w.stop()
	assert.NoError(t, stopErr)
	info, _ := f.Stat()
	assert.Equal(t, int64(4), info.Size())
}

func TestWALConcurrentPush(t *testing.T) {
	testWithTimeout(t, 5*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, _ := os.Create(logPath)
	defer f.Close()
	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	const goroutines = 10
	const msgs = 100
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range msgs {
				payload := []byte{byte(g), byte(i)}
				seq, _, err := w.push(payload, int64(g*msgs+i)*2)
				if err != nil {
					errCh <- err
					return
				}
				if err := w.wait(seq); err != nil {
					errCh <- err
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
}

// TestWALByteBackpressure fills the async channel to its byte-derived capacity and asserts the next push blocks until cancel.
func TestWALByteBackpressure(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "backpressure.log")
	f, err := os.Create(logPath)
	require.NoError(t, err)
	defer f.Close()

	// The WAL is never started, so nothing drains the channel and
	// MaxFlushBytes/4096 is the exact capacity.
	cfg := testConfig()
	cfg.MaxFlushBytes = 8192
	w := newWAL(f, cfg, 0)
	ctx, cancel := context.WithCancel(context.Background())
	entry := asyncEntry{seg: &segment{}, rec: []byte("x"), pos: 0, recLen: 1, offset: 0}

	const capacity = 8192 / 4096
	for range capacity {
		require.NoError(t, w.pushAsync(ctx, entry))
	}

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

// TestWALWakeTokenLossRace pins the wake-token protocol: a push that crosses the
// byte budget returns wake=true, and the token survives a flusher that is mid-linger,
// so wait(seq) unblocks. Virtual time (synctest) makes the interleaving deterministic;
// real sleeps here raced the flusher's FlushLinger under load.
func TestWALWakeTokenLossRace(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal_race.log")
	f, err := os.Create(logPath)
	require.NoError(t, err)
	defer f.Close()

	// MaxFlushWait set to 5s so the ticker won't hide the missing wake signal
	cfg := testConfig()
	cfg.MaxFlushBytes = 8
	cfg.MaxFlushWait = 5 * time.Second
	cfg.FlushLinger = 10 * time.Millisecond

	synctest.Test(t, func(t *testing.T) {
		w := newWAL(f, cfg, 0)
		w.start()
		defer w.stop()

		// 1. Initial push is under the byte budget; nudge the flusher so it enters
		// its adaptive linger.
		seq1, wake1, err := w.push([]byte("msg1"), 0)
		require.NoError(t, err)
		require.False(t, wake1)
		select {
		case w.wake <- struct{}{}:
		default:
		}

		// 2. Advance to mid-linger: the flusher has consumed the token and is parked
		// in its linger sleep, while the producer's 5ms sleep elapses first.
		time.Sleep(5 * time.Millisecond)

		// 3. Second push reaches the byte budget while adaptiveFlush is in flight
		seq2, wake2, err := w.push([]byte("msg2"), 4)
		require.NoError(t, err)
		require.True(t, wake2)
		if wake2 {
			select {
			case w.wake <- struct{}{}:
			default:
			}
		}

		// 4. The linger ends, both records publish, and wait unblocks. A lost token
		// would leave wait parked and the bubble deadlocks instead of hanging.
		assert.NoError(t, w.wait(seq2))
		assert.NoError(t, w.wait(seq1))
		w.mu.Lock()
		published := w.publishedBytes
		w.mu.Unlock()
		assert.GreaterOrEqual(t, published, seq2, "linger-time flush must publish both records")
	})
}

// TestWALAsyncBatchesWrites pins the async flusher to coalesce same-segment
// runs into one log write per flush, and verifies the written ranges tile the
// record area exactly once.
func TestWALAsyncBatchesWrites(t *testing.T) {
	testWithTimeout(t, 5*time.Second)
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxFlushBytes = 1 << 20
	cfg.MaxFlushWait = time.Millisecond
	seg, err := openSegment(dir, 0, 0)
	require.NoError(t, err)
	defer seg.Close()
	w := newWAL(seg.logFile, cfg, 0)
	w.start()

	type span struct{ off, n int64 }
	var mu sync.Mutex
	var writes []span
	writeAtHook = func(off int64, n int) {
		mu.Lock()
		defer mu.Unlock()
		writes = append(writes, span{off, int64(n)})
	}
	t.Cleanup(func() { writeAtHook = nil })

	const count = 1000
	const recLen = 4

	for i := range count {
		e := asyncEntry{
			seg:    seg,
			rec:    []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)},
			pos:    int64(i) * recLen,
			recLen: recLen,
			offset: uint64(i),
		}
		require.NoError(t, w.pushAsync(t.Context(), e))
	}

	require.NoError(t, w.stop())

	mu.Lock()
	defer mu.Unlock()
	require.LessOrEqual(t, len(writes), 8, "1000 entries must coalesce into few writes")

	sort.Slice(writes, func(i, j int) bool { return writes[i].off < writes[j].off })
	var want int64
	for _, s := range writes {
		require.Equal(t, want, s.off, "written ranges must tile the record area with no gaps")
		want += s.n
	}
	require.Equal(t, int64(count*recLen), want, "every record byte must be written exactly once")
}

func BenchmarkWALFlushAllocations(b *testing.B) {
	dir := b.TempDir()
	logPath := filepath.Join(dir, "bench_wal.log")
	f, err := os.Create(logPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	cfg := testConfig()
	cfg.MaxFlushWait = time.Millisecond
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	payload := make([]byte, 512)

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		seq, _, err := w.push(payload, int64(i*len(payload)))
		if err != nil {
			b.Fatal(err)
		}
		if err := w.wait(seq); err != nil {
			b.Fatal(err)
		}
	}
}
