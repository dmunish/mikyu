package engine

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
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

	cfg := Config{BatchSize: 2, MaxInterval: time.Millisecond}
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	seq, wake, err := w.push([]byte("record"), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), seq)
	assert.True(t, wake)

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
	cfg := Config{BatchSize: 3, MaxInterval: time.Hour}
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	recs := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	positions := []int64{0, 1, 2}
	seq, wake, err := w.pushBatch(recs, positions)
	require.NoError(t, err)
	assert.Equal(t, int64(2), seq)
	assert.True(t, wake)

	// Signal the flusher to run immediately (batch threshold was reached)
	if wake {
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}

	require.NoError(t, w.wait(seq))
	assert.Equal(t, int64(3), w.published)
}

func TestWALErrorPropagation(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, _ := os.Create(logPath)
	f.Close() // force write error
	cfg := Config{BatchSize: 1, MaxInterval: time.Millisecond}
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	_, _, err := w.push([]byte("x"), 0)
	require.NoError(t, err)
	err = w.wait(0)
	assert.Error(t, err)
}

func TestWALStopFlushesPending(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	f, _ := os.Create(logPath)
	defer f.Close()
	cfg := Config{BatchSize: 10, MaxInterval: time.Hour}
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
	cfg := Config{BatchSize: 100, MaxInterval: time.Millisecond}
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
				seq, _, err := w.push(payload, int64(g*msgs+i))
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

func TestWALWakeTokenLossRace(t *testing.T) {
	testWithTimeout(t, 2*time.Second)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal_race.log")
	f, err := os.Create(logPath)
	require.NoError(t, err)
	defer f.Close()

	// MaxInterval set to 5s so ticker won't hide the missing wake signal
	cfg := Config{BatchSize: 10, MaxInterval: 5 * time.Second, MinInterval: 10 * time.Millisecond}
	w := newWAL(f, cfg, 0)
	w.start()
	defer w.stop()

	// 1. Initial push triggers flusher
	seq1, wake1, err := w.push([]byte("msg1"), 0)
	require.NoError(t, err)
	if wake1 {
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}

	// 2. Allow flusher to enter adaptiveFlush sleep
	time.Sleep(5 * time.Millisecond)

	// 3. Second push while adaptiveFlush is in flight
	seq2, wake2, err := w.push([]byte("msg2"), 4)
	require.NoError(t, err)
	if wake2 {
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}

	// 4. Must complete within 200ms; if wake token was stolen, wait times out after 5s
	done := make(chan error, 1)
	go func() {
		done <- w.wait(seq2)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
		assert.NoError(t, w.wait(seq1))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WAL wait timed out! Wake token was drained by adaptiveFlush for an unflushed message.")
	}
}

func BenchmarkWALFlushAllocations(b *testing.B) {
	dir := b.TempDir()
	logPath := filepath.Join(dir, "bench_wal.log")
	f, err := os.Create(logPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	cfg := Config{BatchSize: 256, MaxInterval: time.Millisecond}
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
