package iow

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// The process-wide ring and completion port are singletons that production
	// never closes, so their reapers outlive the test binary by design.
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*iocpPort).loop"),
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*ring).loop"),
	)
}

// factory builds one implementation under test.
type factory struct {
	name string
	new  func(t *testing.T, opts Options) Writer
}

// hostFactories lists every implementation this host can actually run: the pool
// everywhere, io_uring on Linux, IOCP on Windows.
func hostFactories(t *testing.T) []factory {
	t.Helper()
	out := []factory{{name: "pool", new: func(t *testing.T, o Options) Writer { return newPool(nil, o) }}}
	if _, ok := newRingWriter(nil, Options{}); ok {
		out = append(out, factory{name: "ring", new: func(t *testing.T, o Options) Writer {
			w, _ := newRingWriter(nil, o)
			return w
		}})
	}
	if _, ok := newIOCPWriter(nil, Options{}); ok {
		out = append(out, factory{name: "iocp", new: func(t *testing.T, o Options) Writer {
			w, _ := newIOCPWriter(nil, o)
			return w
		}})
	}
	return out
}

// eachFactory runs fn against every implementation available on this host.
func eachFactory(t *testing.T, fn func(t *testing.T, w Writer)) {
	t.Helper()
	for _, f := range hostFactories(t) {
		t.Run(f.name, func(t *testing.T) {
			fn(t, f.new(t, Options{Workers: 4, Durability: DataSync, SyncFiles: nil}))
		})
	}
}

func TestWriterDeliversDoneInSubmissionOrder(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "ordered.bin")

	w, err := New([]*os.File{f}, Options{Workers: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	const producers, batches, recLen = 8, 40, 16
	var mu sync.Mutex
	delivered := make([]uint64, 0, producers*batches)
	fired := make(chan struct{}, producers*batches)
	var submitted atomic.Uint64

	// Submission is serialized so the recorded order is the order the writer saw.
	var submit sync.Mutex
	for p := range producers {
		go func() {
			for b := range batches {
				off := int64((p*batches + b) * recLen)
				buf := bytes.Repeat([]byte{byte(p), byte(b)}, recLen/2)
				submit.Lock()
				n := submitted.Add(1) - 1
				err := w.Write(context.Background(), []Write{{F: f, Buf: buf, Off: off}}, func(err error) {
					require.NoError(t, err)
					mu.Lock()
					delivered = append(delivered, n)
					mu.Unlock()
					fired <- struct{}{}
				})
				submit.Unlock()
				require.NoError(t, err)
			}
		}()
	}
	for range producers * batches {
		<-fired
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, delivered, producers*batches)
	for i, got := range delivered {
		if got != uint64(i) {
			require.Equalf(t, uint64(i), got, "done %d fired out of submission order; first 32: %v", i, delivered[:min(32, len(delivered))])
		}
	}

	data, err := os.ReadFile(filepath.Join(dir, "ordered.bin"))
	require.NoError(t, err)
	require.Len(t, data, producers*batches*recLen)
	for p := range producers {
		for b := range batches {
			off := (p*batches + b) * recLen
			require.Equal(t, bytes.Repeat([]byte{byte(p), byte(b)}, recLen/2), data[off:off+recLen])
		}
	}
}

func TestWriterWritesEveryBufferOfABatch(t *testing.T) {
	dir := t.TempDir()
	log := testFile(t, dir, "log.bin")
	idx := testFile(t, dir, "idx.bin")

	w, err := New([]*os.File{log, idx}, Options{Workers: 2, Durability: DataSync})
	require.NoError(t, err)

	payload := bytes.Repeat([]byte("payload"), 512)
	index := []byte("index-entry")
	tail := 128 + int64(len(payload))
	tailText := []byte("tail")
	require.NoError(t, w.Write(context.Background(), []Write{
		{F: log, Buf: payload, Off: 128},
		{F: idx, Buf: index, Off: 0},
		{F: log, Buf: tailText, Off: tail},
	}, nil))
	require.NoError(t, w.Sync())

	got, err := os.ReadFile(filepath.Join(dir, "log.bin"))
	require.NoError(t, err)
	require.Equal(t, int(tail)+len(tailText), len(got))
	require.Equal(t, payload, got[128:tail])
	require.Equal(t, tailText, got[tail:])

	gotIdx, err := os.ReadFile(filepath.Join(dir, "idx.bin"))
	require.NoError(t, err)
	require.Equal(t, index, gotIdx)

	require.NoError(t, w.Close())
}

func TestWriterRejectsWorkAfterClose(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "closed.log")

	w, err := New([]*os.File{f}, Options{Workers: 1})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())

	var fired error
	require.ErrorIs(t, w.Write(context.Background(), oneWrite(f, 1, 0), func(err error) { fired = err }), ErrClosed)
	require.ErrorIs(t, fired, ErrClosed)
}

func TestWriterRejectsCancelledContext(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "cancelled.log")

	w, err := New([]*os.File{f}, Options{Workers: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var fired error
	require.ErrorIs(t, w.Write(ctx, oneWrite(f, 1, 0), func(err error) { fired = err }), context.Canceled)
	require.ErrorIs(t, fired, context.Canceled)

	data, err := os.ReadFile(filepath.Join(dir, "cancelled.log"))
	require.NoError(t, err)
	require.Empty(t, data, "a cancelled batch must not write bytes")
}

func TestWriterSyncOnIdleWriterIsANoop(t *testing.T) {
	w, err := New(nil, Options{Workers: 1})
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
}

func TestWriterFiresDoneOnceForEveryAcceptedBatch(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "once.bin")

	var calls atomic.Int64
	w, err := New([]*os.File{f}, Options{Workers: 4})
	require.NoError(t, err)

	const total = 64
	for i := range total {
		off := int64(i * 8)
		err := w.Write(context.Background(), []Write{{F: f, Buf: []byte("payload"), Off: off}}, func(error) { calls.Add(1) })
		if err != nil {
			calls.Add(1) // a rejected submission still fires done exactly once
		}
	}
	require.NoError(t, w.Close())
	require.EqualValues(t, total, calls.Load())
}

// TestParityEveryImplementation runs the shared contract suite against every implementation this host offers.
func TestParityEveryImplementation(t *testing.T) {
	eachFactory(t, func(t *testing.T, w Writer) {
		dir := t.TempDir()
		f := testFile(t, dir, "parity.bin")
		fired := make(chan error, 32)

		batches := [][]Write{
			{{F: f, Buf: []byte("first-"), Off: 0}},
			{{F: f, Buf: []byte("second-"), Off: 6}},
			{{F: f, Buf: []byte("third-"), Off: 13}},
		}
		for _, b := range batches {
			require.NoError(t, w.Write(context.Background(), b, func(err error) { fired <- err }))
		}
		for range len(batches) {
			require.NoError(t, <-fired)
		}
		require.NoError(t, w.Sync())

		got, err := os.ReadFile(filepath.Join(dir, "parity.bin"))
		require.NoError(t, err)
		require.Equal(t, "first-second-third-", string(got))

		require.NoError(t, w.Close())

		var after error
		require.ErrorIs(t, w.Write(context.Background(), []Write{{F: f, Buf: []byte("late"), Off: 0}}, func(err error) { after = err }), ErrClosed)
		require.ErrorIs(t, after, ErrClosed)
	})
}
