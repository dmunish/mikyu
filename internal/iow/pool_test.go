package iow

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// testFile opens a scratch file that the pool may close.
func testFile(t *testing.T, dir, name string) *os.File {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// stubWriteAt replaces the write primitive and restores it after the test.
func stubWriteAt(t *testing.T, fn func(*os.File, []byte, int64) (int, error)) {
	t.Helper()
	prev := writeAt
	writeAt = fn
	t.Cleanup(func() { writeAt = prev })
}

// stubSyncData replaces the fsync primitive and restores it after the test.
func stubSyncData(t *testing.T, fn func(*os.File) error) {
	t.Helper()
	prev := syncData
	syncData = fn
	t.Cleanup(func() { syncData = prev })
}

// oneWrite renders a single-buffer batch tagged with marker as its first byte.
func oneWrite(f *os.File, marker byte, off int64) []Write {
	buf := make([]byte, 8)
	buf[0] = marker
	return []Write{{F: f, Buf: buf, Off: off}}
}

// awaitDone submits one batch and returns a channel closed when its done fires.
func awaitDone(p *pool, batch []Write) chan error {
	done := make(chan error, 1)
	if err := p.Write(context.Background(), batch, func(err error) { done <- err }); err != nil {
		close(done)
	}
	return done
}

func TestOrderedDelivery(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "ordered.log")

	const total = 64
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	written := make(chan struct{}, total)
	stubWriteAt(t, func(_ *os.File, buf []byte, _ int64) (int, error) {
		if buf[0] == 0 {
			<-gate
		}
		written <- struct{}{}
		return len(buf), nil
	})

	p := newPool([]*os.File{f}, Options{Workers: 4, Outstanding: total})
	t.Cleanup(func() { _ = p.Close() })

	delivered := make(chan error, total)
	for i := range total {
		marker := byte(1)
		if i == 0 {
			marker = 0
		}
		require.NoError(t, p.Write(context.Background(), oneWrite(f, marker, int64(i*8)), func(err error) { delivered <- err }))
	}

	for range total - 1 {
		<-written
	}
	select {
	case err := <-delivered:
		t.Fatalf("done fired %v while batch 0 was still in flight", err)
	default:
	}
	release()

	for i := range total {
		require.NoErrorf(t, <-delivered, "batch %d", i)
	}
}

func TestBoundedSubmissionBlocksAndCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		f := testFile(t, dir, "bounded.log")

		started := make(chan struct{})
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		stubWriteAt(t, func(_ *os.File, buf []byte, _ int64) (int, error) {
			if buf[0] == 0 {
				close(started)
				<-gate
			}
			return len(buf), nil
		})

		p := newPool([]*os.File{f}, Options{Workers: 1, Outstanding: 1})
		t.Cleanup(func() { _ = p.Close() })

		require.NoError(t, p.Write(context.Background(), oneWrite(f, 0, 0), nil))
		<-started
		require.NoError(t, p.Write(context.Background(), oneWrite(f, 1, 8), nil))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fired := make(chan error, 1)
		got := make(chan error, 1)
		go func() {
			got <- p.Write(ctx, oneWrite(f, 2, 16), func(err error) { fired <- err })
		}()
		synctest.Wait()

		select {
		case err := <-got:
			t.Fatalf("Write returned %v while the pool was full", err)
		default:
		}
		select {
		case err := <-fired:
			t.Fatalf("done fired %v while the pool was full", err)
		default:
		}

		cancel()
		require.ErrorIs(t, <-got, context.Canceled)

		release()
		require.ErrorIs(t, <-fired, context.Canceled)
	})
}

func TestFailurePropagation(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "fail.log")

	errBoom := errors.New("boom")
	stubWriteAt(t, func(_ *os.File, buf []byte, _ int64) (int, error) {
		if buf[0] == 3 {
			return 0, errBoom
		}
		return len(buf), nil
	})

	p := newPool([]*os.File{f}, Options{Workers: 1, Outstanding: 4})

	const total = 6
	results := make(chan error, total)
	for i := range total {
		err := p.Write(context.Background(), oneWrite(f, byte(i), int64(i*8)), func(err error) { results <- err })
		if i < 3 {
			require.NoErrorf(t, err, "batch %d", i)
		} else if err != nil {
			require.ErrorIsf(t, err, errBoom, "batch %d", i)
		}
	}
	for i := range total {
		err := <-results
		if i < 3 {
			require.NoErrorf(t, err, "batch %d", i)
		} else {
			require.ErrorIsf(t, err, errBoom, "batch %d", i)
		}
	}

	var fired error
	require.ErrorIs(t, p.Write(context.Background(), oneWrite(f, 9, 0), func(err error) { fired = err }), errBoom)
	require.ErrorIs(t, fired, errBoom)
	require.ErrorIs(t, p.Sync(), errBoom)
	require.ErrorIs(t, p.Close(), errBoom)
}

func TestWriteErrorEntersFailedState(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "short.log")

	errShort := errors.New("short write")
	stubWriteAt(t, func(_ *os.File, buf []byte, _ int64) (int, error) { return 0, errShort })

	p := newPool([]*os.File{f}, Options{Workers: 1})
	require.ErrorIs(t, <-awaitDone(p, oneWrite(f, 1, 0)), errShort)
	require.ErrorIs(t, p.Write(context.Background(), oneWrite(f, 1, 8), nil), errShort)
	require.ErrorIs(t, p.Close(), errShort)
}

func TestCloseDuringFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		f := testFile(t, dir, "close.log")

		started := make(chan struct{})
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		stubWriteAt(t, func(_ *os.File, _ []byte, _ int64) (int, error) {
			close(started)
			<-gate
			return 8, nil
		})

		p := newPool([]*os.File{f}, Options{Workers: 1, Outstanding: 4})
		require.NoError(t, p.Write(context.Background(), oneWrite(f, 1, 0), nil))
		<-started

		closed := make(chan error, 1)
		go func() { closed <- p.Close() }()
		synctest.Wait()

		select {
		case err := <-closed:
			t.Fatalf("Close returned %v while a batch was in flight", err)
		default:
		}
		release()
		require.NoError(t, <-closed)
	})
}

func TestCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "idem.log")

	p := newPool([]*os.File{f}, Options{Workers: 1})
	require.NoError(t, p.Close())
	require.NoError(t, p.Close())

	var fired error
	err := p.Write(context.Background(), oneWrite(f, 1, 0), func(err error) { fired = err })
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, fired, ErrClosed)
}

func TestDataSyncBoundary(t *testing.T) {
	dir := t.TempDir()

	t.Run("buffered never syncs", func(t *testing.T) {
		synced := syncCounter(t)
		f := testFile(t, dir, "buffered.log")
		p := newPool([]*os.File{f}, Options{Workers: 1, Durability: Buffered})
		require.NoError(t, <-awaitDone(p, oneWrite(f, 1, 0)))
		require.Empty(t, synced())
		require.NoError(t, p.Close())
	})

	t.Run("datasync fsyncs every file in the batch", func(t *testing.T) {
		synced := syncCounter(t)
		a, b := testFile(t, dir, "ds-a.log"), testFile(t, dir, "ds-b.log")
		p := newPool([]*os.File{a, b}, Options{Workers: 1, Durability: DataSync})
		batch := []Write{{F: a, Buf: []byte{1}, Off: 0}, {F: b, Buf: []byte{2}, Off: 0}, {F: a, Buf: []byte{3}, Off: 8}}
		require.NoError(t, <-awaitDone(p, batch))
		require.Equal(t, map[*os.File]int{a: 1, b: 1}, synced())
		require.NoError(t, p.Close())
	})

	t.Run("syncfiles restricts the set", func(t *testing.T) {
		synced := syncCounter(t)
		a, b := testFile(t, dir, "sf-a.log"), testFile(t, dir, "sf-b.log")
		p := newPool([]*os.File{a, b}, Options{Workers: 1, Durability: DataSync, SyncFiles: []*os.File{a}})
		require.NoError(t, <-awaitDone(p, []Write{{F: a, Buf: []byte{1}, Off: 0}, {F: b, Buf: []byte{2}, Off: 0}}))
		require.Equal(t, map[*os.File]int{a: 1}, synced())
		require.NoError(t, p.Close())
	})

	t.Run("sync fsyncs every owned file", func(t *testing.T) {
		synced := syncCounter(t)
		a, b := testFile(t, dir, "s-a.log"), testFile(t, dir, "s-b.log")
		p := newPool([]*os.File{a, b}, Options{Workers: 1, Durability: DataSync, SyncFiles: []*os.File{a}})
		require.NoError(t, p.Sync())
		require.Equal(t, map[*os.File]int{a: 1, b: 1}, synced())
		require.NoError(t, p.Close())
	})
}

// syncCounter counts fsync calls per file and returns a snapshot function.
func syncCounter(t *testing.T) func() map[*os.File]int {
	t.Helper()
	var mu sync.Mutex
	counts := map[*os.File]int{}
	stubSyncData(t, func(f *os.File) error {
		mu.Lock()
		counts[f]++
		mu.Unlock()
		return nil
	})
	return func() map[*os.File]int {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[*os.File]int, len(counts))
		maps.Copy(out, counts)
		return out
	}
}

func TestSyncFailsAfterBatchError(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "syncfail.log")

	errBoom := errors.New("boom")
	stubWriteAt(t, func(_ *os.File, _ []byte, _ int64) (int, error) { return 0, errBoom })

	p := newPool([]*os.File{f}, Options{Workers: 1, Durability: DataSync})
	require.ErrorIs(t, <-awaitDone(p, oneWrite(f, 1, 0)), errBoom)
	require.ErrorIs(t, p.Sync(), errBoom)
	require.ErrorIs(t, p.Close(), errBoom)
}

func TestSyncWaitsForInFlightWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		f := testFile(t, dir, "outstanding.log")

		started := make(chan struct{})
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		stubWriteAt(t, func(_ *os.File, _ []byte, _ int64) (int, error) {
			close(started)
			<-gate
			return 8, nil
		})

		w := newPool(nil, Options{Workers: 1, Durability: DataSync})
		t.Cleanup(func() { _ = w.Close() })

		require.NoError(t, w.Write(context.Background(), oneWrite(f, 1, 0), nil))
		<-started

		synced := make(chan error, 1)
		go func() { synced <- w.Sync() }()
		synctest.Wait()

		select {
		case err := <-synced:
			t.Fatalf("Sync returned %v while a write was in flight", err)
		default:
		}
		release()
		require.NoError(t, <-synced)
	})
}

func TestDoneMaySubmitAnotherBatch(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "reenter.log")

	p := newPool([]*os.File{f}, Options{Workers: 1, Outstanding: 4})
	t.Cleanup(func() { _ = p.Close() })

	nested := make(chan error, 1)
	inner := make(chan error, 1)
	require.NoError(t, p.Write(context.Background(), oneWrite(f, 1, 0), func(error) {
		nested <- p.Write(context.Background(), oneWrite(f, 2, 8), func(err error) { inner <- err })
	}))
	require.NoError(t, <-nested)
	require.NoError(t, <-inner)

	require.NoError(t, p.Sync())
	data, err := os.ReadFile(filepath.Join(dir, "reenter.log"))
	require.NoError(t, err)
	require.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}, data)
}
