package iow

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestNewFallsBackToPool(t *testing.T) {
	w, err := New(nil, Options{Workers: 1})
	require.NoError(t, err)
	require.IsType(t, &pool{}, w)
	require.NoError(t, w.Close())
}

func TestNewWritesEveryBufferOfABatch(t *testing.T) {
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

func TestWriteAfterCloseReportsErrClosed(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "closed.log")

	w, err := New([]*os.File{f}, Options{Workers: 1})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var fired error
	require.ErrorIs(t, w.Write(context.Background(), oneWrite(f, 1, 0), func(err error) { fired = err }), ErrClosed)
	require.ErrorIs(t, fired, ErrClosed)
}

func TestSyncWaitsForOutstandingWrites(t *testing.T) {
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

		w, err := New([]*os.File{f}, Options{Workers: 1, Durability: DataSync})
		require.NoError(t, err)
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

func TestCancelledContextRejectsBatch(t *testing.T) {
	dir := t.TempDir()
	f := testFile(t, dir, "cancelled.log")
	stubWriteAt(t, func(*os.File, []byte, int64) (int, error) {
		t.Error("a cancelled batch reached the write primitive")
		return 0, context.Canceled
	})

	w, err := New([]*os.File{f}, Options{Workers: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var fired error
	require.ErrorIs(t, w.Write(ctx, oneWrite(f, 1, 0), func(err error) { fired = err }), context.Canceled)
	require.ErrorIs(t, fired, context.Canceled)
}
