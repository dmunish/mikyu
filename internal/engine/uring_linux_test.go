//go:build linux

package engine

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustRing returns a live ring, skipping hosts where io_uring is unavailable.
func mustRing(t *testing.T) *uringRing {
	t.Helper()
	r, err := newUringRing()
	if err != nil {
		t.Skipf("io_uring unavailable on this host: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func uringTempFile(t *testing.T, name string) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), name))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestUringWriteThenSync pins the durable ring sink: the bytes and their
// index entries are readable when Write returns, all under one tag.
func TestUringWriteThenSync(t *testing.T) {
	r := mustRing(t)
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "seg.log"))
	require.NoError(t, err)
	defer log.Close()
	idx, err := os.Create(filepath.Join(dir, "seg.idx"))
	require.NoError(t, err)
	defer idx.Close()

	s := newRingSink(log, idx, true, r)
	payload := []byte("durable-through-the-ring")
	batch := sinkBatch{
		Records: []sinkWrite{{P: payload, Off: 0}},
		Index:   sinkIndex{Start: 0, Positions: []int64{0}, Lengths: []int64{int64(len(payload))}},
	}
	require.NoError(t, s.Write(batch, nil))

	got, err := os.ReadFile(filepath.Join(dir, "seg.log"))
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	idxBuf := make([]byte, rawIndexEntrySize)
	_, err = idx.ReadAt(idxBuf, 0)
	require.NoError(t, err)
	entry := UnmarshalRawIndexEntry(idxBuf)
	assert.Equal(t, int64(0), entry.pos)
	assert.Equal(t, int64(len(payload)), entry.len)
}

// TestUringParallelWrites drives concurrent sinks through one ring and verifies every batch lands byte for byte.
func TestUringParallelWrites(t *testing.T) {
	r := mustRing(t)
	dir := t.TempDir()

	const writers = 64
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}

	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			name := filepath.Join(dir, "w-"+strconv.Itoa(g)+".log")
			log, err := os.Create(name)
			require.NoError(t, err)
			defer log.Close()
			s := newRingSink(log, nil, true, r)
			for i := range 8 {
				require.NoError(t, s.Write(sinkBatch{
					Records: []sinkWrite{{P: payload, Off: int64(i) * int64(len(payload))}},
				}, nil))
			}
			got, err := os.ReadFile(name)
			require.NoError(t, err)
			require.Len(t, got, 8*len(payload))
			for i := range 8 {
				assert.Equal(t, payload, got[i*len(payload):(i+1)*len(payload)])
			}
		}(g)
	}
	wg.Wait()
}

// TestUringShortWriteIsError submits against a read-only fd and verifies the
// errno surfaces and the tag leaves no pending entry behind.
func TestUringShortWriteIsError(t *testing.T) {
	r := mustRing(t)
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "ro.bin"), os.O_RDONLY|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer f.Close()

	h := r.Register()
	defer func() { _ = h.Close() }()

	tag := h.NextTag()
	ops := []uringOp{{Kind: uringOpWrite, File: f, Buf: []byte("x"), Off: 0, Tag: tag}}
	require.NoError(t, h.Submit(ops, nil))
	require.Error(t, h.Wait(tag))

	h.mu.Lock()
	_, pending := h.pend[tag]
	h.mu.Unlock()
	assert.False(t, pending, "a completed tag must not linger in the pending table")
}

// TestUringCancellationOnClose verifies waiters parked on outstanding tags wake with ErrQueueClosed when the handle closes.
func TestUringCancellationOnClose(t *testing.T) {
	r := mustRing(t)
	log := uringTempFile(t, "cancel.log")

	h := r.Register()
	tag := h.NextTag()
	ops := []uringOp{
		{Kind: uringOpWrite, File: log, Buf: []byte("payload"), Off: 0, Tag: tag},
		{Kind: uringOpFsync, File: log, DataSync: true, Tag: tag},
	}
	require.NoError(t, h.Submit(ops, nil))

	done := make(chan error, 1)
	go func() { done <- h.Wait(tag) }()

	require.NoError(t, h.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrQueueClosed)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Wait did not wake on handle close")
	}
}

// TestUringReaperDoesNotLeak verifies every registered slot returns to the free list.
func TestUringReaperDoesNotLeak(t *testing.T) {
	r := mustRing(t)
	for range 100 {
		h := r.Register()
		require.NoError(t, h.Close())
	}
	r.mu.Lock()
	live := 0
	for _, h := range r.handles {
		if h != nil {
			live++
		}
	}
	free := len(r.free)
	r.mu.Unlock()
	assert.Zero(t, live, "every closed handle must leave the table")
	assert.Equal(t, 100, free, "every closed handle must return its id to the free list")
}

// TestUringHandleUnregister verifies submissions from a closed handle are refused and completions for it are dropped without panicking.
func TestUringHandleUnregister(t *testing.T) {
	r := mustRing(t)
	log := uringTempFile(t, "stale.log")

	stale := r.Register()
	require.NoError(t, stale.Close())

	tag := stale.NextTag()
	err := stale.Submit([]uringOp{{Kind: uringOpWrite, File: log, Buf: []byte("late"), Off: 0, Tag: tag}}, nil)
	require.ErrorIs(t, err, ErrQueueClosed)

	live := newRingSink(log, nil, true, r)
	defer func() { _ = live.Close() }()
	require.NoError(t, live.Write(sinkBatch{Records: []sinkWrite{{P: []byte("ok"), Off: 0}}}, nil))
	got, err := os.ReadFile(log.Name())
	require.NoError(t, err)
	assert.Equal(t, []byte("ok"), got)
}

// TestUringSubmitRollbackOnError forces a submission failure by closing the
// ring fd and verifies the tag leaves the pending table so Wait cannot hang.
func TestUringSubmitRollbackOnError(t *testing.T) {
	r := mustRing(t)
	h := r.Register()
	t.Cleanup(func() { _ = h.Close() })

	require.NoError(t, unix.Close(r.fd))

	tag := h.NextTag()
	ops := []uringOp{{Kind: uringOpWrite, Buf: []byte("x"), Off: 0, Tag: tag}}
	require.Error(t, h.Submit(ops, nil))

	h.mu.Lock()
	_, pending := h.pend[tag]
	h.mu.Unlock()
	assert.False(t, pending, "a failed submission must roll its tag back")

	done := make(chan error, 1)
	go func() { done <- h.Wait(tag) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Wait blocked on a rolled-back tag")
	}
}
