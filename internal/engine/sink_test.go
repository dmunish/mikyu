package engine

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSinkSyncForcesDurability(t *testing.T) {
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	defer log.Close()

	s := &syncSink{log: log}
	err = s.Write(sinkBatch{Records: []sinkWrite{{P: []byte("durable bytes"), Off: 0}}}, nil)
	require.NoError(t, err)

	read, err := os.ReadFile(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	assert.Equal(t, []byte("durable bytes"), read)
}

func TestSinkAsyncMatchesSync(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("payload-0123456789")
	index := sinkIndex{Start: 0, Positions: []int64{0}, Lengths: []int64{int64(len(payload))}}

	syncLog, err := os.Create(filepath.Join(dir, "sync.log"))
	require.NoError(t, err)
	defer syncLog.Close()
	syncIdx, err := os.Create(filepath.Join(dir, "sync.idx"))
	require.NoError(t, err)
	defer syncIdx.Close()
	s := &syncSink{log: syncLog, idx: syncIdx}
	require.NoError(t, s.Write(sinkBatch{Records: []sinkWrite{{P: payload, Off: 0}}, Index: index}, nil))

	asyncLog, err := os.Create(filepath.Join(dir, "async.log"))
	require.NoError(t, err)
	defer asyncLog.Close()
	asyncIdx, err := os.Create(filepath.Join(dir, "async.idx"))
	require.NoError(t, err)
	defer asyncIdx.Close()
	a := newAsyncSink(asyncLog, asyncIdx, 4)
	completed := make(chan error, 1)
	require.NoError(t, a.Write(sinkBatch{Records: []sinkWrite{{P: payload, Off: 0}}, Index: index}, func(err error) {
		completed <- err
	}))
	require.NoError(t, a.Close())
	require.NoError(t, <-completed)

	gotSync, err := os.ReadFile(filepath.Join(dir, "sync.log"))
	require.NoError(t, err)
	gotAsync, err := os.ReadFile(filepath.Join(dir, "async.log"))
	require.NoError(t, err)
	assert.Equal(t, gotSync, gotAsync)
}

func TestSinkCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	defer log.Close()

	s := newAsyncSink(log, nil, 4)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())

	require.ErrorIs(t, s.Write(sinkBatch{}, nil), ErrQueueClosed)
}

func TestSinkIndexWriteIsBatched(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegment(dir, 0, 0)
	require.NoError(t, err)
	defer seg.Close()

	var writes []int
	indexWriteHook = func(off int64, n int) {
		writes = append(writes, n)
	}
	t.Cleanup(func() { indexWriteHook = nil })

	const count = 1000
	positions := make([]int64, count)
	lengths := make([]int64, count)
	for i := range count {
		positions[i] = int64(i) * 8
		lengths[i] = 8
	}
	require.NoError(t, writeIndexBatch(seg.idxFile, sinkIndex{Start: 0, Positions: positions, Lengths: lengths}))

	require.Len(t, writes, 1, "1000 index entries must land in one write")
	assert.Equal(t, count*rawIndexEntrySize, writes[0])
}

func TestSinkAsyncDrainsOnClose(t *testing.T) {
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	defer log.Close()

	s := newAsyncSink(log, nil, 64)
	var completed atomic.Int64
	for i := range 100 {
		payload := []byte{byte(i)}
		require.NoError(t, s.Write(sinkBatch{Records: []sinkWrite{{P: payload, Off: int64(i)}}}, func(err error) {
			require.NoError(t, err)
			completed.Add(1)
		}))
	}
	require.NoError(t, s.Close())
	assert.Equal(t, int64(100), completed.Load(), "Close must drain every queued batch and fire its done callback")
}

func TestNewSinkDurableWithoutRing(t *testing.T) {
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	defer log.Close()
	idx, err := os.Create(filepath.Join(dir, "a.idx"))
	require.NoError(t, err)
	defer idx.Close()

	s := newSink(log, idx, testConfig(), nil)
	_, ok := s.(*syncSink)
	assert.True(t, ok, "a durable sink without a ring must be the portable sync sink")
}
