package engine

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSinkWritesCarryLogAndIndex(t *testing.T) {
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	defer log.Close()
	idx, err := os.Create(filepath.Join(dir, "a.idx"))
	require.NoError(t, err)
	defer idx.Close()

	s := &sink{log: log, idx: idx}
	payload := []byte("payload-0123456789")
	batch := sinkBatch{
		Records: []sinkWrite{{P: payload, Off: 128}},
		Index:   sinkIndex{Start: 16, Positions: []int64{128}, Lengths: []int64{int64(len(payload))}},
	}

	writes, index := s.writes(batch)
	require.NotNil(t, index)
	defer index.release(maxIndexKeepBytes)

	require.Len(t, writes, 2)
	require.Equal(t, log, writes[0].F)
	require.Equal(t, int64(128), writes[0].Off)
	require.Equal(t, payload, writes[0].Buf)
	require.Equal(t, idx, writes[1].F)
	require.Equal(t, int64(16), writes[1].Off)
	require.Equal(t, rawIndexEntrySize, len(writes[1].Buf))
	require.Equal(t, uint64(128), binary.BigEndian.Uint64(writes[1].Buf[0:8]))
	require.Equal(t, uint64(len(payload)), binary.BigEndian.Uint64(writes[1].Buf[8:16]))
}

func TestSinkWritesOmitEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "a.log"))
	require.NoError(t, err)
	defer log.Close()

	s := &sink{log: log}
	writes, index := s.writes(sinkBatch{Records: []sinkWrite{{P: []byte("x"), Off: 0}}})
	assert.Nil(t, index)
	require.Len(t, writes, 1)
	assert.Equal(t, log, writes[0].F)
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
