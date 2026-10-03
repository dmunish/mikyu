package engine

import (
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentWriteAndRecover(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegment(dir, 0, minSegmentPrealloc)
	require.NoError(t, err)
	defer seg.Close()

	rec1 := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("first"), nil)
	rec2 := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("second"), nil)
	seg.pos = 0
	_, err = seg.logFile.WriteAt(rec1, 0)
	require.NoError(t, err)
	_, err = seg.logFile.WriteAt(rec2, int64(len(rec1)))
	require.NoError(t, err)
	seg.pos = int64(len(rec1) + len(rec2))
	_ = seg.writeIndex(0, int64(len(rec1)))
	_ = seg.writeIndex(int64(len(rec1)), int64(len(rec2)))

	entries, err := seg.recover()
	require.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Equal(t, int64(0), entries[0].pos)
	assert.Equal(t, int64(len(rec1)), entries[1].pos)
	assert.Equal(t, int64(len(rec2)), entries[1].len)
}

func TestSegmentRebuildOnCorruptIndex(t *testing.T) {
	dir := t.TempDir()
	seg, _ := openSegment(dir, 0, minSegmentPrealloc)
	defer seg.Close()

	rec := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("payload"), nil)
	_, _ = seg.logFile.WriteAt(rec, 0)
	seg.pos = int64(len(rec))
	_, _ = seg.idxFile.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04})

	entries, err := seg.recover()
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, int64(0), entries[0].pos)
	assert.Equal(t, int64(len(rec)), entries[0].len)
}

func TestSegmentTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	seg, _ := openSegment(dir, 0, minSegmentPrealloc)
	defer seg.Close()

	rec := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("payload"), nil)
	// Write full record, then a partial second record
	_, _ = seg.logFile.WriteAt(rec, 0)
	torn := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("partial"), nil)
	_, _ = seg.logFile.WriteAt(torn[:len(torn)-2], int64(len(rec))) // truncated write

	entries, err := seg.recover()
	require.NoError(t, err)
	assert.Len(t, entries, 1) // only first record recovered
}

func TestSegmentMMap(t *testing.T) {
	dir := t.TempDir()
	seg, _ := openSegment(dir, 0, minSegmentPrealloc)
	defer seg.Close()
	rec := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("data"), nil)
	_, _ = seg.logFile.WriteAt(rec, 0)
	seg.pos = int64(len(rec))

	err := seg.enableMMap()
	require.NoError(t, err)
	assert.NotNil(t, seg.mmapData)
	assert.Equal(t, rec, []byte(seg.mmapData[:len(rec)]))
}

func TestSegmentCloseUnmapsMMap(t *testing.T) {
	dir := t.TempDir()
	seg, _ := openSegment(dir, 0, minSegmentPrealloc)
	rec := encodeRecord(nil, uuid.NewV7(), time.Now(), []byte("data"), nil)
	_, _ = seg.logFile.WriteAt(rec, 0)
	seg.pos = int64(len(rec))
	_ = seg.enableMMap()

	err := seg.Close()
	require.NoError(t, err)
	assert.Nil(t, seg.mmapData)
}
