package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

func TestPayloadSpansMatchReadBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.MaxSegment = 256 << 10
	q, err := openQueue(dir, "spans", cfg, store, nil)
	require.NoError(t, err)
	defer q.Close()

	payloads := make([][]byte, 24)
	for i := range payloads {
		switch i % 3 {
		case 0:
			payloads[i] = make([]byte, 64<<10)
		case 1:
			payloads[i] = []byte("small")
		default:
			payloads[i] = make([]byte, 80<<10)
		}
		for j := range payloads[i] {
			payloads[i][j] = byte(i*7 + j)
		}
	}
	offsets, err := q.AppendBatch(t.Context(), payloads)
	require.NoError(t, err)
	require.Len(t, offsets, len(payloads))

	spans, err := q.PayloadSpans(0, len(payloads))
	require.NoError(t, err)
	require.Len(t, spans, len(payloads))

	recs, err := q.ReadBatch(t.Context(), 0, len(payloads))
	require.NoError(t, err)
	require.Len(t, recs, len(payloads))

	fileBacked := 0
	for i := range spans {
		assert.Equal(t, recs[i].Offset, spans[i].Offset)
		assert.Equal(t, recs[i].ID, spans[i].ID)
		assert.True(t, spans[i].Timestamp.Equal(recs[i].Timestamp))
		assert.Equal(t, recs[i].Group, spans[i].Group)
		assert.Equal(t, len(recs[i].Payload), spans[i].PayloadLen)
		if spans[i].File == nil {
			assert.Equal(t, recs[i].Payload, spans[i].Payload)
			continue
		}
		fileBacked++
		onDisk := make([]byte, spans[i].PayloadLen)
		n, err := spans[i].File.ReadAt(onDisk, spans[i].FileOff)
		require.NoError(t, err)
		require.Equal(t, len(onDisk), n)
		assert.Equal(t, recs[i].Payload, onDisk)
	}
	assert.Positive(t, fileBacked)
}

func TestPayloadSpansDeclineOnCompaction(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.Compaction = spec.CompactionKeys
	q, err := openQueue(dir, "spans-compact", cfg, store, nil)
	require.NoError(t, err)
	defer q.Close()

	_, err = q.AppendBatch(t.Context(), [][]byte{make([]byte, 64<<10), make([]byte, 64<<10)})
	require.NoError(t, err)

	spans, err := q.PayloadSpans(0, 2)
	require.NoError(t, err)
	assert.Nil(t, spans)
}

func TestPayloadSpansDeclineWhenUnflushed(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushBytes = 1 << 30
	cfg.MaxFlushWait = time.Hour
	q, err := openQueue(dir, "spans-unflushed", cfg, store, nil)
	require.NoError(t, err)
	defer q.Close()

	_, err = q.AppendBatch(t.Context(), [][]byte{make([]byte, 64<<10)})
	require.NoError(t, err)

	spans, err := q.PayloadSpans(0, 1)
	require.NoError(t, err)
	assert.Nil(t, spans)
}

func TestPayloadSpansDeclineBelowThreshold(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "spans-small", testConfig(), store, nil)
	require.NoError(t, err)
	defer q.Close()

	_, err = q.AppendBatch(t.Context(), [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)

	spans, err := q.PayloadSpans(0, 2)
	require.NoError(t, err)
	assert.Nil(t, spans)
}
