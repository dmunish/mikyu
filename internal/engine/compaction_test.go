package engine

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCompactionQueue opens a queue with compaction enabled and small segments.
func newCompactionQueue(t *testing.T, dir string, maxSegBytes int64) *Queue {
	t.Helper()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	cfg := Config{
		BatchSize:       16,
		MaxInterval:     10 * time.Millisecond,
		SegmentSize:     maxSegBytes,
		MaxMessageBytes: 1024 * 1024,
		Fsync:           false,
		Compaction:      true,
	}
	q, err := openQueue(dir, "compaction-queue", cfg, store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// appendVersioned appends three versions of one key; with a 256B segment cap
// and 100B payloads each record lands in its own segment, forcing two rolls.
func appendVersioned(t *testing.T, q *Queue, key []byte) []uint64 {
	t.Helper()
	ctx := t.Context()
	offs := make([]uint64, 3)
	for i := range 3 {
		off, err := q.Append(ctx, bytes.Repeat([]byte{byte('v' + byte(i))}, 100), WithGroup(key))
		require.NoError(t, err)
		offs[i] = off
	}
	return offs
}

func TestCompactionKeepsLatestPerKey(t *testing.T) {
	q := newCompactionQueue(t, t.TempDir(), 256)
	offs := appendVersioned(t, q, []byte("user-42"))
	ctx := t.Context()

	// Only the newest version survives: older ones are compacted, the newest is
	// readable and carries the group.
	_, err := q.Read(ctx, offs[0])
	require.ErrorIs(t, err, ErrRecordCompacted)
	_, err = q.Read(ctx, offs[1])
	require.ErrorIs(t, err, ErrRecordCompacted)
	rec, err := q.Read(ctx, offs[2])
	require.NoError(t, err)
	assert.Equal(t, offs[2], rec.Offset)
	assert.Equal(t, []byte("user-42"), rec.Group)

	// The oldest version was physically dropped from its sealed segment: its
	// index slot is zeroed.
	_, _, err = q.segments[0].readIndexEntry(offs[0])
	require.Error(t, err)
}

func TestCompactionOnlyClosedSegments(t *testing.T) {
	q := newCompactionQueue(t, t.TempDir(), 1<<20)
	ctx := t.Context()
	key := []byte("user-42")
	for range 3 {
		off, err := q.Append(ctx, []byte("payload"), WithGroup(key))
		require.NoError(t, err)
		rec, err := q.Read(ctx, off)
		require.NoError(t, err)
		assert.Equal(t, []byte("payload"), rec.Payload)
	}
	// No roll happened, so nothing was compacted; the key index tracks the latest.
	require.Equal(t, uint64(2), q.keyIndex[string(key)])
}

func TestCompactionDisabledKeepsAllVersions(t *testing.T) {
	q, _ := newTestQueue(t, 1<<20)
	ctx := t.Context()
	off, err := q.Append(ctx, []byte("payload"), WithGroup([]byte("k")))
	require.NoError(t, err)
	rec, err := q.Read(ctx, off)
	require.NoError(t, err)
	assert.Equal(t, []byte("k"), rec.Group)
	require.Nil(t, q.keyIndex)
}

func TestReadCompactedOffsetReturnsErrRecordCompacted(t *testing.T) {
	q := newCompactionQueue(t, t.TempDir(), 256)
	offs := appendVersioned(t, q, []byte("user-42"))
	ctx := t.Context()

	// A compacted offset is distinct from a truncated one.
	_, err := q.Read(ctx, offs[0])
	require.ErrorIs(t, err, ErrRecordCompacted)
	require.False(t, errors.Is(err, ErrOffsetTruncated))

	// A genuinely truncated offset still reports ErrOffsetTruncated.
	_, err = q.TruncateBefore(ctx, offs[2])
	require.NoError(t, err)
	_, err = q.Read(ctx, offs[1])
	require.ErrorIs(t, err, ErrOffsetTruncated)
}

func TestStreamSkipsCompactedOffsets(t *testing.T) {
	q := newCompactionQueue(t, t.TempDir(), 256)
	offs := appendVersioned(t, q, []byte("user-42"))
	ctx := t.Context()

	// ReadBatch skips compacted offsets within the window.
	msgs, err := q.ReadBatch(ctx, 0, 100)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, offs[2], msgs[0].Offset)

	// Fetch advances past a fully-compacted window instead of stalling.
	msgs, err = q.Fetch(ctx, 0, 2, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, offs[2], msgs[0].Offset)
}

func TestGroupOffsetsSurviveCompaction(t *testing.T) {
	q := newCompactionQueue(t, t.TempDir(), 256)
	offs := appendVersioned(t, q, []byte("user-42"))
	ctx := t.Context()

	// Consumer offsets are never renumbered by compaction.
	require.NoError(t, q.CommitOffset(ctx, "g", offs[2]))
	committed, err := q.Committed(ctx, "g")
	require.NoError(t, err)
	assert.Equal(t, offs[2], committed)
	rec, err := q.Read(ctx, committed)
	require.NoError(t, err)
	assert.Equal(t, offs[2], rec.Offset)
}

func TestKeyIndexRebuildsOnOpen(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	cfg := Config{
		BatchSize:       16,
		MaxInterval:     10 * time.Millisecond,
		SegmentSize:     256,
		MaxMessageBytes: 1024 * 1024,
		Fsync:           false,
		Compaction:      true,
	}
	q, err := openQueue(dir, "rebuild-queue", cfg, store)
	require.NoError(t, err)
	key := []byte("user-42")
	offs := appendVersioned(t, q, key)
	require.NoError(t, q.Close())

	// Reopen the same directory: the key index is rebuilt from disk.
	q2, err := openQueue(dir, "rebuild-queue", cfg, store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q2.Close() })
	ctx := t.Context()
	require.Equal(t, offs[2], q2.keyIndex[string(key)])

	// Supersession state survived the restart.
	_, err = q2.Read(ctx, offs[0])
	require.ErrorIs(t, err, ErrRecordCompacted)
	rec, err := q2.Read(ctx, offs[2])
	require.NoError(t, err)
	assert.Equal(t, offs[2], rec.Offset)

	// Compaction still works after the reopen: a new version supersedes the rest.
	off3, err := q2.Append(ctx, []byte("newer"), WithGroup(key))
	require.NoError(t, err)
	_, err = q2.Read(ctx, off3)
	require.NoError(t, err)
	_, err = q2.Read(ctx, offs[2])
	require.ErrorIs(t, err, ErrRecordCompacted)
}

func TestKeyIndexSizeWarning(t *testing.T) {
	oldLimit := keyIndexWarnLimit
	keyIndexWarnLimit = 2
	t.Cleanup(func() { keyIndexWarnLimit = oldLimit })

	var buf bytes.Buffer
	oldOut := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(oldOut) })

	q := newCompactionQueue(t, t.TempDir(), 256)
	ctx := t.Context()
	for i := range 3 {
		key := fmt.Appendf(nil, "key-%d", i)
		_, err := q.Append(ctx, []byte("payload"), WithGroup(key))
		require.NoError(t, err)
	}
	require.Equal(t, 3, len(q.keyIndex))
	assert.Contains(t, buf.String(), "key index holds 3 groups")
}
