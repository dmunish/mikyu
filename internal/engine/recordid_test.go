package engine

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordIDIsMonotonic pins the counter half: successive IDs strictly increase.
func TestRecordIDIsMonotonic(t *testing.T) {
	prev := newRecordID()
	for range 1000 {
		next := newRecordID()
		require.Greater(t, binary.BigEndian.Uint64(next[8:]), binary.BigEndian.Uint64(prev[8:]))
		prev = next
	}
}

// TestRecordIDPrefixIsProcessStable checks that every ID minted in this process shares the random prefix.
func TestRecordIDPrefixIsProcessStable(t *testing.T) {
	first := newRecordID()
	for range 100 {
		next := newRecordID()
		assert.Equal(t, first[:8], next[:8], "IDs minted in one process share the prefix")
	}
}

// TestRecordIDRoundTripsThroughDisk encodes a record with a known ID and
// requires decodeRecord to return that ID in the canonical string layout.
func TestRecordIDRoundTripsThroughDisk(t *testing.T) {
	id := newRecordID()
	ts := time.Now().UTC().Truncate(time.Microsecond)
	rec := encodeRecord(nil, id, ts, []byte("grp"), []byte("payload"))
	decodedID, decodedTS, group, payload, err := decodeRecord(rec[recordLengthSize:])
	require.NoError(t, err)
	assert.Equal(t, formatRecordID(id), decodedID)
	assert.WithinDuration(t, ts, decodedTS, time.Microsecond)
	assert.Equal(t, []byte("grp"), group)
	assert.Equal(t, []byte("payload"), payload)
}
