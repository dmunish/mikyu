package engine

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
)

// recordIDGen mints process-scoped 128-bit record IDs: an 8-byte random
// prefix from crypto/rand plus a monotonic counter. Unique across restarts
// (distinct prefixes), sortable, and free of per-record entropy cost.
type recordIDGen struct {
	prefix [8]byte
	seq    atomic.Uint64
}

var idGen recordIDGen

func init() {
	if _, err := rand.Read(idGen.prefix[:]); err != nil {
		panic("engine: crypto/rand unavailable: " + err.Error())
	}
}

// newRecordID returns the next process-scoped record ID.
func newRecordID() [16]byte {
	var id [16]byte
	copy(id[:8], idGen.prefix[:])
	binary.BigEndian.PutUint64(id[8:], idGen.seq.Add(1))
	return id
}

// formatRecordID renders 16 raw ID bytes in the canonical 8-4-4-4-12 hex layout.
func formatRecordID(id [16]byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], id[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], id[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], id[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], id[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], id[10:16])
	return string(buf[:])
}
