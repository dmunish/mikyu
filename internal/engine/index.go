package engine

import "encoding/binary"

const rawIndexEntrySize = 16

// indexEntry represents an O(1) in-memory lookup entry mapping a logical offset
// to a segment reference and its exact byte offset and length on disk.
type indexEntry struct {
	seg *segment
	pos int64
	len int64
}

// rawIndexEntry is the binary format persisted in segment .idx files.
type rawIndexEntry struct {
	pos int64
	len int64
}

func (e rawIndexEntry) MarshalBinary(buf *[rawIndexEntrySize]byte) {
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.pos))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.len))
}

func UnmarshalRawIndexEntry(buf []byte) rawIndexEntry {
	return rawIndexEntry{
		pos: int64(binary.BigEndian.Uint64(buf[0:8])),
		len: int64(binary.BigEndian.Uint64(buf[8:16])),
	}
}
