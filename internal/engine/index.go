package engine

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
