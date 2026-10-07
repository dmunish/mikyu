package engine

import (
	"os"

	"github.com/dmunish/mikyu/internal/iow"
)

// sinkBatch is one producer batch: the log records to append, plus any index entries that must become durable with them.
type sinkBatch struct {
	Records []sinkWrite
	Index   sinkIndex
}

// sinkWrite is one record's bytes and their offset in the log file.
type sinkWrite struct {
	P   []byte
	Off int64
}

// sinkIndex describes the index slots to write for a batch; empty Positions means the caller keeps index writes to itself.
type sinkIndex struct {
	Start     int64
	Positions []int64
	Lengths   []int64
}

func (s sinkIndex) empty() bool { return len(s.Positions) == 0 }

// sinkDone runs once per batch, on any goroutine; a non-nil error means the batch failed.
type sinkDone func(err error)

// sink renders a segment's batches into the flat write list an iow.Writer consumes.
type sink struct {
	log *os.File
	idx *os.File
}

// writes renders one batch as writes for the log and, when present, the index; the returned buffer holds the encoded index entries and must be released once the batch completes.
func (s *sink) writes(b sinkBatch) ([]iow.Write, *pooledBuf) {
	out := make([]iow.Write, 0, len(b.Records)+1)
	for _, r := range b.Records {
		if writeAtHook != nil {
			writeAtHook(r.Off, len(r.P))
		}
		out = append(out, iow.Write{F: s.log, Buf: r.P, Off: r.Off})
	}
	var index *pooledBuf
	if !b.Index.empty() {
		index = encodeIndexBatch(b.Index)
		out = append(out, iow.Write{F: s.idx, Buf: index.b, Off: b.Index.Start})
	}
	return out, index
}

// writerHook, when non-nil, runs before every submitted batch so tests can hold or count submissions.
var writerHook func()

// writeAtHook, when non-nil, receives every submitted log write's offset and length so tests can assert batching.
var writeAtHook func(off int64, n int)

// encodeIndexInto packs consecutive index entries into dst.
func encodeIndexInto(dst []byte, s sinkIndex) {
	for i := range s.Positions {
		var ent [rawIndexEntrySize]byte
		rawIndexEntry{pos: s.Positions[i], len: s.Lengths[i]}.MarshalBinary(&ent)
		copy(dst[i*rawIndexEntrySize:], ent[:])
	}
	if indexWriteHook != nil {
		indexWriteHook(s.Start, len(s.Positions)*rawIndexEntrySize)
	}
}

// encodeIndexBatch packs a batch's index entries into a pooled buffer.
func encodeIndexBatch(s sinkIndex) *pooledBuf {
	buf := getBuf(len(s.Positions) * rawIndexEntrySize)
	encodeIndexInto(buf.b, s)
	return buf
}
