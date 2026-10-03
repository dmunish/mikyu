package engine

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (q *Queue) readFromSegment(seg *segment, offset uint64) (Record, error) {
	pos, recLen, err := seg.readIndexEntry(offset)
	if err != nil || recLen < (minRecordSize-recordLengthSize) {
		// A zeroed slot in a compactable queue means compaction dropped the record.
		if q.cfg.KeyedCompaction() {
			return Record{}, ErrRecordCompacted
		}
		return Record{}, ErrOffsetNotFound
	}

	var buf []byte
	if len(seg.mmapData) >= int(pos+recLen) {
		buf = seg.mmapData[pos : pos+recLen]
	} else {
		buf = make([]byte, recLen)
		if _, err := seg.logFile.ReadAt(buf, pos); err != nil {
			return Record{}, err
		}
	}

	id, ts, group, payload, err := decodeRecord(buf[recordLengthSize:])
	if err != nil {
		return Record{}, fmt.Errorf("%w: offset %d: %v", ErrCorruptRecord, offset, err)
	}
	return Record{Offset: offset, ID: id, Timestamp: ts, Group: group, Payload: payload}, nil
}

// Read implements Storage.Read.
func (q *Queue) Read(ctx context.Context, offset uint64) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	q.hasReader.Store(true)
	start := time.Now()

	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return Record{}, ErrQueueClosed
	}

	if offset < q.MinOffset() {
		return Record{}, ErrOffsetTruncated
	}

	// 1. Fast Path: Check in-memory ring cache first (~10 ns)
	if msg, ok := q.cache.Get(offset); ok {
		q.m.cacheHits.Inc()
		if q.superseded(msg.Group, offset) {
			return Record{}, ErrRecordCompacted
		}
		q.recordRead(1, int64(len(msg.Payload)), start)
		return msg, nil
	}
	q.m.cacheMisses.Inc()

	// 2. Disk / MMap Path: Fall back to historical log segments
	seg, err := q.locateSegment(offset)
	if err != nil {
		return Record{}, err
	}

	msg, err := q.readFromSegment(seg, offset)
	if err != nil {
		return Record{}, err
	}
	if q.superseded(msg.Group, offset) {
		return Record{}, ErrRecordCompacted
	}
	q.recordRead(1, int64(len(msg.Payload)), start)
	return msg, nil
}

// ReadBatch implements Storage.ReadBatch.
func (q *Queue) ReadBatch(ctx context.Context, start uint64, n int) ([]Record, error) {
	q.hasReader.Store(true)
	q.m.inFlightRead.Add(1)
	defer q.m.inFlightRead.Add(-1)

	if n <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	begin := time.Now()

	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, ErrQueueClosed
	}
	minOff := q.minOffset
	total := q.nextOffset
	q.mu.RUnlock()

	// 1. Fast Failure: Reject requests below the minimum available offset
	if start < minOff {
		return nil, ErrOffsetTruncated
	}

	if start >= total {
		return nil, ErrOffsetNotFound
	}

	end := min(start+uint64(n), total)

	count := int(end - start)
	msgs := make([]Record, 0, count)

	for off := start; off < end; {
		seg, err := q.locateSegment(off)
		if err != nil {
			if errors.Is(err, ErrOffsetTruncated) {
				return nil, ErrOffsetTruncated
			}
			break
		}
		runEnd := end
		q.mu.RLock()
		for _, s := range q.segments {
			if s.baseOff > off {
				runEnd = min(runEnd, s.baseOff)
				break
			}
		}
		q.mu.RUnlock()
		if runEnd <= off {
			break
		}
		var advanced bool
		off, msgs, advanced = q.readBatchRun(seg, off, runEnd, msgs)
		if !advanced {
			break
		}
	}

	var outBytes int64
	for _, msg := range msgs {
		outBytes += int64(len(msg.Payload))
	}
	q.recordRead(len(msgs), outBytes, begin)
	return msgs, nil
}

const (
	readBatchMaxRun  = 4096
	readBatchMaxSpan = 4 << 20
)

// batchReadAtHook, when non-nil, receives the byte count of every bulk log read the batch path issues.
var batchReadAtHook func(n int)

type idxEntry struct {
	pos, length int64
}

// readBatchRun reads [start, runEnd) from one segment in bulk — one index
// read plus one contiguous log read per chunk — appending decoded records.
// ok is false when the run must stop early.
func (q *Queue) readBatchRun(seg *segment, start, runEnd uint64, msgs []Record) (uint64, []Record, bool) {
	off := start
	for off < runEnd {
		chunkCount := int(min(uint64(readBatchMaxRun), runEnd-off))
		cached, misses := q.takeCachedRange(off, chunkCount, msgs)
		msgs = cached.msgs
		if len(misses) == 0 {
			off += uint64(chunkCount)
			continue
		}

		entries := q.readIndexRun(seg, off, chunkCount)
		if entries == nil {
			return off, msgs, false
		}
		first, last := -1, -1
		for _, i := range misses {
			if entries[i].length >= minRecordSize-recordLengthSize {
				if first < 0 {
					first = i
				}
				last = i
			}
		}
		if first < 0 {
			if q.cfg.KeyedCompaction() {
				off += uint64(chunkCount)
				continue
			}
			return off, msgs, false
		}
		spanStart := entries[first].pos
		spanEnd := entries[last].pos + entries[last].length
		if spanEnd-spanStart > readBatchMaxSpan {
			var kept int64
			trim := 0
			for trim < chunkCount && kept < readBatchMaxSpan {
				kept += entries[trim].length
				trim++
			}
			last = -1
			for _, i := range misses {
				if i < trim && entries[i].length >= minRecordSize-recordLengthSize {
					last = i
				}
			}
			if last < 0 {
				return off, msgs, false
			}
			chunkCount = trim
			entries = entries[:trim]
			spanEnd = entries[last].pos + entries[last].length
		}

		logData, _ := seg.views()
		var data []byte
		if len(logData) >= int(spanEnd) {
			data = logData[spanStart:spanEnd]
		} else {
			buf := make([]byte, spanEnd-spanStart)
			if batchReadAtHook != nil {
				batchReadAtHook(len(buf))
			}
			if _, err := seg.logFile.ReadAt(buf, spanStart); err != nil {
				return off, msgs, false
			}
			data = buf
		}

		for i := range chunkCount {
			if cached.flags[i] {
				off++
				continue
			}
			e := entries[i]
			if e.length < minRecordSize-recordLengthSize {
				if q.cfg.KeyedCompaction() {
					off++
					continue
				}
				return off, msgs, false
			}
			id, ts, group, payload, err := decodeRecord(data[e.pos-spanStart:][:e.length][recordLengthSize:])
			if err != nil {
				return off, msgs, false
			}
			if q.superseded(group, off) {
				off++
				continue
			}
			msgs = append(msgs, Record{Offset: off, ID: id, Timestamp: ts, Group: group, Payload: payload})
			off++
		}
	}
	return off, msgs, true
}

// cachedRange records which offsets of a chunk the ring cache already serves.
type cachedRange struct {
	flags []bool
	msgs  []Record
}

// takeCachedRange drains cache hits out of a chunk and returns the miss positions.
func (q *Queue) takeCachedRange(off uint64, count int, msgs []Record) (cachedRange, []int) {
	cr := cachedRange{flags: make([]bool, count), msgs: msgs}
	misses := make([]int, 0, count)
	for i := range count {
		if msg, ok := q.cache.Get(off + uint64(i)); ok {
			q.m.cacheHits.Inc()
			cr.flags[i] = true
			if !q.superseded(msg.Group, off+uint64(i)) {
				cr.msgs = append(cr.msgs, msg)
			}
			continue
		}
		q.m.cacheMisses.Inc()
		misses = append(misses, i)
	}
	return cr, misses
}

// readIndexRun reads one block of consecutive index entries from the segment.
func (q *Queue) readIndexRun(seg *segment, off uint64, count int) []idxEntry {
	idxPos := int64(off-seg.baseOff) * rawIndexEntrySize
	raw := make([]byte, count*rawIndexEntrySize)
	_, idxData := seg.views()
	if len(idxData) >= int(idxPos)+count*rawIndexEntrySize {
		copy(raw, idxData[idxPos:int(idxPos)+count*rawIndexEntrySize])
	} else if _, err := seg.idxFile.ReadAt(raw, idxPos); err != nil {
		return nil
	}
	entries := make([]idxEntry, count)
	for i := range count {
		e := UnmarshalRawIndexEntry(raw[i*rawIndexEntrySize : (i+1)*rawIndexEntrySize])
		entries[i] = idxEntry{pos: e.pos, length: e.len}
	}
	return entries
}
