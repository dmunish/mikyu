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

	var currentSeg *segment
	for off := start; off < end; off++ {
		// 2. Fast Path: Check in-memory ring cache first (~10 ns)
		if msg, ok := q.cache.Get(off); ok {
			q.m.cacheHits.Inc()
			if !q.superseded(msg.Group, off) {
				msgs = append(msgs, msg)
			}
			continue
		}
		q.m.cacheMisses.Inc()

		// 3. Disk / MMap Path: Fall back to historical log segments
		if currentSeg == nil || off < currentSeg.baseOff {
			seg, err := q.locateSegment(off)
			if err != nil {
				if errors.Is(err, ErrOffsetTruncated) {
					return nil, ErrOffsetTruncated
				}
				break
			}
			currentSeg = seg
		}

		msg, err := q.readFromSegment(currentSeg, off)
		if err != nil {
			if errors.Is(err, ErrRecordCompacted) {
				continue
			}
			// Offset crossed current segment boundary, locate new segment
			seg, err := q.locateSegment(off)
			if err != nil {
				if errors.Is(err, ErrOffsetTruncated) {
					return nil, ErrOffsetTruncated
				}
				break
			}
			currentSeg = seg
			msg, err = q.readFromSegment(currentSeg, off)
			if err != nil {
				if errors.Is(err, ErrRecordCompacted) {
					continue
				}
				break
			}
		}
		if q.superseded(msg.Group, off) {
			continue
		}
		msgs = append(msgs, msg)
	}

	var outBytes int64
	for _, msg := range msgs {
		outBytes += int64(len(msg.Payload))
	}
	q.recordRead(len(msgs), outBytes, begin)
	return msgs, nil
}
