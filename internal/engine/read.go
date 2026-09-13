package engine

import (
	"context"
	"errors"
)

func (q *Queue) readFromSegment(seg *segment, offset uint64) (Message, error) {
	pos, recLen, err := seg.readIndexEntry(offset)
	if err != nil || recLen < (minRecordSize-recordLengthSize) {
		return Message{}, ErrOffsetNotFound
	}

	var buf []byte
	if len(seg.mmapData) >= int(pos+recLen) {
		buf = seg.mmapData[pos : pos+recLen]
	} else {
		buf = make([]byte, recLen)
		if _, err := seg.logFile.ReadAt(buf, pos); err != nil {
			return Message{}, err
		}
	}

	id, ts, payload, err := decodeRecord(buf[recordLengthSize:])
	if err != nil {
		return Message{}, err
	}
	return Message{Offset: offset, ID: id, Timestamp: ts, Payload: payload}, nil
}

// Read implements Storage.Read.
func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return Message{}, ErrQueueClosed
	}

	if offset < q.MinOffset() {
		return Message{}, ErrOffsetTruncated
	}

	// 1. Fast Path: Check in-memory ring cache first (~10 ns)
	if msg, ok := q.cache.Get(offset); ok {
		return msg, nil
	}

	// 2. Disk / MMap Path: Fall back to historical log segments
	seg, err := q.locateSegment(offset)
	if err != nil {
		return Message{}, err
	}

	return q.readFromSegment(seg, offset)
}

// ReadBatch implements Storage.ReadBatch.
func (q *Queue) ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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
	msgs := make([]Message, 0, count)

	var currentSeg *segment
	for off := start; off < end; off++ {
		// 2. Fast Path: Check in-memory ring cache first (~10 ns)
		if msg, ok := q.cache.Get(off); ok {
			msgs = append(msgs, msg)
			continue
		}

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
				break
			}
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}
