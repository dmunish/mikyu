package queue

import (
	"context"
	"time"
	"uuid"
)

type Message struct {
	Offset    uint64
	Timestamp time.Time
	ID        string
	Payload   []byte
}

func (q *Queue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(payload)) > q.cfg.MaxMessageBytes {
		return 0, ErrMessageTooLarge
	}

	ts := time.Now()
	id := uuid.NewV7()
	rec := encodeRecord(id.String(), ts, payload)
	recLen := int64(len(rec))

	q.mu.Lock()
	if q.active.pos+recLen > q.cfg.MaxSegmentBytes || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge {
		if err := q.roll(); err != nil {
			q.mu.Unlock()
			return 0, err
		}
	}

	pos := q.active.pos
	q.active.pos += recLen

	seq, wake, err := q.w.push(rec, pos)
	q.mu.Unlock()
	if err != nil {
		return 0, err
	}

	if wake {
		select {
		case q.w.wake <- struct{}{}:
		default:
		}
	}

	if err := q.w.wait(seq); err != nil {
		return 0, err
	}

	offset := uint64(seq)

	q.mu.Lock()
	_ = q.active.writeIndex(pos, recLen)
	for len(q.index) <= int(offset) {
		q.index = append(q.index, indexEntry{})
	}
	q.index[offset] = indexEntry{seg: q.active, pos: pos, len: recLen}
	q.mu.Unlock()

	return offset, nil
}

func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	q.mu.Lock()
	if offset >= uint64(len(q.index)) {
		q.mu.Unlock()
		return Message{}, ErrOffsetNotFound
	}
	entry := q.index[offset]
	q.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	var rec []byte
	if entry.seg != nil && entry.seg.mmapData != nil {
		// Fast path: Zero-copy slice directly into the mmapped OS page cache
		rec = entry.seg.mmapData[entry.pos : entry.pos+entry.len]
	} else {
		// Fallback path: Traditional ReadAt for the active, unmapped segment
		rec = make([]byte, entry.len)
		if _, err := entry.seg.logFile.ReadAt(rec, entry.pos); err != nil {
			return Message{}, err
		}
	}

	id, ts, payload, err := decodeRecord(rec[4:])
	if err != nil {
		return Message{}, err
	}

	return Message{Offset: offset, ID: id, Timestamp: ts, Payload: payload}, nil
}

func (q *Queue) LastOffset() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return uint64(len(q.index))
}
