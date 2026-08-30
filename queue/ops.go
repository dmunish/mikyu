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

func (q *Queue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	offsets := make([]uint64, 0, len(payloads))
	for _, p := range payloads {
		off, err := q.Produce(ctx, p)
		if err != nil {
			return offsets, err
		}
		offsets = append(offsets, off)
	}
	return offsets, nil
}

func (q *Queue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	out := make([]Message, 0, count)
	for i := range count {
		msg, err := q.Read(ctx, startOffset+uint64(i))
		if err != nil {
			return out, err
		}
		out = append(out, msg)
	}
	return out, nil
}

func (q *Queue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.offsets == nil {
		return ErrQueueNotFound
	}
	return q.offsets.Commit(q.name, group, offset)
}

func (q *Queue) FetchOffset(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if q.offsets == nil {
		return 0, ErrQueueNotFound
	}
	return q.offsets.Fetch(q.name, group), nil
}
