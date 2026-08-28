package queue

import (
	"context"
	"time"

	"github.com/gofrs/uuid/v5"
)

type Message struct {
	Offset  uint64
	ID      string
	Payload []byte
}

func (q *Queue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(payload)) > q.cfg.MaxMessageBytes {
		return 0, ErrMessageTooLarge
	}

	id, err := uuid.NewV7()
	if err != nil {
		return 0, err
	}
	rec := encodeRecord(id.String(), payload)
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
	q.mu.Unlock()

	seq, wake, err := q.w.push(rec, pos)
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
	q.index[offset] = indexEntry{file: q.active.logFile, pos: pos, len: recLen}
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

	rec := make([]byte, entry.len)
	if _, err := entry.file.ReadAt(rec, entry.pos); err != nil {
		return Message{}, err
	}

	id, payload, err := decodeRecord(rec[4:])
	if err != nil {
		return Message{}, err
	}

	return Message{Offset: offset, ID: id, Payload: payload}, nil
}

func (q *Queue) LastOffset() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return uint64(len(q.index))
}
