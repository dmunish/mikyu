package queue

import (
	"context"

	"github.com/gofrs/uuid/v5"
)

type Message struct {
	Offset  uint64
	ID      string
	Payload []byte
}

// Produce appends payload to the log and returns the offset it was written at.
func (q *Queue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return 0, err
	}
	rec := encodeRecord(id.String(), payload)

	q.mu.Lock()
	pos := q.pos
	q.pos += int64(len(rec))
	q.mu.Unlock()

	// Stage payload in flusher
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

	// Wait until durable on disk
	if err := q.w.wait(seq); err != nil {
		return 0, err
	}

	// Record in-memory index for local reads
	offset := uint64(seq)
	q.mu.Lock()
	for len(q.index) <= int(seq) {
		q.index = append(q.index, indexEntry{})
	}
	q.index[seq] = indexEntry{pos: pos, len: int64(len(rec))}
	q.mu.Unlock()

	return offset, nil
}

// Read returns the message at offset without consuming or advancing state.
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
	if _, err := q.file.ReadAt(rec, entry.pos); err != nil {
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
