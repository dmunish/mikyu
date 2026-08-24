package queue

import (
	"io"

	"github.com/gofrs/uuid/v5"
)

type Message struct {
	Offset  uint64
	ID      string
	Payload []byte
}

// Push appends payload to the log and returns the offset it was written at.
func (q *Queue) Push(payload []byte) (uint64, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return 0, err
	}
	rec := encodeRecord(id.String(), payload)

	q.mu.Lock()
	defer q.mu.Unlock()

	pos, err := q.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := q.file.Write(rec); err != nil {
		return 0, err
	}
	if err := q.file.Sync(); err != nil {
		return 0, err
	}

	offset := uint64(len(q.index))
	q.index = append(q.index, indexEntry{pos: pos, len: int64(len(rec))})
	return offset, nil
}

// Returns the message at offset without consuming or advancing anything
func (q *Queue) Read(offset uint64) (Message, error) {
	q.mu.Lock()
	if offset >= uint64(len(q.index)) {
		q.mu.Unlock()
		return Message{}, ErrOffsetNotFound
	}
	entry := q.index[offset]
	q.mu.Unlock()

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

func (q *Queue) Tail() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return uint64(len(q.index))
}
