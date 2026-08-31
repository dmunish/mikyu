package queue

import (
	"cmp"
	"context"
	"encoding/binary"
	"slices"
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

	seg := q.active
	pos := seg.pos
	seg.pos += recLen

	offset := q.nextOffset
	q.nextOffset++

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

	q.mu.Lock()
	_ = seg.writeIndex(pos, recLen)
	if offset%64 == 0 || len(q.sparseIndex) == 0 || q.sparseIndex[len(q.sparseIndex)-1].seg != seg {
		q.sparseIndex = append(q.sparseIndex, indexEntry{
			offset: offset,
			seg:    seg,
			pos:    pos,
			len:    recLen,
		})
	}
	q.mu.Unlock()

	return offset, nil
}

func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	q.mu.Lock()
	if offset >= q.nextOffset {
		q.mu.Unlock()
		return Message{}, ErrOffsetNotFound
	}

	// Binary search sparse index for closest entry <= target offset
	idx, found := slices.BinarySearchFunc(q.sparseIndex, offset, func(e indexEntry, target uint64) int {
		return cmp.Compare(e.offset, target)
	})
	if !found {
		idx--
	}
	baseEntry := q.sparseIndex[idx]
	q.mu.Unlock()

	pos := baseEntry.pos

	// Skip intermediate records up to target offset by reading only 4-byte length prefixes
	for currentOff := baseEntry.offset; currentOff < offset; currentOff++ {
		var lenBuf [4]byte
		if baseEntry.seg != nil && baseEntry.seg.mmapData != nil {
			copy(lenBuf[:], baseEntry.seg.mmapData[pos:pos+4])
		} else {
			if _, err := baseEntry.seg.logFile.ReadAt(lenBuf[:], pos); err != nil {
				return Message{}, err
			}
		}
		bodyLen := int64(binary.BigEndian.Uint32(lenBuf[:]))
		pos += 4 + bodyLen
	}

	// Pos now points directly to the record at target offset
	var lenBuf [4]byte
	if baseEntry.seg != nil && baseEntry.seg.mmapData != nil {
		copy(lenBuf[:], baseEntry.seg.mmapData[pos:pos+4])
	} else {
		if _, err := baseEntry.seg.logFile.ReadAt(lenBuf[:], pos); err != nil {
			return Message{}, err
		}
	}
	bodyLen := int64(binary.BigEndian.Uint32(lenBuf[:]))
	totalLen := 4 + bodyLen

	var rec []byte
	if baseEntry.seg != nil && baseEntry.seg.mmapData != nil {
		rec = baseEntry.seg.mmapData[pos : pos+totalLen]
	} else {
		rec = make([]byte, totalLen)
		if _, err := baseEntry.seg.logFile.ReadAt(rec, pos); err != nil {
			return Message{}, err
		}
	}

	id, ts, payload, err := decodeRecord(rec[4:])
	if err != nil {
		return Message{}, err
	}

	return Message{
		Offset:    offset,
		ID:        id,
		Timestamp: ts,
		Payload:   payload,
	}, nil
}

func (q *Queue) LastOffset() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.nextOffset
}

func (q *Queue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}

	recs := make([][]byte, len(payloads))
	positions := make([]int64, len(payloads))
	recLens := make([]int64, len(payloads))
	segments := make([]*segment, len(payloads))
	ts := time.Now()

	q.mu.Lock()
	for i, p := range payloads {
		if int64(len(p)) > q.cfg.MaxMessageBytes {
			q.mu.Unlock()
			return nil, ErrMessageTooLarge
		}
		id := uuid.NewV7()
		rec := encodeRecord(id.String(), ts, p)
		recLen := int64(len(rec))

		if q.active.pos+recLen > q.cfg.MaxSegmentBytes || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge {
			if err := q.roll(); err != nil {
				q.mu.Unlock()
				return nil, err
			}
		}

		positions[i] = q.active.pos
		recLens[i] = recLen
		recs[i] = rec
		segments[i] = q.active
		q.active.pos += recLen
	}

	startOffset := q.nextOffset
	q.nextOffset += uint64(len(payloads))

	seq, wake, err := q.w.pushBatch(recs, positions)
	q.mu.Unlock()

	if err != nil {
		return nil, err
	}

	if wake {
		select {
		case q.w.wake <- struct{}{}:
		default:
		}
	}

	if err := q.w.wait(seq); err != nil {
		return nil, err
	}

	offsets := make([]uint64, len(payloads))
	q.mu.Lock()
	for i, recLen := range recLens {
		off := startOffset + uint64(i)
		pos := positions[i]
		seg := segments[i]
		_ = seg.writeIndex(pos, recLen)
		if off%64 == 0 || len(q.sparseIndex) == 0 || q.sparseIndex[len(q.sparseIndex)-1].seg != seg {
			q.sparseIndex = append(q.sparseIndex, indexEntry{
				offset: off,
				seg:    seg,
				pos:    pos,
				len:    recLen,
			})
		}
		offsets[i] = off
	}
	q.mu.Unlock()

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
