package engine

import (
	"bytes"
	"context"
	"time"
	"uuid"
)

func (q *Queue) Append(ctx context.Context, payload []byte) (uint64, error) {
	offs, err := q.AppendBatch(ctx, [][]byte{payload})
	if err != nil {
		return 0, err
	}
	return offs[0], nil
}

// AppendBatch implements Storage.AppendBatch.
func (q *Queue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	start := time.Now()
	var totalBytes int64

	type stagedBatch struct {
		w         *wal
		seg       *segment
		recs      [][]byte
		positions []int64
		recLens   []int64
		offsets   []uint64
		ids       []uuid.UUID
		totalLen  int64
		startIdx  int
	}

	q.mu.Lock()
	if q.closed || q.active == nil || q.w == nil {
		q.mu.Unlock()
		return nil, ErrQueueClosed
	}
	if err := q.w.currentError(); err != nil {
		q.mu.Unlock()
		return nil, err
	}

	offsets := make([]uint64, len(payloads))
	ids := make([]uuid.UUID, len(payloads))
	recs := make([][]byte, len(payloads))
	recLens := make([]int64, len(payloads))

	// Prepare records up-front while the queue is locked, but do not mutate queue
	// positions until a complete per-segment group is ready to commit.
	ts := time.Now()
	for i, payload := range payloads {
		if int64(len(payload)) > q.cfg.MaxMessageBytes {
			q.mu.Unlock()
			return nil, ErrMessageTooLarge
		}
		totalBytes += int64(len(payload))
		id := uuid.NewV7()
		rec := encodeRecord(id, ts, payload)
		recLen := int64(len(rec))
		if recLen > q.cfg.SegmentSize {
			q.mu.Unlock()
			return nil, ErrRecordTooLarge
		}
		ids[i], recs[i], recLens[i] = id, rec, recLen
	}

	commitSync := func(b *stagedBatch) (int64, bool, error) {
		seq, wake, err := b.w.pushBatch(b.recs, b.positions)
		if err != nil {
			return 0, false, err
		}
		b.seg.pos += b.totalLen
		for i := range b.recs {
			if err := b.seg.writeIndexAt(int64(b.offsets[i]-b.seg.baseOff)*rawIndexEntrySize, b.positions[i], b.recLens[i]); err != nil {
				b.w.setError(err)
				return 0, false, err
			}
		}
		return seq, wake, nil
	}

	commitAsync := func(b *stagedBatch) error {
		for i, rec := range b.recs {
			q.cache.Put(Message{
				Offset:    b.offsets[i],
				ID:        b.ids[i].String(),
				Timestamp: ts,
				Payload:   bytes.Clone(payloads[b.startIdx+i]),
			})
			b.w.asyncChan <- asyncEntry{seg: b.seg, rec: rec, pos: b.positions[i], recLen: b.recLens[i], offset: b.offsets[i]}
		}
		b.seg.pos += b.totalLen
		last := b.offsets[len(b.offsets)-1]
		if last+1 > q.publishedOffset {
			q.publishedOffset = last + 1
		}
		return nil
	}

	var staged stagedBatch
	reset := func(start int) {
		staged = stagedBatch{w: q.w, seg: q.active, startIdx: start}
	}
	reset(0)

	for i := range payloads {
		recLen := recLens[i]
		rollRequired := q.active.pos+staged.totalLen+recLen > q.cfg.SegmentSize || time.Since(q.active.createdAt) > q.cfg.SegmentAge
		if rollRequired {
			if len(staged.recs) == 0 {
				// An old/aged active segment may already be unable to accept the next
				// record. Roll before staging the record so every group fits entirely
				// inside one segment.
				if err := q.roll(); err != nil {
					q.mu.Unlock()
					return nil, err
				}
				reset(i)
				rollRequired = false
			}
		}
		if rollRequired {
			if !q.cfg.Fsync {
				if err := commitAsync(&staged); err != nil {
					q.mu.Unlock()
					return nil, err
				}
			} else {
				seq, wake, err := commitSync(&staged)
				if err != nil {
					q.mu.Unlock()
					return nil, err
				}
				q.mu.Unlock()
				if wake {
					select {
					case staged.w.wake <- struct{}{}:
					default:
					}
				}
				if err := staged.w.wait(seq); err != nil {
					return nil, err
				}
				q.mu.Lock()
				if q.closed {
					q.mu.Unlock()
					return nil, ErrQueueClosed
				}
			}
			if err := q.roll(); err != nil {
				q.mu.Unlock()
				return nil, err
			}
			reset(i)
		}

		staged.recs = append(staged.recs, recs[i])
		staged.recLens = append(staged.recLens, recLens[i])
		staged.positions = append(staged.positions, q.active.pos+staged.totalLen)
		staged.offsets = append(staged.offsets, q.nextOffset)
		staged.ids = append(staged.ids, ids[i])
		staged.totalLen += recLen
		q.nextOffset++
		offsets[i] = q.nextOffset - 1
	}

	if !q.cfg.Fsync {
		if err := commitAsync(&staged); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		q.broadcastLocked()
		q.mu.Unlock()
		q.recordAppend(len(payloads), totalBytes, start)
		return offsets, nil
	}

	seq, wake, err := commitSync(&staged)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}
	q.mu.Unlock()
	if wake {
		select {
		case staged.w.wake <- struct{}{}:
		default:
		}
	}
	if err := staged.w.wait(seq); err != nil {
		return nil, err
	}
	q.mu.Lock()
	if last := offsets[len(offsets)-1]; last+1 > q.publishedOffset {
		q.publishedOffset = last + 1
	}
	q.broadcastLocked()
	q.mu.Unlock()
	q.recordAppend(len(payloads), totalBytes, start)
	return offsets, nil
}
