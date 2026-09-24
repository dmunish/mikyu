package engine

import (
	"bytes"
	"context"
	"fmt"
	"time"
	"uuid"
)

// appendOptions carries the effective per-call append settings.
type appendOptions struct {
	group []byte
}

// AppendOption configures the record(s) being appended.
type AppendOption func(*appendOptions)

// WithGroup associates a compaction group with the appended record(s).
func WithGroup(g []byte) AppendOption {
	return func(o *appendOptions) { o.group = append([]byte(nil), g...) }
}

// ResolveAppendOptions folds a set of append options into their effective values.
func ResolveAppendOptions(opts ...AppendOption) (group []byte) {
	var o appendOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.group
}

func (q *Queue) Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error) {
	offs, err := q.AppendBatch(ctx, [][]byte{payload}, opts...)
	if err != nil {
		return 0, err
	}
	return offs[0], nil
}

// AppendBatch implements Storage.AppendBatch.
func (q *Queue) AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error) {
	q.m.inFlightApp.Add(1)
	defer q.m.inFlightApp.Add(-1)

	group := ResolveAppendOptions(opts...)
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
		if int64(len(payload)) > q.cfg.MaxPayload {
			q.mu.Unlock()
			return nil, fmt.Errorf("%w: queue %q payload of %d bytes exceeds max_payload %d", ErrMessageTooLarge, q.name, len(payload), q.cfg.MaxPayload)
		}
		totalBytes += int64(len(payload))
		id := uuid.NewV7()
		rec := encodeRecord(id, ts, group, payload)
		recLen := int64(len(rec))
		if recLen > q.cfg.MaxSegment {
			q.mu.Unlock()
			return nil, fmt.Errorf("%w: queue %q record of %d bytes exceeds max_segment %d", ErrRecordTooLarge, q.name, recLen, q.cfg.MaxSegment)
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
			entry := asyncEntry{seg: b.seg, rec: rec, pos: b.positions[i], recLen: b.recLens[i], offset: b.offsets[i]}
			if err := b.w.pushAsync(ctx, entry); err != nil {
				return err
			}
			q.cache.Put(Record{
				Offset:    b.offsets[i],
				ID:        b.ids[i].String(),
				Timestamp: ts,
				Group:     group,
				Payload:   bytes.Clone(payloads[b.startIdx+i]),
			})
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
		rollRequired := q.active.pos+staged.totalLen+recLen > q.cfg.MaxSegment || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge
		if rollRequired && len(staged.recs) == 0 {
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
		// The staged group also commits once it would overflow the WAL byte budget.
		flushRequired := len(staged.recs) > 0 && staged.totalLen+recLen > q.cfg.MaxFlushBytes
		if rollRequired || flushRequired {
			if q.cfg.Async() {
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
			if rollRequired {
				if err := q.roll(); err != nil {
					q.mu.Unlock()
					return nil, err
				}
			}
			reset(i)
		}

		staged.recs = append(staged.recs, recs[i])
		staged.recLens = append(staged.recLens, recLens[i])
		staged.positions = append(staged.positions, q.active.pos+staged.totalLen)
		staged.offsets = append(staged.offsets, q.nextOffset)
		staged.ids = append(staged.ids, ids[i])
		staged.totalLen += recLen
		if group != nil && q.keyIndex != nil {
			q.keyIndex[string(group)] = q.nextOffset
		}
		q.nextOffset++
		offsets[i] = q.nextOffset - 1
	}

	if group != nil && q.keyIndex != nil {
		q.warnKeyIndexSizeLocked()
	}

	if q.cfg.Async() {
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
