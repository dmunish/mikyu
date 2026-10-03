package engine

import (
	"context"
	"fmt"
	"time"
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

// stagedBatch holds the records of one append that share a target segment.
type stagedBatch struct {
	w         *wal
	seg       *segment
	recs      [][]byte
	bufs      []*pooledBuf
	positions []int64
	recLens   []int64
	offsets   []uint64
	ids       [][16]byte
	totalLen  int64
	startIdx  int
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
	ids := make([][16]byte, len(payloads))
	recs := make([][]byte, len(payloads))
	bufs := make([]*pooledBuf, len(payloads))
	recLens := make([]int64, len(payloads))

	// Prepare records up-front while the queue is locked, but do not mutate queue
	// positions until a complete per-segment group is ready to commit.
	ts := time.Now()
	cacheable := !q.cacheGated || q.hasReader.Load()
	recyclable := !q.cfg.Async() || !cacheable
	for i, payload := range payloads {
		if int64(len(payload)) > q.cfg.MaxPayload {
			q.mu.Unlock()
			return nil, fmt.Errorf("%w: queue %q payload of %d bytes exceeds max_payload %d", ErrMessageTooLarge, q.name, len(payload), q.cfg.MaxPayload)
		}
		totalBytes += int64(len(payload))
		recLen := int64(minRecordSize + len(group) + len(payload))
		if recLen > q.cfg.MaxSegment {
			q.mu.Unlock()
			return nil, fmt.Errorf("%w: queue %q record of %d bytes exceeds max_segment %d", ErrRecordTooLarge, q.name, recLen, q.cfg.MaxSegment)
		}
		id := newRecordID()
		var rec []byte
		if recyclable {
			bb := getBuf(int(recLen))
			bufs[i] = bb
			rec = encodeRecord(bb.b, id, ts, group, payload)
		} else {
			rec = encodeRecord(nil, id, ts, group, payload)
		}
		ids[i], recs[i], recLens[i] = id, rec, recLen
	}

	commitSync := func(b *stagedBatch) (int64, error) {
		seq, _, err := b.w.pushBatch(b.recs, b.positions)
		if err != nil {
			return 0, err
		}
		encodeKeep := 4 * int(q.cfg.MaxPayload)
		for _, bb := range b.bufs {
			bb.release(encodeKeep)
		}
		b.seg.pos += b.totalLen
		idxPos := int64(b.offsets[0]-b.seg.baseOff) * rawIndexEntrySize
		if err := b.seg.writeIndexBatchAt(idxPos, b.positions, b.recLens); err != nil {
			b.w.setError(err)
			return 0, err
		}
		return seq, nil
	}

	// commitAsyncGroup runs the async commit in three steps: reserve space and
	// offsets under the queue lock, push onto the WAL channel with the lock
	// released (a full channel must not stall other producers), then commit the
	// reservation under the lock again.
	commitAsyncGroup := func(b *stagedBatch) error {
		node := q.reserveLocked(b)
		entries := make([]asyncEntry, len(b.recs))
		for i, rec := range b.recs {
			entries[i] = asyncEntry{seg: b.seg, rec: rec, pos: b.positions[i], recLen: b.recLens[i], offset: b.offsets[i], buf: b.bufs[i]}
		}
		q.mu.Unlock()
		pushErr := b.w.pushAsyncBatch(ctx, entries)
		if pushErr == nil && cacheable {
			// The rec buffers are fresh allocations the flusher only writes and
			// discards, so the cache can hold their payload slices by reference.
			groupLen := len(group)
			for i, rec := range b.recs {
				q.cache.Put(Record{
					Offset:    b.offsets[i],
					ID:        formatRecordID(b.ids[i]),
					Timestamp: ts,
					Group:     group,
					Payload:   rec[offBody+groupLen:],
				})
			}
		}
		q.mu.Lock()
		if pushErr != nil {
			q.abortReservationLocked(node)
			return pushErr
		}
		if q.closed {
			q.abortReservationLocked(node)
			return ErrQueueClosed
		}
		q.commitReservationLocked(node)
		return nil
	}

	var staged stagedBatch
	reset := func(start int) {
		staged = stagedBatch{w: q.w, seg: q.active, startIdx: start}
	}
	reset(0)

	for i := range payloads {
		recLen := recLens[i]
		rollRequired := q.active.pos+q.reservedPos+staged.totalLen+recLen > q.cfg.MaxSegment || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge
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
				if err := commitAsyncGroup(&staged); err != nil {
					q.mu.Unlock()
					return nil, err
				}
			} else {
				seq, err := commitSync(&staged)
				if err != nil {
					q.mu.Unlock()
					return nil, err
				}
				q.mu.Unlock()
				// A sync producer is about to block on this data, so always wake
				// the flusher; its linger and byte budget decide when to flush.
				select {
				case staged.w.wake <- struct{}{}:
				default:
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
		staged.bufs = append(staged.bufs, bufs[i])
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
		if len(staged.recs) > 0 {
			if err := commitAsyncGroup(&staged); err != nil {
				q.mu.Unlock()
				return nil, err
			}
		}
		q.broadcastLocked()
		q.mu.Unlock()
		q.recordAppend(len(payloads), totalBytes, start)
		return offsets, nil
	}

	seq, err := commitSync(&staged)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}
	q.mu.Unlock()
	select {
	case staged.w.wake <- struct{}{}:
	default:
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
