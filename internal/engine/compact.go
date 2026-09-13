package engine

import (
	"context"
	"time"
)

// SegmentInfo holds physical metadata for an engine log segment.
type SegmentInfo struct {
	BaseOffset uint64
	NextOffset uint64
	SizeBytes  int64
	CreatedAt  time.Time
	IsActive   bool
}

// MinOffset returns the lowest readable logical offset currently on disk.
func (q *Queue) MinOffset() uint64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.minOffset
}

// Segments returns structural metadata for all active and closed log segments.
func (q *Queue) Segments() []SegmentInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	infos := make([]SegmentInfo, 0, len(q.segments))
	for i, seg := range q.segments {
		isActive := (seg == q.active)
		var nextOff uint64
		if isActive {
			nextOff = q.nextOffset
		} else if i+1 < len(q.segments) {
			nextOff = q.segments[i+1].baseOff
		} else {
			nextOff = seg.nextOffset()
		}

		infos = append(infos, SegmentInfo{
			BaseOffset: seg.baseOff,
			NextOffset: nextOff,
			SizeBytes:  seg.diskSize(),
			CreatedAt:  seg.createdAt,
			IsActive:   isActive,
		})
	}
	return infos
}

// FloorOlderThan implements Storage.FloorOlderThan.
func (q *Queue) FloorOlderThan(ctx context.Context, olderThan time.Time) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return 0, ErrQueueClosed
	}

	floor := q.minOffset
	for i, seg := range q.segments {
		if seg == q.active {
			break
		}
		ts, ok := seg.lastTimestamp()
		if !ok || !ts.Before(olderThan) {
			break
		}
		floor = segmentNextOffset(q.segments, i)
	}
	return floor, nil
}

// FloorForSize implements Storage.FloorForSize.
func (q *Queue) FloorForSize(ctx context.Context, maxBytes int64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return 0, ErrQueueClosed
	}
	if maxBytes <= 0 {
		return q.minOffset, nil
	}

	var total int64
	for _, seg := range q.segments {
		total += seg.diskSize()
	}

	floor := q.minOffset
	for i, seg := range q.segments {
		if seg == q.active || total <= maxBytes {
			break
		}
		total -= seg.diskSize()
		floor = segmentNextOffset(q.segments, i)
	}
	return floor, nil
}

// segmentNextOffset returns the base offset of the segment following index i,
// or the segment's own next offset if it is the last.
func segmentNextOffset(segments []*segment, i int) uint64 {
	if i+1 < len(segments) {
		return segments[i+1].baseOff
	}
	return segments[i].nextOffset()
}

// TruncateBefore safely unmaps and unlinks closed physical log segments whose record offsets fall strictly below targetOffset.
func (q *Queue) TruncateBefore(ctx context.Context, targetOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.segments) == 0 || targetOffset <= q.minOffset {
		return 0, nil
	}

	var (
		reclaimedBytes int64
		remaining      []*segment
		truncated      bool
	)

	for i, seg := range q.segments {
		// Active write segment can NEVER be truncated
		if seg == q.active {
			remaining = append(remaining, q.segments[i:]...)
			break
		}

		// Determine NextOffset boundary for closed segment
		var nextOff uint64
		if i+1 < len(q.segments) {
			nextOff = q.segments[i+1].baseOff
		} else {
			nextOff = seg.nextOffset()
		}

		// Segment is eligible for physical deletion ONLY if all records are < targetOffset
		if nextOff <= targetOffset {
			freed, err := seg.destroy()
			if err != nil {
				// Abort on file deletion failure; retain remaining segments
				remaining = append(remaining, q.segments[i:]...)
				break
			}
			reclaimedBytes += freed
			truncated = true
		} else {
			// Stop at the first segment spanning offsets >= targetOffset
			remaining = append(remaining, q.segments[i:]...)
			break
		}
	}

	if truncated {
		q.segments = remaining
		if len(q.segments) > 0 {
			q.minOffset = q.segments[0].baseOff
		}
		// Invalidate ring cache slots for evicted offsets
		q.cache.EvictBefore(q.minOffset)
	}

	return reclaimedBytes, nil
}
