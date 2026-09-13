package logqueue

import (
	"context"
)

// TruncateFloor reports the consumer-lag floor for this LogQueue: the minimum high-water mark across its consumer groups.
func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}

	q.mu.RLock()
	defer q.mu.RUnlock()

	var (
		minConsumerOff uint64
		hasGroups      bool
	)
	for _, g := range q.groups {
		g.Mu.Lock()
		hw := g.HighWater
		g.Mu.Unlock()

		if !hasGroups || hw < minConsumerOff {
			minConsumerOff = hw
			hasGroups = true
		}
	}
	return minConsumerOff, hasGroups, nil
}

// Truncate executes physical segment truncation before minOffset.
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if minOffset == 0 {
		floor, constrained, err := q.TruncateFloor(ctx)
		if err != nil || !constrained {
			return 0, err
		}
		minOffset = floor
	}
	return q.storage.TruncateBefore(ctx, minOffset)
}
