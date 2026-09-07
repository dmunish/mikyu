package logqueue

import (
	"context"
	"time"
)

// LogRetentionPolicy configures log driver retention parameters.
type LogRetentionPolicy struct {
	TTL time.Duration
}

// SetRetentionPolicy updates the retention configuration for this LogQueue.
func (q *LogQueue) SetRetentionPolicy(policy LogRetentionPolicy) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.policy = policy
}

// RetentionPolicy returns the current retention policy.
func (q *LogQueue) RetentionPolicy() LogRetentionPolicy {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.policy
}

// TruncateFloor calculates the minimum safe truncation offset for this LogQueue.
func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	q.mu.RLock()
	policy := q.policy
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
	q.mu.RUnlock()

	var consumerFloor uint64
	if hasGroups {
		consumerFloor = minConsumerOff
	}

	var ttlFloor uint64
	if policy.TTL > 0 {
		segments := q.storage.Segments()
		now := time.Now()
		for _, seg := range segments {
			// Never expire the active write segment
			if seg.IsActive {
				break
			}
			if now.Sub(seg.CreatedAt) > policy.TTL {
				ttlFloor = seg.NextOffset
			} else {
				break
			}
		}
	}

	floor := consumerFloor
	if ttlFloor > floor {
		floor = ttlFloor
	}

	return floor, nil
}

// Truncate executes physical segment truncation before minOffset.
// If minOffset is 0, it automatically computes the driver's retention floor via TruncateFloor().
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if minOffset == 0 {
		floor, err := q.TruncateFloor(ctx)
		if err != nil || floor == 0 {
			return 0, err
		}
		minOffset = floor
	}
	return q.storage.TruncateBefore(ctx, minOffset)
}
