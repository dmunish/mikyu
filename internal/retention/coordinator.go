package retention

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// DriverProvider defines the interface required from consumption drivers.
type DriverProvider interface {
	TruncateFloor(ctx context.Context) (uint64, error)
}

// QueueTarget pairs a physical storage engine queue with its active drivers.
type QueueTarget struct {
	Name    string
	Storage engine.Storage
	Drivers []DriverProvider
}

// Coordinator arbitrates retention truncation across queues and drivers.
type Coordinator struct {
	mu       sync.RWMutex
	targets  map[string]*QueueTarget
	interval time.Duration
}

// NewCoordinator initializes a Retention Coordinator.
func NewCoordinator(interval time.Duration) *Coordinator {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Coordinator{
		targets:  make(map[string]*QueueTarget),
		interval: interval,
	}
}

// RegisterTarget registers a queue and its associated consumption drivers.
func (c *Coordinator) RegisterTarget(target *QueueTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets[target.Name] = target
}

// UnregisterTarget removes a queue target from the coordinator.
func (c *Coordinator) UnregisterTarget(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.targets, name)
}

// ReconcileQueue calculates min(driver.TruncateFloor) across all drivers
// for a single queue and instructs engine.Storage to truncate.
func (c *Coordinator) ReconcileQueue(ctx context.Context, name string) (int64, error) {
	c.mu.RLock()
	target, ok := c.targets[name]
	c.mu.RUnlock()

	if !ok || target == nil || target.Storage == nil || len(target.Drivers) == 0 {
		return 0, nil
	}

	var (
		effectiveFloor uint64
		hasFloor       bool
	)

	for _, driver := range target.Drivers {
		floor, err := driver.TruncateFloor(ctx)
		if err != nil {
			return 0, err
		}
		// Pick the minimum safe floor across all drivers
		if !hasFloor || floor < effectiveFloor {
			effectiveFloor = floor
			hasFloor = true
		}
	}

	if !hasFloor || effectiveFloor == 0 {
		return 0, nil
	}

	return target.Storage.TruncateBefore(ctx, effectiveFloor)
}

// ReconcileAll executes retention arbitration on all registered queue targets.
func (c *Coordinator) ReconcileAll(ctx context.Context) map[string]int64 {
	c.mu.RLock()
	names := make([]string, 0, len(c.targets))
	for name := range c.targets {
		names = append(names, name)
	}
	c.mu.RUnlock()

	results := make(map[string]int64)
	for _, name := range names {
		reclaimed, err := c.ReconcileQueue(ctx, name)
		if err == nil && reclaimed > 0 {
			results[name] = reclaimed
		}
	}
	return results
}

// Start runs a background loop executing ReconcileAll periodically.
func (c *Coordinator) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.ReconcileAll(ctx)
		}
	}
}
