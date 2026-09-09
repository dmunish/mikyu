package retention

import (
	"context"
	"slices"
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
	mu      sync.Mutex
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

// RegisterTarget registers or replaces the complete driver set for a queue.
func (c *Coordinator) RegisterTarget(target *QueueTarget) {
	if target == nil || target.Name == "" || target.Storage == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets[target.Name] = target
}

// RegisterDriver adds a driver to a queue's retention target. Registration is
// idempotent by driver identity for the normal pointer-backed drivers used by
// Mikyu, while keeping the coordinator independent of their concrete types.
func (c *Coordinator) RegisterDriver(name string, storage engine.Storage, driver DriverProvider) {
	if name == "" || storage == nil || driver == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.targets[name]
	if target == nil {
		target = &QueueTarget{Name: name, Storage: storage}
		c.targets[name] = target
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.Storage == nil {
		target.Storage = storage
	}
	if slices.Contains(target.Drivers, driver) {
		return
	}
	target.Drivers = append(target.Drivers, driver)
}

// UnregisterDriver removes a driver from a queue's retention target.
func (c *Coordinator) UnregisterDriver(name string, driver DriverProvider) {
	if name == "" || driver == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.targets[name]
	if target == nil {
		return
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	for i, existing := range target.Drivers {
		if existing == driver {
			target.Drivers = append(target.Drivers[:i], target.Drivers[i+1:]...)
			break
		}
	}
	if len(target.Drivers) == 0 {
		delete(c.targets, name)
	}
}

// UnregisterTarget removes a queue target from the coordinator.
func (c *Coordinator) UnregisterTarget(name string) {
	c.mu.Lock()
	target := c.targets[name]
	if target == nil {
		c.mu.Unlock()
		return
	}
	target.mu.Lock()
	delete(c.targets, name)
	target.mu.Unlock()
	c.mu.Unlock()
}

// ReconcileQueue calculates the maximum currently safe truncation floor and
// applies it. It is the policy-driven/default maintenance operation.
func (c *Coordinator) ReconcileQueue(ctx context.Context, name string) (int64, error) {
	return c.ReconcileQueueAt(ctx, name, 0)
}

// ReconcileQueueAt enforces a caller-requested truncation floor without ever
// exceeding the floor that all registered drivers agree is safe. A zero
// requested floor means "use the computed safe floor".
func (c *Coordinator) ReconcileQueueAt(ctx context.Context, name string, requested uint64) (int64, error) {
	c.mu.RLock()
	target, ok := c.targets[name]
	c.mu.RUnlock()
	if !ok || target == nil {
		return 0, nil
	}

	target.mu.Lock()
	defer target.mu.Unlock()
	if target.Storage == nil || len(target.Drivers) == 0 {
		return 0, nil
	}

	var (
		safeFloor uint64
		hasFloor  bool
	)
	for _, driver := range target.Drivers {
		floor, err := driver.TruncateFloor(ctx)
		if err != nil {
			return 0, err
		}
		if !hasFloor || floor < safeFloor {
			safeFloor = floor
			hasFloor = true
		}
	}

	if !hasFloor || safeFloor == 0 {
		return 0, nil
	}
	if requested > 0 && requested < safeFloor {
		safeFloor = requested
	}
	return target.Storage.TruncateBefore(ctx, safeFloor)
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
