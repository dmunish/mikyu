// Package retention arbitrates how much history each queue may delete.
package retention

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// FloorProvider defines the interface required from consumption drivers.
type FloorProvider interface {
	TruncateFloor(ctx context.Context) (floor uint64, constrained bool, err error)
}

// Policy is a queue's retention policy.
type Policy struct {
	Age   time.Duration
	Bytes int64
}

// isShrink reports whether p tightens retention relative to old: a dimension
// that was unlimited becomes bounded, or a bound decreases.
func (p Policy) isShrink(old Policy) bool {
	return tighter(p.Age, old.Age) || tighter(p.Bytes, old.Bytes)
}

// tighter reports whether new is a tighter bound than old, where zero means
// unlimited and therefore sorts above every finite bound.
func tighter[D ~int64](new, old D) bool {
	switch {
	case old == 0: // was unlimited
		return new > 0
	case new == 0: // became unlimited
		return false
	default:
		return new < old
	}
}

// Target pairs a physical storage engine queue with its active drivers and its retention policy.
type Target struct {
	Name    string
	Storage engine.Storage
	Drivers []FloorProvider
	Policy  Policy
	mu      sync.Mutex
}

// Coordinator arbitrates retention truncation across queues and drivers.
type Coordinator struct {
	mu       sync.RWMutex
	targets  map[string]*Target
	interval time.Duration
}

// NewCoordinator initializes a Retention Coordinator.
func NewCoordinator(interval time.Duration) *Coordinator {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Coordinator{
		targets:  make(map[string]*Target),
		interval: interval,
	}
}

// RegisterTarget registers or replaces the complete driver set for a queue.
func (c *Coordinator) RegisterTarget(target *Target) {
	if target == nil || target.Name == "" || target.Storage == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets[target.Name] = target
}

// RegisterDriver adds a driver to a queue's retention target.
func (c *Coordinator) RegisterDriver(name string, storage engine.Storage, driver FloorProvider) {
	if name == "" || storage == nil || driver == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.targets[name]
	if target == nil {
		target = &Target{Name: name, Storage: storage}
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

// SetPolicy updates the retention policy for name and reports whether the new policy shrinks retention relative to the previous one.
func (c *Coordinator) SetPolicy(name string, p Policy) (shrunk bool) {
	if name == "" {
		return false
	}
	c.mu.Lock()
	target := c.targets[name]
	if target == nil {
		target = &Target{Name: name}
		c.targets[name] = target
	}
	c.mu.Unlock()
	target.mu.Lock()
	old := target.Policy
	target.Policy = p
	target.mu.Unlock()
	return p.isShrink(old)
}

// UnregisterDriver removes a driver from a queue's retention target.
func (c *Coordinator) UnregisterDriver(name string, driver FloorProvider) {
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

// ReconcileQueue applies the queue's policy-driven floor.
func (c *Coordinator) ReconcileQueue(ctx context.Context, name string) (int64, error) {
	return c.ReconcileAt(ctx, name, 0)
}

// ReconcileAt truncates a queue toward the floor its retention policy (and an optional explicit requested floor) allows, capped by the floor the constrained drivers agree is safe.
func (c *Coordinator) ReconcileAt(ctx context.Context, name string, requested uint64) (int64, error) {
	c.mu.RLock()
	target, ok := c.targets[name]
	c.mu.RUnlock()
	if !ok || target == nil {
		return 0, nil
	}

	target.mu.Lock()
	defer target.mu.Unlock()
	if target.Storage == nil {
		return 0, nil
	}

	policyFloor, err := target.policyFloor(ctx, requested)
	if err != nil {
		return 0, err
	}
	if policyFloor == 0 {
		return 0, nil
	}

	// Driver floors cap deletability; they never mandate it.
	var (
		safeFloor   uint64
		constrained bool
	)
	for _, driver := range target.Drivers {
		floor, has, err := driver.TruncateFloor(ctx)
		if err != nil {
			return 0, err
		}
		if !has {
			continue
		}
		if !constrained || floor < safeFloor {
			safeFloor, constrained = floor, true
		}
	}
	if constrained && safeFloor < policyFloor {
		policyFloor = safeFloor
	}
	if policyFloor == 0 {
		return 0, nil
	}
	return target.Storage.TruncateBefore(ctx, policyFloor)
}

// policyFloor computes how far this target's retention policy permits
// deletion, including an explicit requested floor. A zero result means the
// policy permits no deletion.
func (t *Target) policyFloor(ctx context.Context, requested uint64) (uint64, error) {
	var floor uint64
	if t.Policy.Age > 0 {
		f, err := t.Storage.FloorOlderThan(ctx, time.Now().Add(-t.Policy.Age))
		if err != nil {
			return 0, err
		}
		if f > floor {
			floor = f
		}
	}
	if t.Policy.Bytes > 0 {
		f, err := t.Storage.FloorForSize(ctx, t.Policy.Bytes)
		if err != nil {
			return 0, err
		}
		if f > floor {
			floor = f
		}
	}
	if requested > floor {
		floor = requested
	}
	return floor, nil
}

// ReconcileAll executes retention arbitration on all registered queue targets.
func (c *Coordinator) ReconcileAll(ctx context.Context) map[string]int64 {
	c.mu.RLock()
	names := slices.Collect(maps.Keys(c.targets))
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
