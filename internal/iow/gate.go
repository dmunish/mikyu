package iow

import (
	"fmt"
	"log/slog"
	"sync"
)

// ringGate serializes publications to the io_uring ring and retires it on the
// first failure. Once broken, no publication runs again: the caller gets the
// recorded error and nothing is published.
//
// The ring itself is Linux-only, but the gate is pure bookkeeping and is tested
// on every host.
type ringGate struct {
	mu     sync.Mutex
	broken error
}

// run holds the gate for one publication and breaks it when fn fails.
func (g *ringGate) run(fn func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.broken != nil {
		return g.broken
	}
	if err := fn(); err != nil {
		return g.breakLocked(err)
	}
	return nil
}

// fail retires the gate from outside a publication; the first error wins.
func (g *ringGate) fail(err error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.breakLocked(err)
}

// isBroken reports whether the gate has been retired.
func (g *ringGate) isBroken() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.broken != nil
}

// breakLocked records the first failure and reports it wrapped for errors.Is.
func (g *ringGate) breakLocked(err error) error {
	if g.broken == nil {
		g.broken = fmt.Errorf("%w: %w", ErrRingBroken, err)
		slog.Error("iow: io_uring submission failed; ring retired", "err", err)
	}
	return g.broken
}