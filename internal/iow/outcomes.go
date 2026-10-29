package iow

import (
	"sync"
	"syscall"
)

// outcomes tracks one writer's tagged submissions: how many completions each tag
// still owes, the first error seen, and the callback that fires when the last one
// lands. A tag submitted without a callback is waited on through wait, which is
// how Sync observes a lone fsync.
type outcomes struct {
	mu     sync.Mutex
	cond   *sync.Cond
	pend   map[uint32]int32
	res    map[uint32]int64
	dones  map[uint32]func(error)
	closed bool
}

// newOutcomes builds empty tag bookkeeping for one writer.
func newOutcomes() *outcomes {
	o := &outcomes{
		pend:  make(map[uint32]int32),
		res:   make(map[uint32]int64),
		dones: make(map[uint32]func(error)),
	}
	o.cond = sync.NewCond(&o.mu)
	return o
}

// register records a submission before its operations reach the kernel, so a
// completion that lands immediately still finds the tag. It fails once closed.
func (o *outcomes) register(tag uint32, n int32, done func(error)) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	o.pend[tag] += n
	if done != nil {
		o.dones[tag] = done
	}
	return nil
}

// isClosed reports whether the bookkeeping has been shut down.
func (o *outcomes) isClosed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closed
}

// rollback removes a submission whose operations never reached the kernel.
func (o *outcomes) rollback(tag uint32, n int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pend[tag] -= n
	if o.pend[tag] > 0 {
		return
	}
	delete(o.pend, tag)
	delete(o.res, tag)
	delete(o.dones, tag)
}

// complete records one completion for tag. The last one either fires the tag's
// callback or, for a tag waited on through wait, keeps the outcome until the
// waiter consumes it.
func (o *outcomes) complete(tag uint32, res int64) {
	o.mu.Lock()
	n, ok := o.pend[tag]
	if !ok {
		o.mu.Unlock()
		return
	}
	if n > 0 {
		o.pend[tag] = n - 1
	}
	if res < 0 && o.res[tag] == 0 {
		o.res[tag] = res
	}
	if o.pend[tag] != 0 {
		o.mu.Unlock()
		return
	}
	if o.dones[tag] == nil {
		// A waited tag keeps its first error in res for wait to read; deleting it
		// here would report a failed fsync as a success.
		o.cond.Broadcast()
		o.mu.Unlock()
		return
	}
	done := o.dones[tag]
	err := o.outcomeLocked(tag)
	delete(o.pend, tag)
	delete(o.res, tag)
	delete(o.dones, tag)
	o.mu.Unlock()
	done(err)
}

// wait parks until every completion for tag has landed and reports the first
// error recorded for it.
func (o *outcomes) wait(tag uint32) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for o.pend[tag] > 0 && !o.closed {
		o.cond.Wait()
	}
	if o.closed {
		return ErrClosed
	}
	err := o.outcomeLocked(tag)
	delete(o.pend, tag)
	delete(o.res, tag)
	return err
}

// close marks the bookkeeping closed and wakes every waiter with ErrClosed. It
// reports whether this call closed it.
func (o *outcomes) close() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	o.closed = true
	o.cond.Broadcast()
	return true
}

// outcomeLocked renders a tag's first kernel error as an errno.
func (o *outcomes) outcomeLocked(tag uint32) error {
	if e := o.res[tag]; e != 0 {
		return syscall.Errno(-e)
	}
	return nil
}
