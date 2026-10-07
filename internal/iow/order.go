package iow

import "sync"

// completion is one finished batch awaiting in-order delivery.
type completion struct {
	err  error
	done func(error)
}

// order turns out-of-order batch completions into submission-order callbacks,
// so a caller can advance a monotonic durable watermark without a reorder
// buffer of its own.
//
// The frontier advances only after a callback has been invoked, so no later
// batch's callback can start before an earlier one's.
type order struct {
	mu        sync.Mutex
	cond      *sync.Cond
	reserved  uint64
	delivered uint64
	pending   map[uint64]completion
}

func newOrder() *order {
	o := &order{pending: make(map[uint64]completion)}
	o.cond = sync.NewCond(&o.mu)
	return o
}

// reserve takes the next delivery slot for a submission about to be made.
func (o *order) reserve() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	seq := o.reserved
	o.reserved++
	return seq
}

// settle records one outcome and delivers every callback the frontier has reached.
func (o *order) settle(seq uint64, err error, done func(error)) {
	o.mu.Lock()
	o.pending[seq] = completion{err: err, done: done}
	o.cond.Broadcast()
	o.mu.Unlock()
	o.pump()
}

// pump hands the frontier's completion to its callback before advancing, so
// invocations never overtake the frontier.
func (o *order) pump() {
	for {
		o.mu.Lock()
		c, ok := o.pending[o.delivered]
		if !ok {
			o.mu.Unlock()
			return
		}
		delete(o.pending, o.delivered)
		o.mu.Unlock()

		if c.done != nil {
			c.done(c.err)
		}

		o.mu.Lock()
		o.delivered++
		o.cond.Broadcast()
		o.mu.Unlock()
	}
}

// waitDrained parks until every reserved slot has been delivered.
func (o *order) waitDrained() {
	o.mu.Lock()
	for o.delivered < o.reserved {
		o.cond.Wait()
	}
	o.mu.Unlock()
}
