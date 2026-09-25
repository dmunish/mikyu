package node

import (
	"context"
	"log/slog"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/retention"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
)

// checkpointSweepInterval is how often the node checkpoints dirty task queues.
const checkpointSweepInterval = 5 * time.Second

// logEntry is a node-owned log driver; ready closes once the install finishes
// and err carries a fault that must not fail the whole node.
type logEntry struct {
	ready chan struct{}
	q     logqueue.Log
	err   error
}

// taskEntry is a node-owned task driver; ready closes once the install finishes
// and err carries a fault that must not fail the whole node.
type taskEntry struct {
	ready chan struct{}
	q     taskqueue.Queue
	err   error
}

// installAll builds a driver for every queue on disk so the catalog mirrors the directory.
func (n *Node) installAll(infos []QueueInfo) {
	for _, info := range infos {
		eff := n.storedSpec(info.Name, info.Kind).Effective(n.defaults, spec.Builtin())
		switch info.Kind {
		case manager.KindLog:
			_, _ = n.installLog(info.Name, eff)
		case manager.KindTask:
			_, _ = n.installTask(info.Name, eff)
		}
	}
}

// installLog adds a log driver to the catalog, building it outside n.mu.
func (n *Node) installLog(name string, eff spec.Spec) (*logEntry, error) {
	e := &logEntry{ready: make(chan struct{})}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, engine.ErrQueueClosed
	}
	n.logCatalog[name] = e
	n.installWG.Add(1)
	n.mu.Unlock()

	storage, err := n.mgr.GetOf(name, manager.KindLog)
	if err == nil {
		e.q = logqueue.New(name, storage)
		n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: e.q})
		n.retention.SetPolicy(name, policyFor(eff))
	}
	n.finishInstall(func() { close(e.ready) })
	if err != nil {
		e.err = err
		n.recordFault(name, err)
		return e, err
	}
	return e, nil
}

// installTask adds a task driver to the catalog, replaying its log outside n.mu.
func (n *Node) installTask(name string, eff spec.Spec) (*taskEntry, error) {
	e := &taskEntry{ready: make(chan struct{})}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, engine.ErrQueueClosed
	}
	n.taskCatalog[name] = e
	n.installWG.Add(1)
	n.mu.Unlock()

	storage, err := n.mgr.GetOf(name, manager.KindTask)
	if err == nil {
		e.q, err = taskqueue.New(name, storage, taskDefaultsFor(eff),
			n.archiveLogFor(), n.metrics.Registry().Scope(name))
	}
	if err == nil {
		n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: e.q})
		n.retention.SetPolicy(name, policyFor(eff))
	}
	n.finishInstall(func() { close(e.ready) })
	if err != nil {
		e.err = err
		n.recordFault(name, err)
		return e, err
	}
	return e, nil
}

// finishInstall releases the install slot and wakes every waiter on the entry.
func (n *Node) finishInstall(wake func()) {
	n.mu.Lock()
	n.installWG.Done()
	n.mu.Unlock()
	wake()
}

// recordFault counts a queue whose driver could not be built and logs why.
func (n *Node) recordFault(name string, err error) {
	n.metrics.Registry().Scope(name).Counter("queue_faults").Inc()
	slog.Warn("queue install failed; queue is faulted", "queue", name, "err", err)
}

// dropLog removes a failed log install so the name can be installed again.
func (n *Node) dropLog(name string, e *logEntry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.logCatalog[name] == e {
		delete(n.logCatalog, name)
	}
}

// logEntryFor returns the installed log driver, or the reason it is unavailable.
func (n *Node) logEntryFor(name string) (*logEntry, error) {
	n.mu.RLock()
	e, installed := n.logCatalog[name]
	closed := n.closed
	n.mu.RUnlock()
	switch {
	case closed:
		return nil, engine.ErrQueueClosed
	case installed:
		<-e.ready
		return e, e.err
	default:
		return nil, n.catalogMiss(name, manager.KindLog)
	}
}

// taskEntryFor returns the installed task driver, or the reason it is unavailable.
func (n *Node) taskEntryFor(name string) (*taskEntry, error) {
	n.mu.RLock()
	e, installed := n.taskCatalog[name]
	closed := n.closed
	n.mu.RUnlock()
	switch {
	case closed:
		return nil, engine.ErrQueueClosed
	case installed:
		<-e.ready
		return e, e.err
	default:
		return nil, n.catalogMiss(name, manager.KindTask)
	}
}

// catalogMiss explains why name has no driver of the requested kind.
func (n *Node) catalogMiss(name string, want manager.Kind) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if want == manager.KindTask {
		if _, isLog := n.logCatalog[name]; isLog {
			return ErrWrongKind
		}
	} else if _, isTask := n.taskCatalog[name]; isTask {
		return ErrWrongKind
	}
	return engine.ErrQueueNotFound
}

// removeCatalog drops a queue from the catalog and stops its task driver.
func (n *Node) removeCatalog(name string) {
	n.mu.Lock()
	task, isTask := n.taskCatalog[name]
	delete(n.taskCatalog, name)
	delete(n.logCatalog, name)
	n.mu.Unlock()
	if isTask {
		<-task.ready
		if task.q != nil {
			_ = taskqueue.Close(task.q)
		}
	}
	n.retention.UnregisterTarget(name)
}

// closeCatalog stops every installed driver and empties the catalog. The caller
// must have waited on installWG, so every entry is already published.
func (n *Node) closeCatalog() {
	n.mu.Lock()
	entries := make([]*taskEntry, 0, len(n.taskCatalog))
	for _, e := range n.taskCatalog {
		entries = append(entries, e)
	}
	clear(n.taskCatalog)
	clear(n.logCatalog)
	n.mu.Unlock()

	var firstErr error
	for _, e := range entries {
		if e.q == nil {
			continue
		}
		if err := taskqueue.Close(e.q); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		slog.Warn("task queue close reported an error", "err", firstErr)
	}
}

// storedSpec returns the persisted spec for a queue, or the zero spec.
func (n *Node) storedSpec(name string, kind manager.Kind) spec.Spec {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.registry.Queues[registryKey(kind, name)].Spec
}

// maintenanceLoop checkpoints dirty task queues on a fixed interval.
func (n *Node) maintenanceLoop(ctx context.Context) {
	t := time.NewTicker(checkpointSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.sweepCheckpoints()
		}
	}
}

// sweepCheckpoints checkpoints every installed queue that has unwritten state.
func (n *Node) sweepCheckpoints() {
	n.mu.RLock()
	entries := make([]*taskEntry, 0, len(n.taskCatalog))
	for _, e := range n.taskCatalog {
		entries = append(entries, e)
	}
	n.mu.RUnlock()

	for _, e := range entries {
		select {
		case <-e.ready:
		default:
			continue // install still in flight
		}
		if e.err != nil || e.q == nil {
			continue
		}
		if err := taskqueue.Checkpoint(e.q); err != nil {
			slog.Warn("task queue checkpoint failed", "queue", e.q.Name(), "err", err)
		}
	}
}
