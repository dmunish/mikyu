// Package node owns the local queue registry shared by embedded clients and the daemon server.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/retention"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
)

var errNodeClosed = errors.New("node is closed")

// Shutdown contract: closeInternal stops new operations, wakes every parked
// waiter with ErrQueueClosed, drains each task checkpoint, closes every queue
// (final WAL sync and active-segment truncate), and releases the directory lock.

// Node is the shared local-node core: one data directory, its kind-partitioned queues, cached drivers, and retention lifecycle.
type Node struct {
	mgr        *manager.Manager
	retention  *retention.Coordinator
	logQueues  map[string]logqueue.Log
	taskQueues map[string]taskqueue.Queue
	mu         sync.RWMutex

	retentionCtx    context.Context
	retentionCancel context.CancelFunc
	retentionWG     sync.WaitGroup
	reconcileWG     sync.WaitGroup
	closed          bool

	dir      string
	defaults spec.Spec
	registry *registry
	lock     *dirLock
	openedAt time.Time
	metrics  *metrics.Collector
}

// openNode acquires the directory lock, reconciles the registry against the
// filesystem, and starts the retention reconciler. Callers outside this
// package must go through Acquire.
func openNode(dir string, defaults spec.Spec, mc MetricsConfig) (*Node, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	reg, err := loadRegistry(dir)
	if err != nil {
		_ = lock.release()
		return nil, err
	}

	builtin := spec.Builtin()
	perQueue := make(map[manager.Key]engine.Config, len(reg.Queues))
	for _, entry := range reg.Queues {
		perQueue[manager.Key{Name: entry.Name, Kind: entry.Kind}] = toEngineConfig(entry.Spec.Effective(defaults, builtin))
	}
	defaultEngineCfg := toEngineConfig(defaults.Merge(builtin))

	mreg := newRegistry(mc)
	mgr, err := manager.Open(dir, defaultEngineCfg, perQueue, mreg)
	if err != nil {
		_ = lock.release()
		return nil, err
	}
	infos, err := mgr.List(context.Background())
	if err != nil {
		_ = mgr.Close()
		_ = lock.release()
		return nil, err
	}
	if reconcileRegistry(reg, infos) {
		if err := saveRegistry(dir, reg); err != nil {
			_ = mgr.Close()
			_ = lock.release()
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		mgr:             mgr,
		retention:       retention.NewCoordinator(0),
		logQueues:       make(map[string]logqueue.Log),
		taskQueues:      make(map[string]taskqueue.Queue),
		retentionCtx:    ctx,
		retentionCancel: cancel,
		dir:             dir,
		defaults:        defaults,
		registry:        reg,
		lock:            lock,
		openedAt:        time.Now(),
		metrics:         newCollector(mreg, mc, NewRuntimeSource(), mgr),
	}
	preregisterServerMetrics(mreg)
	n.retentionWG.Go(func() { n.retention.Start(ctx) })
	n.metrics.Start(ctx)
	return n, nil
}

// closeInternal releases the manager, cached drivers, lock, and reconciler.
func (n *Node) closeInternal() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.retentionCancel()
	n.mu.Unlock()

	n.retentionWG.Wait()
	n.reconcileWG.Wait()

	n.metrics.Stop()

	n.mu.Lock()
	var firstErr error
	for name, tq := range n.taskQueues {
		if err := taskqueue.Close(tq); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(n.taskQueues, name)
	}
	clear(n.logQueues)
	n.mu.Unlock()

	if err := n.mgr.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := n.lock.release(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Manager returns the underlying manager.
func (n *Node) Manager() *manager.Manager { return n.mgr }

// createQueue validates a spec, opens the queue under its kind root, and persists the spec.
func (n *Node) createQueue(ctx context.Context, name string, kind manager.Kind, s spec.Spec) (engine.Storage, spec.Spec, error) {
	if err := ctx.Err(); err != nil {
		return nil, spec.Spec{}, err
	}
	if err := spec.ValidateCreate(s); err != nil {
		return nil, spec.Spec{}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, spec.Spec{}, errNodeClosed
	}
	key := registryKey(kind, name)
	if _, ok := n.registry.Queues[key]; ok {
		return nil, spec.Spec{}, engine.ErrQueueExists
	}

	eff := s.Effective(n.defaults, spec.Builtin())
	if err := spec.ValidateCreate(eff); err != nil {
		return nil, spec.Spec{}, err
	}
	storage, err := n.mgr.Create(ctx, name, kind, toEngineConfig(eff))
	if err != nil {
		return nil, spec.Spec{}, err
	}

	n.registry.Queues[key] = QueueEntry{Name: name, Kind: kind, Spec: s}
	if err := saveRegistry(n.dir, n.registry); err != nil {
		delete(n.registry.Queues, key)
		_ = n.mgr.Delete(ctx, name)
		return nil, spec.Spec{}, err
	}
	return storage, eff, nil
}

// CreateLogQueue creates a log queue and returns its handle.
func (n *Node) CreateLogQueue(ctx context.Context, name string, s spec.Spec) (logqueue.Log, error) {
	storage, eff, err := n.createQueue(ctx, name, manager.KindLog, s)
	if err != nil {
		return nil, err
	}
	lq := logqueue.New(name, storage)
	n.mu.Lock()
	n.logQueues[name] = lq
	n.mu.Unlock()
	n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: lq})
	n.retention.SetPolicy(name, policyFor(eff))
	return lq, nil
}

// CreateTaskQueue creates a task queue and returns its handle.
func (n *Node) CreateTaskQueue(ctx context.Context, name string, s spec.Spec) (taskqueue.Queue, error) {
	storage, eff, err := n.createQueue(ctx, name, manager.KindTask, s)
	if err != nil {
		return nil, err
	}
	tq, err := taskqueue.New(name, storage, taskDefaultsFor(eff), n.archiveLogFor(), n.metrics.Registry().Scope(name))
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	n.taskQueues[name] = tq
	n.mu.Unlock()
	n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: tq})
	return tq, nil
}

// archiveLogFor returns a factory that lazily creates or returns the archive log queue for a source queue name.
func (n *Node) archiveLogFor() taskqueue.ArchiveLogFor {
	return func(ctx context.Context, source string) (taskqueue.ArchiveAppender, error) {
		name := source + taskqueue.ArchivePrefix
		_, kind, err := n.mgr.Get(name)
		switch {
		case err == nil && kind == manager.KindLog:
			lq, err := n.LogQueue(name)
			if err != nil {
				return nil, err
			}
			return lq, nil
		case err == nil && kind != manager.KindLog:
			return nil, fmt.Errorf("%w: %q exists as a %s queue", ErrWrongKind, name, kind)
		case errors.Is(err, engine.ErrQueueNotFound):
			return n.CreateLogQueue(ctx, name, spec.Spec{})
		default:
			return nil, err
		}
	}
}

// UpdateQueue merges patch into the queue's stored spec, persists the result, and applies the mutable fields to the live engine queue.
func (n *Node) UpdateQueue(ctx context.Context, name string, patch spec.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := spec.ValidateUpdate(patch); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return errNodeClosed
	}

	_, kind, err := n.mgr.Get(name)
	if err != nil {
		return engine.ErrQueueNotFound
	}
	key := registryKey(kind, name)
	current := n.registry.Queues[key].Spec
	// The patch is the more specific layer: it overrides the stored spec and
	// the stored spec fills whatever the patch leaves unset.
	merged := patch.Merge(current)
	eff := merged.Effective(n.defaults, spec.Builtin())
	if err := spec.ValidateCreate(eff); err != nil {
		return err
	}
	n.registry.Queues[key] = QueueEntry{Name: name, Kind: kind, Spec: merged}
	if err := saveRegistry(n.dir, n.registry); err != nil {
		n.registry.Queues[key] = QueueEntry{Name: name, Kind: kind, Spec: current}
		return err
	}

	storage, err := n.mgr.GetOf(name, kind)
	if err != nil {
		return err
	}
	if ac, ok := storage.(interface{ ApplyConfig(engine.Config) }); ok {
		ac.ApplyConfig(toEngineConfig(eff))
	}

	switch kind {
	case manager.KindLog:
		lq := n.logQueues[name]
		if lq == nil {
			lq = logqueue.New(name, storage)
			n.logQueues[name] = lq
		}
		n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: lq})
	case manager.KindTask:
		if tq, ok := n.taskQueues[name]; ok {
			tq.SetDefaults(taskDefaultsFor(eff))
		}
	}

	if n.retention.SetPolicy(name, policyFor(eff)) {
		// A tightened bound may expose data the periodic tick has not reached
		// yet; reconcile immediately, bound to the node's lifetime.
		n.reconcileWG.Go(func() {
			cctx, cancel := context.WithTimeout(n.retentionCtx, 5*time.Minute)
			defer cancel()
			if _, err := n.retention.ReconcileQueue(cctx, name); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("retention: immediate reconcile after policy shrink", "queue", name, "err", err)
			}
		})
	}
	return nil
}

// QueueDescription is the full truth about one queue: its kind, stored spec, effective spec, and the session defaults it inherits.
type QueueDescription struct {
	Name      string       `json:"name"`
	Kind      manager.Kind `json:"kind"`
	Spec      spec.Spec    `json:"spec"`
	Effective spec.Spec    `json:"effective"`
	Defaults  spec.Spec    `json:"defaults"`
}

// DescribeQueue returns the kind, stored spec, effective spec, and session defaults for one queue.
func (n *Node) DescribeQueue(ctx context.Context, name string) (QueueDescription, error) {
	if err := ctx.Err(); err != nil {
		return QueueDescription{}, err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return QueueDescription{}, errNodeClosed
	}
	_, kind, err := n.mgr.Get(name)
	if err != nil {
		return QueueDescription{}, err
	}
	builtin := spec.Builtin()
	stored := n.registry.Queues[registryKey(kind, name)].Spec
	return QueueDescription{
		Name:      name,
		Kind:      kind,
		Spec:      stored,
		Effective: stored.Effective(n.defaults, builtin),
		Defaults:  n.defaults,
	}, nil
}

// LogQueue returns the cached log-queue driver for name, or ErrWrongKind when the directory is a task queue.
func (n *Node) LogQueue(name string) (logqueue.Log, error) {
	n.mu.RLock()
	if lq, ok := n.logQueues[name]; ok {
		n.mu.RUnlock()
		return lq, nil
	}
	n.mu.RUnlock()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, engine.ErrQueueClosed
	}
	if lq, ok := n.logQueues[name]; ok {
		return lq, nil
	}
	storage, kind, err := n.mgr.Get(name)
	if err != nil {
		return nil, err
	}
	if kind != manager.KindLog {
		return nil, fmt.Errorf("%w: %q is a %s queue; a log queue is required", ErrWrongKind, name, kind)
	}
	lq := logqueue.New(name, storage)
	n.logQueues[name] = lq
	n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: lq})
	return lq, nil
}

// TaskQueue returns the cached task-queue driver for name, or ErrWrongKind when the directory is a log queue.
func (n *Node) TaskQueue(name string) (taskqueue.Queue, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, engine.ErrQueueClosed
	}
	if tq, ok := n.taskQueues[name]; ok {
		return tq, nil
	}
	storage, kind, err := n.mgr.Get(name)
	if err != nil {
		return nil, err
	}
	if kind != manager.KindTask {
		return nil, fmt.Errorf("%w: %q is a %s queue; a task queue is required", ErrWrongKind, name, kind)
	}
	var d taskqueue.Defaults
	if entry, ok := n.registry.Queues[registryKey(manager.KindTask, name)]; ok {
		d = taskDefaultsFor(entry.Spec.Effective(n.defaults, spec.Builtin()))
	}
	tq, err := taskqueue.New(name, storage, d, n.archiveLogFor(), n.metrics.Registry().Scope(name))
	if err != nil {
		return nil, err
	}
	n.taskQueues[name] = tq
	n.retention.RegisterTarget(&retention.Target{Name: name, Storage: storage, Driver: tq})
	return tq, nil
}

// DeleteQueue removes the queue from storage and from the registry.
func (n *Node) DeleteQueue(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, kind, err := n.mgr.Get(name)
	if err != nil {
		return err
	}
	n.mu.Lock()
	if kind == manager.KindTask {
		if tq, ok := n.taskQueues[name]; ok {
			_ = taskqueue.Close(tq)
			delete(n.taskQueues, name)
		}
	}
	delete(n.logQueues, name)
	n.mu.Unlock()
	n.retention.UnregisterTarget(name)

	if err := n.mgr.Delete(ctx, name); err != nil {
		return err
	}
	n.mu.Lock()
	delete(n.registry.Queues, registryKey(kind, name))
	err = saveRegistry(n.dir, n.registry)
	n.mu.Unlock()
	return err
}

// Truncate truncates the log queue's segments before minOffset, limited by the retention floor its single driver allows.
func (n *Node) Truncate(ctx context.Context, name string, minOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if _, err := n.LogQueue(name); err != nil {
		return 0, err
	}
	return n.retention.ReconcileAt(ctx, name, minOffset)
}

// ListQueues returns every queue's name and kind, sorted by name and then kind.
func (n *Node) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return n.mgr.List(ctx)
}

// taskDefaultsFor converts an effective spec's task block into driver defaults.
func taskDefaultsFor(s spec.Spec) taskqueue.Defaults {
	var d taskqueue.Defaults
	if s.Task.DefaultVisibility != nil {
		d.Visibility = s.Task.DefaultVisibility.Duration()
	}
	if s.Task.DefaultMaxAttempts != nil {
		d.MaxAttempts = *s.Task.DefaultMaxAttempts
	}
	if s.Task.MaxInFlight != nil {
		d.MaxInFlight = *s.Task.MaxInFlight
	}
	if s.Task.MaxDLQ != nil {
		d.MaxDLQ = *s.Task.MaxDLQ
	}
	return d
}

// policyFor converts a total (effective) spec's retention block into a
// coordinator policy. Nil fields are read as unlimited.
func policyFor(s spec.Spec) retention.Policy {
	var p retention.Policy
	if s.Retention.Age != nil {
		p.Age = s.Retention.Age.Duration()
	}
	if s.Retention.Bytes != nil {
		p.Bytes = *s.Retention.Bytes
	}
	return p
}

// toEngineConfig converts an effective spec into an engine.Config.
func toEngineConfig(s spec.Spec) engine.Config {
	cfg := engine.Config{}
	if s.Engine.BatchSize != nil {
		cfg.BatchSize = *s.Engine.BatchSize
	}
	if s.Engine.MaxInterval != nil {
		cfg.MaxInterval = s.Engine.MaxInterval.Duration()
	}
	if s.Engine.MinInterval != nil {
		cfg.MinInterval = s.Engine.MinInterval.Duration()
	}
	if s.Engine.SegmentSize != nil {
		cfg.SegmentSize = *s.Engine.SegmentSize
	}
	if s.Engine.MaxMessageBytes != nil {
		cfg.MaxMessageBytes = *s.Engine.MaxMessageBytes
	}
	if s.Engine.SegmentAge != nil {
		cfg.SegmentAge = s.Engine.SegmentAge.Duration()
	}
	if s.Engine.Encrypted != nil {
		cfg.Encrypted = *s.Engine.Encrypted
	}
	if s.Engine.Fsync != nil {
		cfg.Fsync = *s.Engine.Fsync
	}
	return cfg
}