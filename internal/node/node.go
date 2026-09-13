// Package node owns the local queue registry shared by embedded clients and the daemon server.
package node

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/retention"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
)

var errNodeClosed = errors.New("node is closed")

// Node is the shared local-node core: one data directory, its queue registry, cached drivers, and retention lifecycle.
type Node struct {
	mgr        *engine.Manager
	retention  *retention.Coordinator
	logQueues  map[string]*logqueue.LogQueue
	taskQueues map[string]*taskqueue.TaskQueue
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
}

// openNode acquires the directory lock, loads the registry, and starts the
// retention reconciler. Callers outside this package must go through Acquire.
func openNode(dir string, defaults spec.Spec) (*Node, error) {
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
	if err := reconcileRegistry(dir, reg); err != nil {
		_ = lock.release()
		return nil, err
	}

	builtin := spec.Builtin()
	perQueue := make(map[string]engine.Config, len(reg.Queues))
	for name, s := range reg.Queues {
		perQueue[name] = toEngineConfig(s.Effective(defaults, builtin))
	}
	defaultEngineCfg := toEngineConfig(defaults.Merge(builtin))

	mgr, err := engine.OpenManager(dir, defaultEngineCfg, perQueue)
	if err != nil {
		_ = lock.release()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		mgr:             mgr,
		retention:       retention.NewCoordinator(0),
		logQueues:       make(map[string]*logqueue.LogQueue),
		taskQueues:      make(map[string]*taskqueue.TaskQueue),
		retentionCtx:    ctx,
		retentionCancel: cancel,
		dir:             dir,
		defaults:        defaults,
		registry:        reg,
		lock:            lock,
		openedAt:        time.Now(),
	}
	n.retentionWG.Go(func() { n.retention.Start(ctx) })
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

	n.mu.Lock()
	var firstErr error
	for name, tq := range n.taskQueues {
		if err := tq.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(n.taskQueues, name)
	}
	n.mu.Unlock()

	if err := n.mgr.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := n.lock.release(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Manager returns the underlying engine manager.
func (n *Node) Manager() *engine.Manager { return n.mgr }

// CreateQueue validates spec, opens the engine queue, and persists the spec to the registry.
func (n *Node) CreateQueue(ctx context.Context, name string, s spec.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := spec.ValidateCreate(s); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return errNodeClosed
	}
	if _, ok := n.registry.Queues[name]; ok {
		return engine.ErrQueueExists
	}

	eff := s.Effective(n.defaults, spec.Builtin())
	storage, err := n.mgr.CreateQueue(name, toEngineConfig(eff))
	if err != nil {
		return err
	}

	n.registry.Queues[name] = s
	if err := saveRegistry(n.dir, n.registry); err != nil {
		delete(n.registry.Queues, name)
		_ = n.mgr.DeleteQueue(name)
		return err
	}

	lq := logqueue.NewLogQueue(name, storage)
	n.logQueues[name] = lq
	n.retention.RegisterDriver(name, storage, lq)
	n.retention.SetPolicy(name, policyFor(eff))
	return nil
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

	current, ok := n.registry.Queues[name]
	if !ok {
		if _, err := n.mgr.GetQueue(name); err != nil {
			return engine.ErrQueueNotFound
		}
		// Orphan directory: adopt it, then update.
		current = spec.Spec{}
	}
	// The patch is the more specific layer: it overrides the stored spec and
	// the stored spec fills whatever the patch leaves unset.
	merged := patch.Merge(current)
	n.registry.Queues[name] = merged
	if err := saveRegistry(n.dir, n.registry); err != nil {
		n.registry.Queues[name] = current
		return err
	}

	storage, err := n.mgr.GetQueue(name)
	if err != nil {
		return err
	}
	eff := merged.Effective(n.defaults, spec.Builtin())
	if ac, ok := storage.(interface{ ApplyConfig(engine.Config) }); ok {
		ac.ApplyConfig(toEngineConfig(eff))
	}

	// Keep the cached driver and the retention target in sync. This also
	// adopts orphan queues whose directories UpdateQueue found on disk.
	lq := n.logQueues[name]
	if lq == nil {
		lq = logqueue.NewLogQueue(name, storage)
		n.logQueues[name] = lq
	}
	n.retention.RegisterDriver(name, storage, lq)

	// Live task-queue defaults: a cached handle observes the new spec.
	if tq, ok := n.taskQueues[name]; ok {
		tq.SetDefaults(taskDefaultsFor(eff))
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

// QueueDescription is the full truth about one queue: its stored spec, its effective spec (stored over defaults over built-in), the session defaults it inherits from, and its current storage stats.
type QueueDescription struct {
	Spec       spec.Spec  `json:"spec"`
	Effective  spec.Spec  `json:"effective"`
	Defaults   spec.Spec  `json:"defaults"`
	QueueStats QueueStats `json:"stats"`
}

// QueueStats is a per-queue storage snapshot.
type QueueStats struct {
	MessageCount uint64 `json:"message_count"`
	SegmentCount int    `json:"segment_count"`
	SizeBytes    int64  `json:"size_bytes"`
	LastOffset   uint64 `json:"last_offset"`
	MinOffset    uint64 `json:"min_offset"`
}

// DescribeQueue returns the stored spec, effective spec, defaults, and stats.
func (n *Node) DescribeQueue(ctx context.Context, name string) (QueueDescription, error) {
	if err := ctx.Err(); err != nil {
		return QueueDescription{}, err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return QueueDescription{}, errNodeClosed
	}
	if _, err := n.mgr.GetQueue(name); err != nil {
		return QueueDescription{}, err
	}

	builtin := spec.Builtin()
	stored := n.registry.Queues[name]
	stats := n.mgr.EngineStats().Queues[name]
	return QueueDescription{
		Spec:      stored,
		Effective: stored.Effective(n.defaults, builtin),
		Defaults:  n.defaults,
		QueueStats: QueueStats{
			MessageCount: stats.MessageCount,
			SegmentCount: stats.SegmentCount,
			SizeBytes:    stats.SizeBytes,
			LastOffset:   stats.LastOffset,
			MinOffset:    stats.MinOffset,
		},
	}, nil
}

// Info is a snapshot of the node's identity and aggregate storage state.
type Info struct {
	Version      string                `json:"version"`
	UptimeNs     int64                 `json:"uptime_ns"`
	DataDir      string                `json:"data_dir"`
	ActiveQueues int                   `json:"active_queues"`
	TotalBytes   int64                 `json:"total_bytes"`
	Queues       map[string]QueueStats `json:"queues"`
}

// Info returns a snapshot of the node's identity and aggregate storage state.
func (n *Node) Info(ctx context.Context) (Info, error) {
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return Info{}, errNodeClosed
	}
	dir := n.dir
	openedAt := n.openedAt
	n.mu.RUnlock()

	s := n.mgr.EngineStats()
	queues := make(map[string]QueueStats, len(s.Queues))
	for name, qs := range s.Queues {
		queues[name] = QueueStats{
			MessageCount: qs.MessageCount,
			SegmentCount: qs.SegmentCount,
			SizeBytes:    qs.SizeBytes,
			LastOffset:   qs.LastOffset,
			MinOffset:    qs.MinOffset,
		}
	}
	return Info{
		Version:      versionString(),
		UptimeNs:     time.Since(openedAt).Nanoseconds(),
		DataDir:      dir,
		ActiveQueues: s.ActiveQueues,
		TotalBytes:   s.TotalBytes,
		Queues:       queues,
	}, nil
}

// LogQueue returns the cached log-queue driver for name, materializing it on first use and registering its retention driver.
func (n *Node) LogQueue(name string) (*logqueue.LogQueue, error) {
	n.mu.RLock()
	if lq, ok := n.logQueues[name]; ok {
		n.mu.RUnlock()
		return lq, nil
	}
	n.mu.RUnlock()

	n.mu.Lock()
	defer n.mu.Unlock()
	if lq, ok := n.logQueues[name]; ok {
		return lq, nil
	}
	storage, err := n.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}
	lq := logqueue.NewLogQueue(name, storage)
	n.logQueues[name] = lq
	n.retention.RegisterDriver(name, storage, lq)
	return lq, nil
}

// TaskQueue returns the cached task-queue driver for name, materializing it on first use (recovering any durable task state) and registering its retention driver.
func (n *Node) TaskQueue(name string) (*taskqueue.TaskQueue, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if tq, ok := n.taskQueues[name]; ok {
		return tq, nil
	}
	storage, err := n.mgr.GetQueue(name)
	if err != nil {
		return nil, err
	}
	var d taskqueue.Defaults
	if stored, known := n.registry.Queues[name]; known {
		d = taskDefaultsFor(stored.Effective(n.defaults, spec.Builtin()))
	}
	tq, err := taskqueue.NewTaskQueue(name, storage, d)
	if err != nil {
		return nil, err
	}
	n.taskQueues[name] = tq
	n.retention.RegisterDriver(name, storage, tq)
	return tq, nil
}

// DeleteQueue removes the queue from storage and from the registry.
func (n *Node) DeleteQueue(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	if tq, ok := n.taskQueues[name]; ok {
		_ = tq.Close()
		delete(n.taskQueues, name)
	}
	delete(n.logQueues, name)
	n.mu.Unlock()
	n.retention.UnregisterTarget(name)

	if err := n.mgr.DeleteQueue(name); err != nil {
		return err
	}
	n.mu.Lock()
	delete(n.registry.Queues, name)
	err := saveRegistry(n.dir, n.registry)
	n.mu.Unlock()
	return err
}

// Truncate truncates the queue's log segments before minOffset, limited by the retention floor that all registered drivers agree is safe.
func (n *Node) Truncate(ctx context.Context, name string, minOffset uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if _, err := n.LogQueue(name); err != nil {
		return 0, err
	}
	if _, err := n.TaskQueue(name); err != nil {
		return 0, err
	}
	return n.retention.ReconcileQueueAt(ctx, name, minOffset)
}

// ListQueues returns the names of all queues in the node's data directory.
func (n *Node) ListQueues(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return n.mgr.ListQueues(), nil
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

func versionString() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "devel"
}
