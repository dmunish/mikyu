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
	"github.com/dmunish/mikyu/internal/logtap"
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

// Node is the shared local-node core: one data directory, its kind-partitioned queues, the resident queue catalog, and retention lifecycle.
type Node struct {
	mgr         *manager.Manager
	retention   *retention.Coordinator
	logCatalog  map[string]*logEntry
	taskCatalog map[string]*taskEntry
	installWG   sync.WaitGroup
	mu          *metrics.RWLockObserver

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
	keyFile  string
	keys     engine.MasterKeySet
	tap      *logtap.Tap
}

// openNode acquires the directory lock, reconciles the registry against the
// filesystem, and starts the retention reconciler. Callers outside this
// package must go through Acquire.
func openNode(dir string, defaults spec.Spec, ao acquireOptions) (*Node, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	var masters engine.MasterKeySet
	if ao.keyFile != "" {
		masters, err = engine.LoadMasterKeys(ao.keyFile)
		if err != nil {
			_ = lock.release()
			return nil, err
		}
	}
	reg, err := loadRegistry(dir)
	if err != nil {
		_ = lock.release()
		return nil, err
	}

	builtin := spec.Builtin()
	perQueue := make(map[manager.Key]engine.Config, len(reg.Queues))
	for _, entry := range reg.Queues {
		cfg, err := toEngineConfigWithKeys(entry.Spec.Effective(defaults, builtin), masters, entry.Name)
		if err != nil {
			_ = lock.release()
			return nil, err
		}
		cfg.GateCache = entry.Kind == manager.KindLog
		perQueue[manager.Key{Name: entry.Name, Kind: entry.Kind}] = cfg
	}
	defaultEngineCfg, err := toEngineConfigWithKeys(defaults.Merge(builtin), masters, "session defaults")
	if err != nil {
		_ = lock.release()
		return nil, err
	}

	mreg := newRegistry(ao.metrics)
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
	nodeLock := metrics.NewRWLockObserver("node.mu", nil)
	metrics.RegisterLockObserver(nodeLock)
	n := &Node{
		mgr:             mgr,
		retention:       retention.NewCoordinator(0),
		logCatalog:      make(map[string]*logEntry),
		taskCatalog:     make(map[string]*taskEntry),
		mu:              nodeLock,
		retentionCtx:    ctx,
		retentionCancel: cancel,
		dir:             dir,
		defaults:        defaults,
		registry:        reg,
		lock:            lock,
		openedAt:        time.Now(),
		keyFile:         ao.keyFile,
		keys:            masters,
		tap:             logtap.New(logtap.DefaultCapacity),
	}
	n.metrics = newCollector(mreg, ao.metrics, newRuntimeSource(), mgr, configSource{n})
	preregisterServerMetrics(mreg)
	n.installAll(infos)
	n.retentionWG.Go(func() { n.retention.Start(ctx) })
	n.retentionWG.Go(func() { n.maintenanceLoop(ctx) })
	n.metrics.Start(ctx)
	return n, nil
}

// LogTap returns the node's log ring: the sink every rendered log line in this process can fan into, and the backlog a wire log follower replays.
func (n *Node) LogTap() *logtap.Tap { return n.tap }

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
	var firstErr error

	n.retentionWG.Wait()
	n.reconcileWG.Wait()

	n.metrics.Stop()

	// Wait for installs that started before the closed flag so no driver
	// outlives the manager it was built on.
	n.installWG.Wait()
	n.closeCatalog()

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
	if kind == manager.KindTask && deref(eff.Storage.Compaction, spec.CompactionNone) == spec.CompactionKeys {
		return nil, spec.Spec{}, fmt.Errorf("%w: compaction is a log-queue setting", spec.ErrInvalidConfig)
	}
	cfg, err := toEngineConfigWithKeys(eff, n.keys, name)
	if err != nil {
		return nil, spec.Spec{}, err
	}
	cfg.GateCache = kind == manager.KindLog
	storage, err := n.mgr.Create(ctx, name, kind, cfg)
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
	_, eff, err := n.createQueue(ctx, name, manager.KindLog, s)
	if err != nil {
		return nil, err
	}
	e, err := n.installLog(name, eff)
	if err != nil {
		return nil, err
	}
	return e.q, nil
}

// CreateTaskQueue creates a task queue and returns its handle.
func (n *Node) CreateTaskQueue(ctx context.Context, name string, s spec.Spec) (taskqueue.Queue, error) {
	_, eff, err := n.createQueue(ctx, name, manager.KindTask, s)
	if err != nil {
		return nil, err
	}
	e, err := n.installTask(name, eff)
	if err != nil {
		return nil, err
	}
	return e.q, nil
}

// archiveAppender adapts a log queue to the task driver's archive writer.
type archiveAppender struct {
	l logqueue.Log
}

func (a archiveAppender) Append(ctx context.Context, payload []byte) (uint64, error) {
	return a.l.Append(ctx, payload)
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
			return archiveAppender{lq}, nil
		case err == nil && kind != manager.KindLog:
			return nil, fmt.Errorf("%w: %q exists as a %s queue", ErrWrongKind, name, kind)
		case errors.Is(err, engine.ErrQueueNotFound):
			lq, err := n.CreateLogQueue(ctx, name, spec.Spec{})
			if err != nil {
				return nil, err
			}
			return archiveAppender{lq}, nil
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
	if err := spec.ValidateCreate(merged); err != nil {
		return err
	}
	eff := merged.Effective(n.defaults, spec.Builtin())
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
	case manager.KindTask:
		if e, ok := n.taskCatalog[name]; ok {
			<-e.ready
			if e.err == nil && e.q != nil {
				e.q.SetDefaults(taskDefaultsFor(eff))
			}
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

// QueueDescription is the full truth about one queue: its kind, stored spec, derived spec, effective spec, and the session defaults it inherits.
type QueueDescription struct {
	Name      string       `json:"name"`
	Kind      manager.Kind `json:"kind"`
	Spec      spec.Spec    `json:"spec"`
	Derived   spec.Spec    `json:"derived"`
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
		Derived:   spec.Resolve(stored),
		Effective: stored.Effective(n.defaults, builtin),
		Defaults:  n.defaults,
	}, nil
}

// wrongKind builds the error returned when a queue is opened as the wrong kind.
func wrongKind(name string, kind manager.Kind, want string) error {
	return fmt.Errorf("%w: %q is a %s queue; a %s queue is required", ErrWrongKind, name, kind, want)
}

// LogQueue returns the installed log-queue driver for name, or ErrWrongKind when the directory is a task queue.
func (n *Node) LogQueue(name string) (logqueue.Log, error) {
	e, err := n.logEntryFor(name)
	if err != nil {
		if errors.Is(err, ErrWrongKind) {
			return nil, wrongKind(name, manager.KindTask, "log")
		}
		return nil, err
	}
	return e.q, nil
}

// TaskQueue returns the installed task-queue driver for name, or ErrWrongKind when the directory is a log queue.
func (n *Node) TaskQueue(name string) (taskqueue.Queue, error) {
	e, err := n.taskEntryFor(name)
	if err != nil {
		if errors.Is(err, ErrWrongKind) {
			return nil, wrongKind(name, manager.KindLog, "task")
		}
		return nil, err
	}
	return e.q, nil
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
	n.removeCatalog(name)
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

// taskDefaultsFor converts a resolved spec's task block into driver defaults.
func taskDefaultsFor(s spec.Spec) taskqueue.Defaults {
	return taskqueue.Defaults{
		Lease:          s.Task.Lease.Duration(),
		MaxAttempts:    deref(s.Task.MaxAttempts, 5),
		MaxInFlight:    deref(s.Task.MaxInFlight, 100_000),
		MaxDeadLetters: deref(s.Task.MaxDeadLetters, 100_000),
	}
}

// policyFor converts a resolved spec's retention block into a coordinator policy.
func policyFor(s spec.Spec) retention.Policy {
	return retention.Policy{
		Age:   s.Retention.MaxAge.Duration(),
		Bytes: deref(s.Retention.MaxBytes, 0),
	}
}

// toEngineConfig converts a resolved spec into an engine.Config.
func toEngineConfig(s spec.Spec) engine.Config {
	return engine.Config{
		Durability:    deref(s.Storage.Durability, spec.DurabilitySync),
		Encryption:    deref(s.Storage.Encryption, spec.EncryptionNone),
		Compaction:    deref(s.Storage.Compaction, spec.CompactionNone),
		MaxSegment:    deref(s.Storage.MaxSegment, 64<<20),
		MaxSegmentAge: s.Storage.MaxSegmentAge.Duration(),
		MaxPayload:    deref(s.Storage.MaxPayload, 1<<20),
		MaxCache:      deref(s.Storage.MaxCache, 32<<20),
		MaxFlushBytes: deref(s.Flush.MaxBytes, 1<<20),
		MaxFlushWait:  s.Flush.MaxWait.Duration(),
		FlushLinger:   spec.FlushLinger(s),
	}
}

// toEngineConfigWithKeys converts a resolved spec and fails closed when the
// queue seals payloads but the node holds no master keys.
func toEngineConfigWithKeys(s spec.Spec, masters engine.MasterKeySet, name string) (engine.Config, error) {
	cfg := toEngineConfig(s)
	if !cfg.EncryptedPayloads() {
		return cfg, nil
	}
	if len(masters) == 0 {
		return engine.Config{}, fmt.Errorf("%w: queue %q wants %q payloads but no encryption_key_file is loaded", engine.ErrEncryptionKey, name, spec.EncryptionAES)
	}
	cfg.MasterKeys = masters
	return cfg, nil
}

func deref[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}