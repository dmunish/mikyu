package manager

import (
	"cmp"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
)

// Manager owns a data directory: its log/ and task/ storage roots and the queues inside them.
type Manager struct {
	dir  string
	log  *engine.Manager
	task *engine.Manager
}

// Open creates the data directory and opens both kind roots, applying per-queue configs.
func Open(dir string, defaultCfg engine.Config, perQueue map[Key]engine.Config, regs ...*metrics.Registry) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	logCfg := make(map[string]engine.Config)
	taskCfg := make(map[string]engine.Config)
	for key, cfg := range perQueue {
		switch key.Kind {
		case KindLog:
			logCfg[key.Name] = cfg
		case KindTask:
			taskCfg[key.Name] = cfg
		}
	}
	logMgr, err := engine.OpenManager(filepath.Join(dir, KindLog.String()), defaultCfg, logCfg, regs...)
	if err != nil {
		return nil, err
	}
	taskMgr, err := engine.OpenManager(filepath.Join(dir, KindTask.String()), defaultCfg, taskCfg, regs...)
	if err != nil {
		_ = logMgr.Close()
		return nil, err
	}
	return &Manager{dir: dir, log: logMgr, task: taskMgr}, nil
}

// Dir returns the data directory the manager owns.
func (m *Manager) Dir() string { return m.dir }

// engineFor returns the storage root for a kind, or ErrWrongKind.
func (m *Manager) engineFor(kind Kind) (*engine.Manager, error) {
	switch kind {
	case KindLog:
		return m.log, nil
	case KindTask:
		return m.task, nil
	default:
		return nil, ErrWrongKind
	}
}

// Get returns the storage for name and the kind its directory declares, checking log/ before task/.
func (m *Manager) Get(name string) (engine.Storage, Kind, error) {
	storage, err := m.log.Get(name)
	if err == nil {
		return storage, KindLog, nil
	}
	if !errors.Is(err, engine.ErrQueueNotFound) {
		return nil, 0, err
	}
	storage, err = m.task.Get(name)
	if err == nil {
		return storage, KindTask, nil
	}
	if !errors.Is(err, engine.ErrQueueNotFound) {
		return nil, 0, err
	}
	return nil, 0, engine.ErrQueueNotFound
}

// GetOf returns the storage for name under kind, or ErrWrongKind when the directory belongs to the other kind.
func (m *Manager) GetOf(name string, kind Kind) (engine.Storage, error) {
	root, err := m.engineFor(kind)
	if err != nil {
		return nil, err
	}
	storage, err := root.Get(name)
	if errors.Is(err, engine.ErrQueueNotFound) {
		if _, otherKind, otherErr := m.Get(name); otherErr == nil && otherKind != kind {
			return nil, ErrWrongKind
		}
	}
	return storage, err
}

// Create opens a new queue of kind under its kind root and returns its storage.
func (m *Manager) Create(ctx context.Context, name string, kind Kind, cfg engine.Config) (engine.Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := m.engineFor(kind)
	if err != nil {
		return nil, err
	}
	return root.Create(name, cfg)
}

// Delete closes and removes the queue name, resolving its kind from the filesystem.
func (m *Manager) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, kind, err := m.Get(name)
	if err != nil {
		return err
	}
	root, err := m.engineFor(kind)
	if err != nil {
		return err
	}
	return root.Delete(name)
}

// List returns every queue on disk, sorted by name and then kind.
func (m *Manager) List(ctx context.Context) ([]QueueInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logNames := m.log.List()
	taskNames := m.task.List()
	out := make([]QueueInfo, 0, len(logNames)+len(taskNames))
	for _, name := range logNames {
		out = append(out, QueueInfo{Name: name, Kind: KindLog})
	}
	for _, name := range taskNames {
		out = append(out, QueueInfo{Name: name, Kind: KindTask})
	}
	slices.SortFunc(out, func(a, b QueueInfo) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return cmp.Compare(a.Kind, b.Kind)
	})
	return out, nil
}

// Collect fills every queue's gauges and labels each with its kind.
func (m *Manager) Collect(ctx context.Context, dst *metrics.Snapshot) {
	start := len(dst.Queues)
	m.log.Collect(ctx, dst)
	logEnd := len(dst.Queues)
	m.task.Collect(ctx, dst)
	for i := start; i < logEnd; i++ {
		dst.Queues[i].Kind = KindLog.String()
	}
	for i := logEnd; i < len(dst.Queues); i++ {
		dst.Queues[i].Kind = KindTask.String()
	}
	dst.Storage.DataDir = m.dir
}

// Close shuts down the task root first (final checkpoints), then the log root.
func (m *Manager) Close() error {
	taskErr := m.task.Close()
	logErr := m.log.Close()
	if taskErr != nil {
		return taskErr
	}
	return logErr
}
