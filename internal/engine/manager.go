package engine

import (
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/dmunish/mikyu/internal/metrics"
)

type Manager struct {
	mu      sync.RWMutex
	dir     string
	root    *os.Root
	cfg     Config
	queues  map[string]*Queue
	offsets *OffsetStore
	reg     *metrics.Registry
}

// OpenManager creates a Manager and loads existing queues.
func OpenManager(dir string, cfg Config, perQueue map[string]Config, regs ...*metrics.Registry) (*Manager, error) {
	cfg = cfg.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if cfg.Encrypted {
		_ = EnsureDirectoryEncrypted(dir)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	store, err := openOffsetStore(dir)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	m := &Manager{
		dir:     dir,
		root:    root,
		cfg:     cfg,
		queues:  make(map[string]*Queue),
		offsets: store,
		reg:     scopeRegistry(regs),
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		_ = m.Close()
		return nil, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		qCfg := cfg
		if override, ok := perQueue[name]; ok {
			qCfg = override
		}
		q, err := openQueue(dir, name, qCfg, store, m.scopeFor(name))
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.queues[name] = q
	}
	return m, nil
}

// CreateQueue creates a new queue and returns it as a Storage.
func (m *Manager) Create(name string, opts ...Config) (Storage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[name]; exists {
		return nil, ErrQueueExists
	}
	cfg := m.cfg
	if len(opts) > 0 {
		cfg = opts[0]
	}
	q, err := openQueue(m.dir, name, cfg, m.offsets, m.scopeFor(name))
	if err != nil {
		return nil, err
	}
	m.queues[name] = q
	return q, nil
}

// scopeFor returns the instrumentation scope for a queue, if a registry is set.
func (m *Manager) scopeFor(name string) *metrics.Scope {
	if m.reg == nil {
		return nil
	}
	return m.reg.Scope(name)
}

// scopeRegistry returns the single optional registry, or nil.
func scopeRegistry(regs []*metrics.Registry) *metrics.Registry {
	if len(regs) == 0 {
		return nil
	}
	return regs[0]
}

// GetQueue returns an existing queue as a Storage.
func (m *Manager) Get(name string) (Storage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

// ListQueues returns the names of all active queues.
func (m *Manager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Sorted(maps.Keys(m.queues))
}

// DeleteQueue closes and removes a queue and its data.
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	q, ok := m.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	if err := q.Close(); err != nil {
		return err
	}
	delete(m.queues, name)
	return m.root.RemoveAll(name)
}

// Close shuts down the manager and all its queues cleanly.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, q := range m.queues {
		if err := q.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.queues = make(map[string]*Queue)
	if m.offsets != nil {
		if err := m.offsets.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.offsets = nil
	}
	if m.root != nil {
		if err := m.root.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.root = nil
	}
	return firstErr
}
