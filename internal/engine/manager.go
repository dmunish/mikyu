package engine

import (
	"maps"
	"os"
	"slices"
	"sync"
)

type Manager struct {
	mu      sync.RWMutex
	dir     string
	root    *os.Root
	cfg     Config
	queues  map[string]*Queue
	offsets *OffsetStore
}

type EngineStats struct {
	ActiveQueues int
	TotalBytes   int64
	Queues       map[string]QueueStats
}

type QueueStats struct {
	MessageCount uint64
	SegmentCount int
	SizeBytes    int64
	LastOffset   uint64
	MinOffset    uint64
}

// OpenManager creates a Manager and loads existing queues.
func OpenManager(dir string, cfg Config, perQueue map[string]Config) (*Manager, error) {
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
		q, err := openQueue(dir, name, qCfg, store)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.queues[name] = q
	}
	return m, nil
}

// CreateQueue creates a new queue and returns it as a Storage.
func (m *Manager) CreateQueue(name string, opts ...Config) (Storage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[name]; exists {
		return nil, ErrQueueExists
	}
	cfg := m.cfg
	if len(opts) > 0 {
		cfg = opts[0]
	}
	q, err := openQueue(m.dir, name, cfg, m.offsets)
	if err != nil {
		return nil, err
	}
	m.queues[name] = q
	return q, nil
}

// GetQueue returns an existing queue as a Storage.
func (m *Manager) GetQueue(name string) (Storage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

// ListQueues returns the names of all active queues.
func (m *Manager) ListQueues() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Sorted(maps.Keys(m.queues))
}

// DeleteQueue closes and removes a queue and its data.
func (m *Manager) DeleteQueue(name string) error {
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

// EngineStats returns aggregate engine statistics.
func (m *Manager) EngineStats() EngineStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := EngineStats{
		ActiveQueues: len(m.queues),
		Queues:       make(map[string]QueueStats, len(m.queues)),
	}

	for name, q := range m.queues {
		q.mu.RLock()
		msgCount := q.publishedOffset
		segCount := len(q.segments)
		lastOff := q.nextOffset
		minOff := q.minOffset

		var size int64
		for _, seg := range q.segments {
			size += seg.pos
		}
		q.mu.RUnlock()

		stats.Queues[name] = QueueStats{
			MessageCount: msgCount,
			SegmentCount: segCount,
			SizeBytes:    size,
			LastOffset:   lastOff,
			MinOffset:    minOff,
		}
		stats.TotalBytes += size
	}

	return stats
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
