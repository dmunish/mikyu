package engine

import (
	"os"
	"path/filepath"
	"sync"
)

type Manager struct {
	mu      sync.RWMutex
	dir     string
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
}

// OpenManager creates a Manager and loads existing queues.
func OpenManager(dir string, cfg Config) (*Manager, error) {
	cfg = cfg.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if cfg.Encrypted {
		_ = EnsureDirectoryEncrypted(dir)
	}
	store, err := openOffsetStore(dir)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		dir:     dir,
		cfg:     cfg,
		queues:  make(map[string]*Queue),
		offsets: store,
	}

	// Load existing queue subdirectories.
	entries, err := os.ReadDir(dir)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		q, err := openQueue(dir, name, cfg, store)
		if err != nil {
			_ = m.Close() // Clean up open queues and offset store on failure
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

	names := make([]string, 0, len(m.queues))
	for name := range m.queues {
		names = append(names, name)
	}
	return names
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
	return os.RemoveAll(filepath.Join(m.dir, name))
}

// Stats returns aggregate engine statistics.
func (m *Manager) Stats() EngineStats {
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
	if m.offsets != nil {
		if err := m.offsets.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
