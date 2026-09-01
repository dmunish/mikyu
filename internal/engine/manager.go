package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

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

type Manager struct {
	mu      sync.RWMutex
	dir     string
	cfg     Config
	queues  map[string]*Queue
	offsets *OffsetStore
}

func OpenManager(dir string, cfg Config) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	if cfg.Encrypted {
		if err := EnsureDirectoryEncrypted(dir); err != nil {
			return nil, fmt.Errorf("failed to enable data_dir encryption: %w", err)
		}
	}

	offsets, err := openOffsetStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open manager offset store: %w", err)
	}

	m := &Manager{
		dir:     dir,
		cfg:     cfg,
		queues:  make(map[string]*Queue),
		offsets: offsets,
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		_ = offsets.Close()
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		q, err := openQueue(dir, name, cfg, m.offsets)
		if err != nil {
			_ = offsets.Close()
			return nil, fmt.Errorf("restore queue %q: %w", name, err)
		}
		m.queues[name] = q
	}

	return m, nil
}

func (m *Manager) CreateQueue(name string, opts ...Config) (*Queue, error) {
	cfg := m.cfg
	if len(opts) > 0 {
		cfg = opts[0]
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, ErrQueueExists
	}

	queueDir := filepath.Join(m.dir, name)
	if _, err := os.Stat(queueDir); err == nil {
		return nil, ErrQueueExists
	}

	q, err := openQueue(m.dir, name, cfg, m.offsets)
	if err != nil {
		return nil, err
	}

	m.queues[name] = q
	return q, nil
}

func (m *Manager) GetQueue(name string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

func (m *Manager) ListQueues() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.queues))
	for name := range m.queues {
		names = append(names, name)
	}
	return names
}

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

func (m *Manager) Stats() EngineStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := EngineStats{
		ActiveQueues: len(m.queues),
		Queues:       make(map[string]QueueStats, len(m.queues)),
	}

	for name, q := range m.queues {
		q.mu.Lock()
		msgCount := uint64(len(q.index))
		segCount := len(q.segments)
		var size int64
		for _, seg := range q.segments {
			size += seg.pos
		}
		q.mu.Unlock()

		stats.Queues[name] = QueueStats{
			MessageCount: msgCount,
			SegmentCount: segCount,
			SizeBytes:    size,
			LastOffset:   msgCount,
		}
		stats.TotalBytes += size
	}

	return stats
}

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
