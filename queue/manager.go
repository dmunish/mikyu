package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manager struct {
	mu     sync.RWMutex
	dir    string
	queues map[string]*Queue
}

// Open initializes a directory and loads any existing queues.
func Open(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	m := &Manager{
		dir:    dir,
		queues: make(map[string]*Queue),
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".wal")
		q, err := openQueue(dir, name, Config{})
		if err != nil {
			return nil, fmt.Errorf("restore queue %q: %w", name, err)
		}
		m.queues[name] = q
	}

	return m, nil
}

func (m *Manager) CreateQueue(name string, opts ...Config) (*Queue, error) {
	var cfg Config
	if len(opts) > 0 {
		cfg = opts[0]
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, ErrQueueExists
	}

	path := filepath.Join(m.dir, name+".wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create wal file: %w", err)
	}

	q := newQueue(name, f, cfg)
	m.queues[name] = q
	return q, nil
}

func (m *Manager) Queue(name string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
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
	return firstErr
}
