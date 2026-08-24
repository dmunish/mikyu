package queue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrQueueExists    = errors.New("queue with this name already exists")
	ErrQueueNotFound  = errors.New("queue not found")
	ErrOffsetNotFound = errors.New("offset not found")
	ErrCorruptRecord  = errors.New("corrupt record")
)

type indexEntry struct {
	pos int64 // where the length field of the record is in the file
	len int64 // size of the actual record
}

type Queue struct {
	name string
	file *os.File

	mu    sync.Mutex
	index []indexEntry
}

type Manager struct {
	mu     sync.RWMutex
	dir    string
	queues map[string]*Queue
}

// Opens/creates WAL directory and rebuilds any found queues
func Open(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	m := &Manager{dir: dir, queues: make(map[string]*Queue)}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".wal")
		q, err := openQueue(dir, name)
		if err != nil {
			return nil, fmt.Errorf("restore queue %q: %w", name, err)
		}
		m.queues[name] = q
	}

	return m, nil
}

func (m *Manager) CreateQueue(name string) (*Queue, error) {
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

	q := &Queue{name: name, file: f}
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

func openQueue(dir, name string) (*Queue, error) {
	path := filepath.Join(dir, name+".wal")
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	q := &Queue{name: name, file: f}
	if err := q.rebuildIndex(); err != nil {
		return nil, err
	}
	return q, nil
}

// rebuilds the offset index in-memory, discarding truncated records
func (q *Queue) rebuildIndex() error {
	var pos int64
	for {
		var lenBuf [4]byte
		n, err := q.file.ReadAt(lenBuf[:], pos)
		if n < 4 {
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			break
		}

		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		total := int64(4) + int64(bodyLen)

		body := make([]byte, bodyLen)
		n, err = q.file.ReadAt(body, pos+4)
		if int64(n) < int64(bodyLen) {
			break // torn write
		}
		if err != nil && err != io.EOF {
			return err
		}

		if _, _, err := decodeRecord(body); err != nil {
			break // checksum mismatch
		}

		q.index = append(q.index, indexEntry{pos: pos, len: total})
		pos += total
	}

	return q.file.Truncate(pos)
}
