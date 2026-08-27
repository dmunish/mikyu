package queue

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrQueueExists    = errors.New("queue with this name already exists")
	ErrQueueNotFound  = errors.New("queue not found")
	ErrOffsetNotFound = errors.New("offset not found")
	ErrCorruptRecord  = errors.New("corrupt record")
)

type Queue struct {
	name string
	file *os.File
	w    *wal

	mu    sync.Mutex
	pos   int64        // Next write byte offset in file
	index []indexEntry // In-memory offset lookup table
}

func newQueue(name string, f *os.File, cfg Config) *Queue {
	q := &Queue{
		name: name,
		file: f,
		w:    newWAL(f, cfg),
	}
	q.w.start()
	return q
}

func openQueue(dir, name string, cfg Config) (*Queue, error) {
	path := filepath.Join(dir, name+".wal")
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	q := newQueue(name, f, cfg)
	if err := q.rebuildIndex(); err != nil {
		f.Close()
		return nil, err
	}
	return q, nil
}

func (q *Queue) Close() error {
	flushErr := q.w.stop()
	closeErr := q.file.Close()

	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
