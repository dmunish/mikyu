package queue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

var (
	ErrQueueExists     = errors.New("queue with this name already exists")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrOffsetNotFound  = errors.New("offset not found")
	ErrCorruptRecord   = errors.New("corrupt record")
	ErrMessageTooLarge = errors.New("message payload exceeds maximum allowed size")
)

type indexEntry struct {
	seg *segment
	pos int64
	len int64
}

type Queue struct {
	name     string
	dir      string
	cfg      Config
	active   *segment
	segments []*segment
	index    []indexEntry
	w        *wal
	mu       sync.Mutex
	offsets  *OffsetStore
}

type Config struct {
	BatchSize       int
	Interval        time.Duration
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.Interval <= 0 {
		c.Interval = 10 * time.Millisecond
	}
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = 64 * 1024 * 1024
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = 1 * 1024 * 1024
	}
	if c.MaxSegmentAge <= 0 {
		c.MaxSegmentAge = 1 * time.Hour
	}
	return c
}

func openQueue(parentDir, name string, cfg Config) (*Queue, error) {
	cfg = cfg.withDefaults()
	dir := filepath.Join(parentDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	offsets, err := OpenOffsetStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open offset store: %w", err)
	}

	q := &Queue{name: name, dir: dir, cfg: cfg, offsets: offsets}
	if err := q.loadSegments(); err != nil {
		_ = offsets.Close()
		return nil, err
	}
	return q, nil
}

func (q *Queue) loadSegments() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return err
	}

	var bases []uint64
	for _, e := range entries {
		var base uint64
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			if _, err := fmt.Sscanf(e.Name(), "%020d.log", &base); err == nil {
				bases = append(bases, base)
			}
		}
	}
	slices.Sort(bases)

	if len(bases) == 0 {
		bases = []uint64{0}
	}

	for i, base := range bases {
		seg, err := openSegment(q.dir, base)
		if err != nil {
			return err
		}
		rawEntries, err := seg.recover()
		if err != nil {
			seg.Close()
			return err
		}

		if i < len(bases)-1 {
			if err := seg.enableMMap(); err != nil {
				seg.Close()
				return err
			}
		}

		for _, e := range rawEntries {
			q.index = append(q.index, indexEntry{seg: seg, pos: e.pos, len: e.len})
		}
		q.segments = append(q.segments, seg)
		q.active = seg
	}

	q.w = newWAL(q.active.logFile, q.cfg, int64(len(q.index)))
	q.w.start()
	return nil
}

func (q *Queue) roll() error {
	if err := q.w.stop(); err != nil {
		return err
	}

	if q.active != nil {
		_ = q.active.enableMMap()
	}

	nextOff := uint64(len(q.index))
	seg, err := openSegment(q.dir, nextOff)
	if err != nil {
		return err
	}
	if _, err := seg.recover(); err != nil {
		seg.Close()
		return err
	}

	q.active = seg
	q.segments = append(q.segments, seg)
	q.w = newWAL(seg.logFile, q.cfg, int64(len(q.index)))
	q.w.start()
	return nil
}

// CommitOffset records a consumer group's progress. Monotonic; stale commits
// (offset <= previously stored) are silently ignored.
func (q *Queue) CommitOffset(_ /*ctx*/, group string, offset uint64) error {
	return q.offsets.Commit(group, offset)
}

// FetchOffset returns the last committed offset for a group, or 0 if none.
func (q *Queue) FetchOffset(_ /*ctx*/, group string) (uint64, error) {
	return q.offsets.Fetch(group), nil
}

func (q *Queue) Close() error {
	flushErr := q.w.stop()
	var closeErr error
	for _, seg := range q.segments {
		if err := seg.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if err := q.offsets.Close(); err != nil && closeErr == nil {
		closeErr = err
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
