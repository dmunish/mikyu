package engine

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

const (
	minSegmentPrealloc = 8 * 1024 * 1024   // 8 MB floor
	maxSegmentPrealloc = 256 * 1024 * 1024 // 256 MB ceiling
	fastFillThreshold  = 10 * time.Second  // Double allocation if filled under 10s
	slowFillThreshold  = 5 * time.Minute   // Halve allocation if filling took longer than 5m
)

// Queue implements Storage using a write‑ahead log and memory‑mapped segments.
type Queue struct {
	name              string
	dir               string
	cfg               Config
	active            *segment
	segments          []*segment
	nextOffset        uint64
	minOffset         uint64
	publishedOffset   uint64
	w                 *wal
	offsets           *OffsetStore
	cache             *ringCache
	nextPreallocBytes int64
	closed            bool
	notify            chan struct{}
	keyIndex          map[string]uint64
	m                 *queueMetrics
	scope             *metrics.Scope
	mu                sync.RWMutex
}

// compile‑time assertion that Queue satisfies Storage.
var _ Storage = (*Queue)(nil)

// openQueue opens (or creates) the queue directory name under parentDir.
// The directory is created through an os.Root scoped to parentDir, so a
// hostile name can never escape the data directory. The resulting Queue
// keeps parentDir/name only for read-only directory listing; all file
// creation, opening, and removal happen through os.Root handles. An optional
// instrumentation scope records queue operations; without one they are no-ops.
func openQueue(parentDir, name string, cfg Config, store *OffsetStore, scopes ...*metrics.Scope) (*Queue, error) {
	cfg = cfg.withDefaults()

	parent, err := os.OpenRoot(parentDir)
	if err != nil {
		return nil, err
	}
	defer parent.Close()

	if err := parent.Mkdir(name, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	dir := filepath.Join(parentDir, name)
	if cfg.Encrypted {
		if err := EnsureDirectoryEncrypted(dir); err != nil {
			return nil, fmt.Errorf("failed to enable storage encryption for queue %q: %w", name, err)
		}
	}

	// Clamp initial pre-allocation size to SegmentSize
	initialPrealloc := min(int64(minSegmentPrealloc), cfg.SegmentSize)

	scope := scopeOf(scopes)
	q := &Queue{
		name:              name,
		dir:               dir,
		cfg:               cfg,
		offsets:           store,
		cache:             newRingCache(defaultRingCacheCapacity),
		nextPreallocBytes: initialPrealloc,
		notify:            make(chan struct{}),
		m:                 newQueueMetrics(scope),
		scope:             scope,
	}
	if err := q.loadSegments(); err != nil {
		return nil, err
	}
	if cfg.Compaction {
		if err := q.buildKeyIndex(); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (q *Queue) locateSegment(offset uint64) (*segment, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if offset < q.minOffset {
		return nil, ErrOffsetTruncated
	}

	if len(q.segments) == 0 || offset >= q.publishedOffset || offset < q.segments[0].baseOff {
		if offset < q.segments[0].baseOff {
			return nil, ErrOffsetTruncated
		}
		return nil, ErrOffsetNotFound
	}

	// O(log S) binary search across open segment base offsets
	idx, _ := slices.BinarySearchFunc(q.segments, offset, func(seg *segment, target uint64) int {
		if target < seg.baseOff {
			return 1
		}
		return -1
	})

	if idx > 0 {
		idx--
	}

	if idx < len(q.segments) {
		return q.segments[idx], nil
	}
	return nil, ErrOffsetNotFound
}

// loadSegments opens all existing segment files and initialises the WAL.
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

	var lastSegNextOffset uint64
	for i, base := range bases {
		seg, err := openSegment(q.dir, base, q.nextPreallocBytes)
		if err != nil {
			q.closeSegments()
			return err
		}
		rawEntries, err := seg.recover()
		if err != nil {
			_ = seg.Close()
			q.closeSegments()
			return err
		}

		if i < len(bases)-1 {
			if err := seg.enableMMap(); err != nil {
				_ = seg.Close()
				q.closeSegments()
				return err
			}
		}

		// Calculate logical sequence high-water mark from baseOff + record count
		lastSegNextOffset = seg.baseOff + uint64(len(rawEntries))
		q.segments = append(q.segments, seg)
		q.active = seg
	}

	q.nextOffset = lastSegNextOffset
	q.publishedOffset = lastSegNextOffset
	if len(q.segments) > 0 {
		q.minOffset = q.segments[0].baseOff
	}
	q.w = newWAL(q.active.logFile, q.cfg, int64(q.nextOffset), q.scope)
	q.w.start()
	return nil
}

// broadcastLocked wakes every Fetch parked on the current notify channel.
func (q *Queue) broadcastLocked() {
	old := q.notify
	q.notify = make(chan struct{})
	close(old)
}

func (q *Queue) closeSegments() {
	for _, seg := range q.segments {
		_ = seg.Close()
	}
	q.segments = nil
}

// roll rotates to a new active segment.
func (q *Queue) roll() error {
	if err := q.w.stop(); err != nil {
		return err
	}

	if q.active != nil {
		fillDuration := time.Since(q.active.createdAt)

		maxCap := min(q.cfg.SegmentSize, maxSegmentPrealloc)
		minCap := min(int64(minSegmentPrealloc), q.cfg.SegmentSize)

		if fillDuration < fastFillThreshold {
			q.nextPreallocBytes *= 2
			if q.nextPreallocBytes > maxCap {
				q.nextPreallocBytes = maxCap
			}
		} else if fillDuration > slowFillThreshold {
			q.nextPreallocBytes /= 2
			if q.nextPreallocBytes < minCap {
				q.nextPreallocBytes = minCap
			}
		}

		// Truncate unwritten pre-allocated bytes before memory mapping
		if err := q.active.logFile.Truncate(q.active.pos); err != nil {
			return err
		}
		if err := syncData(q.active.logFile); err != nil {
			return err
		}

		if err := q.active.enableMMap(); err != nil {
			return err
		}
	}

	nextOff := q.nextOffset
	seg, err := openSegment(q.dir, nextOff, q.nextPreallocBytes)
	if err != nil {
		return err
	}

	q.active = seg
	q.segments = append(q.segments, seg)
	// Every segment before the new active is closed, including the one just
	// sealed; drop records superseded by newer appends of the same group.
	if err := q.compactClosedLocked(); err != nil {
		return err
	}
	q.m.segmentRolls.Inc()
	q.w = newWAL(seg.logFile, q.cfg, int64(q.nextOffset), q.scope)
	q.w.start()
	return nil
}

func (q *Queue) MaxAppendPayloadBytes() int64 {
	maxPayload := min(q.cfg.MaxMessageBytes, q.cfg.SegmentSize-int64(minRecordSize))
	if maxPayload < 0 {
		return 0
	}
	return maxPayload
}

// Close flushes the WAL and closes all segment resources.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}

	var firstErr error
	if q.w != nil {
		if err := q.w.stop(); err != nil {
			firstErr = err
		}
	}
	for _, seg := range q.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	q.closed = true
	q.broadcastLocked()
	return firstErr
}

// ApplyConfig updates the mutable parameters of a live queue.
func (q *Queue) ApplyConfig(cfg Config) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if cfg.BatchSize > 0 {
		q.cfg.BatchSize = cfg.BatchSize
	}
	if cfg.MaxInterval > 0 {
		q.cfg.MaxInterval = cfg.MaxInterval
	}
	if cfg.MinInterval < 0 {
		q.cfg.MinInterval = 0
	} else if cfg.MinInterval > 0 {
		q.cfg.MinInterval = cfg.MinInterval
	}
	if cfg.SegmentAge > 0 {
		q.cfg.SegmentAge = cfg.SegmentAge
	}
	if q.w != nil {
		q.w.UpdateConfig(q.cfg.BatchSize, q.cfg.MaxInterval, q.cfg.MinInterval)
	}
}
