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

const (
	resPending = iota
	resCommitted
	resAborted
)

// reservation holds one async batch's place in the queue's FIFO of outstanding segment-space reservations.
type reservation struct {
	seg        *segment
	startPos   int64
	totalLen   int64
	lastOffset uint64
	state      int
}

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
	reservedPos       int64
	reservations      []*reservation
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
	parent, err := os.OpenRoot(parentDir)
	if err != nil {
		return nil, err
	}
	defer parent.Close()

	if err := parent.Mkdir(name, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	dir := filepath.Join(parentDir, name)
	if cfg.EncryptedStorage() {
		if err := EnsureDirectoryEncrypted(dir); err != nil {
			return nil, fmt.Errorf("failed to enable storage encryption for queue %q: %w", name, err)
		}
	}

	// Clamp initial pre-allocation size to the segment cap
	initialPrealloc := min(int64(minSegmentPrealloc), cfg.MaxSegment)

	scope := scopeOf(scopes)
	q := &Queue{
		name:              name,
		dir:               dir,
		cfg:               cfg,
		offsets:           store,
		cache:             newRingCache(cfg.MaxCache),
		nextPreallocBytes: initialPrealloc,
		notify:            make(chan struct{}),
		m:                 newQueueMetrics(scope),
		scope:             scope,
	}
	if err := q.loadSegments(); err != nil {
		return nil, err
	}
	if cfg.KeyedCompaction() {
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
	q.w = newWAL(q.active.logFile, q.cfg, q.nextOffset, q.scope)
	q.w.start()
	return nil
}

// broadcastLocked wakes every Fetch parked on the current notify channel.
func (q *Queue) broadcastLocked() {
	old := q.notify
	q.notify = make(chan struct{})
	close(old)
}

// reserveLocked reserves contiguous segment space and offsets for one async staged group without blocking.
func (q *Queue) reserveLocked(b *stagedBatch) *reservation {
	base := q.active.pos + q.reservedPos
	node := &reservation{seg: b.seg, startPos: base, totalLen: b.totalLen, lastOffset: b.offsets[len(b.offsets)-1]}
	q.reservations = append(q.reservations, node)
	q.reservedPos += b.totalLen
	b.positions = b.positions[:0]
	var cum int64
	for i := range b.recs {
		b.positions = append(b.positions, base+cum)
		cum += b.recLens[i]
	}
	return node
}

// commitReservationLocked marks a reservation delivered and publishes every reservation resolved at the front of the FIFO.
func (q *Queue) commitReservationLocked(node *reservation) {
	node.state = resCommitted
	q.drainReservationsLocked()
}

// abortReservationLocked marks a reservation failed; its offsets stay consumed but its space becomes a hole.
func (q *Queue) abortReservationLocked(node *reservation) {
	node.state = resAborted
	q.drainReservationsLocked()
}

// drainReservationsLocked resolves the committed-prefix frontier: segment end and published offset only ever advance over resolved reservations in order.
func (q *Queue) drainReservationsLocked() {
	for len(q.reservations) > 0 && q.reservations[0].state != resPending {
		node := q.reservations[0]
		q.reservations = q.reservations[1:]
		q.reservedPos -= node.totalLen
		if node.state != resCommitted {
			continue
		}
		if end := node.startPos + node.totalLen; end > node.seg.pos {
			node.seg.pos = end
		}
		if node.lastOffset+1 > q.publishedOffset {
			q.publishedOffset = node.lastOffset + 1
		}
	}
}

// hasPendingReservation reports whether any async batch is between its reserve and commit steps.
func (q *Queue) hasPendingReservation() bool {
	for _, node := range q.reservations {
		if node.state == resPending {
			return true
		}
	}
	return false
}

func (q *Queue) closeSegments() {
	for _, seg := range q.segments {
		_ = seg.Close()
	}
	q.segments = nil
}

// roll rotates to a new active segment.
func (q *Queue) roll() error {
	// A pending async reservation still owns space at the tail of the active
	// segment; its producer is between the reserve and commit steps, holding
	// no lock while it pushes onto the WAL channel. Wait for it to resolve
	// before sealing the segment, so no write lands on a closed file.
	for q.hasPendingReservation() {
		q.mu.Unlock()
		time.Sleep(time.Millisecond)
		q.mu.Lock()
		if q.closed {
			return ErrQueueClosed
		}
	}

	if err := q.w.stop(); err != nil {
		return err
	}

	if q.active != nil {
		fillDuration := time.Since(q.active.createdAt)

		maxCap := min(q.cfg.MaxSegment, maxSegmentPrealloc)
		minCap := min(int64(minSegmentPrealloc), q.cfg.MaxSegment)

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
	q.w = newWAL(seg.logFile, q.cfg, q.nextOffset, q.scope)
	q.w.start()
	return nil
}

func (q *Queue) MaxAppendPayloadBytes() int64 {
	maxPayload := min(q.cfg.MaxPayload, q.cfg.MaxSegment-int64(minRecordSize))
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
	if cfg.MaxSegmentAge > 0 {
		q.cfg.MaxSegmentAge = cfg.MaxSegmentAge
	}
	if cfg.MaxCache > 0 {
		q.cfg.MaxCache = cfg.MaxCache
		q.cache.SetMaxBytes(cfg.MaxCache)
	}
	if cfg.MaxFlushBytes > 0 {
		q.cfg.MaxFlushBytes = cfg.MaxFlushBytes
	}
	if cfg.MaxFlushWait > 0 {
		q.cfg.MaxFlushWait = cfg.MaxFlushWait
	}
	if cfg.FlushLinger > 0 {
		q.cfg.FlushLinger = cfg.FlushLinger
	}
	if q.w != nil {
		q.w.UpdateConfig(q.cfg.MaxFlushBytes, q.cfg.MaxFlushWait, q.cfg.FlushLinger)
	}
}
