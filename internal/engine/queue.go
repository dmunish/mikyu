package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
	"uuid"
)

const (
	minSegmentPrealloc = 8 * 1024 * 1024   // 8 MB floor
	maxSegmentPrealloc = 256 * 1024 * 1024 // 256 MB ceiling
	fastFillThreshold  = 10 * time.Second  // Double allocation if filled under 10s
	slowFillThreshold  = 5 * time.Minute   // Halve allocation if filling took longer than 5m
)

var (
	ErrQueueExists     = errors.New("queue with this name already exists")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrOffsetNotFound  = errors.New("offset not found")
	ErrMessageTooLarge = errors.New("message payload exceeds maximum allowed size")
	ErrRecordTooLarge  = errors.New("record size exceeds max segment size")
	ErrOffsetTruncated = errors.New("offset has been truncated from storage")
)

// Message is the internal engine‑level message representation.
type Message struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Payload   []byte
}

// Config holds per‑queue configuration parameters.
type Config struct {
	BatchSize       int
	MaxInterval     time.Duration
	MinInterval     time.Duration
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
	Encrypted       bool
	Fsync           bool
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.MaxInterval <= 0 {
		c.MaxInterval = 10 * time.Millisecond
	}
	if c.MinInterval == 0 {
		c.MinInterval = 30 * time.Microsecond
	} else if c.MinInterval < 0 {
		c.MinInterval = 0 // Negative duration explicitly disables linger
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
	mu                sync.RWMutex
}

// compile‑time assertion that Queue satisfies Storage.
var _ Storage = (*Queue)(nil)

func openQueue(parentDir, name string, cfg Config, store *OffsetStore) (*Queue, error) {
	cfg = cfg.withDefaults()
	dir := filepath.Join(parentDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if cfg.Encrypted {
		if err := EnsureDirectoryEncrypted(dir); err != nil {
			return nil, fmt.Errorf("failed to enable storage encryption for queue %q: %w", name, err)
		}
	}

	// Clamp initial pre-allocation size to MaxSegmentBytes
	initialPrealloc := min(int64(minSegmentPrealloc), cfg.MaxSegmentBytes)

	q := &Queue{
		name:              name,
		dir:               dir,
		cfg:               cfg,
		offsets:           store,
		cache:             newRingCache(defaultRingCacheCapacity),
		nextPreallocBytes: initialPrealloc,
	}
	if err := q.loadSegments(); err != nil {
		return nil, err
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
	q.w = newWAL(q.active.logFile, q.cfg, int64(q.nextOffset))
	q.w.start()
	return nil
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

		// Max cap is the lesser of maxSegmentPrealloc (256MB) and q.cfg.MaxSegmentBytes
		maxCap := min(q.cfg.MaxSegmentBytes, maxSegmentPrealloc)

		// Min cap is the lesser of minSegmentPrealloc (8MB) and q.cfg.MaxSegmentBytes
		minCap := min(int64(minSegmentPrealloc), q.cfg.MaxSegmentBytes)

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
		_ = q.active.logFile.Sync()

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
	q.w = newWAL(seg.logFile, q.cfg, int64(q.nextOffset))
	q.w.start()
	return nil
}

func (q *Queue) MaxAppendPayloadBytes() int64 {
	maxPayload := min(q.cfg.MaxMessageBytes, q.cfg.MaxSegmentBytes-int64(minRecordSize))
	if maxPayload < 0 {
		return 0
	}
	return maxPayload
}

func (q *Queue) Append(ctx context.Context, payload []byte) (uint64, error) {
	offs, err := q.AppendBatch(ctx, [][]byte{payload})
	if err != nil {
		return 0, err
	}
	return offs[0], nil
}

// AppendBatch implements Storage.AppendBatch.
func (q *Queue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := len(payloads)
	if n == 0 {
		return nil, nil
	}

	var (
		recs      [][]byte
		positions []int64
		recLens   []int64
		segments  []*segment
		offsets   []uint64
		ids       []uuid.UUID
	)

	// Stack-allocate fixed arrays for single-element batches (e.g. delegated from Append)
	// to prevent escaping slice headers to the heap.
	if n == 1 {
		var (
			rBuf [1][]byte
			pBuf [1]int64
			lBuf [1]int64
			sBuf [1]*segment
			oBuf [1]uint64
			iBuf [1]uuid.UUID
		)
		recs = rBuf[:1]
		positions = pBuf[:1]
		recLens = lBuf[:1]
		segments = sBuf[:1]
		offsets = oBuf[:1]
		ids = iBuf[:1]
	} else {
		recs = make([][]byte, n)
		positions = make([]int64, n)
		recLens = make([]int64, n)
		segments = make([]*segment, n)
		offsets = make([]uint64, n)
		ids = make([]uuid.UUID, n)
	}

	ts := time.Now()

	q.mu.Lock()
	var batchTotalLen int64
	for i, p := range payloads {
		if int64(len(p)) > q.cfg.MaxMessageBytes {
			q.mu.Unlock()
			return nil, ErrMessageTooLarge
		}
		id := uuid.NewV7()
		ids[i] = id
		rec := encodeRecord(id, ts, p)
		recLen := int64(len(rec))

		if recLen > q.cfg.MaxSegmentBytes {
			q.mu.Unlock()
			return nil, ErrRecordTooLarge
		}
		if q.active.pos+batchTotalLen+recLen > q.cfg.MaxSegmentBytes || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge {
			if err := q.roll(); err != nil {
				q.mu.Unlock()
				return nil, err
			}
			batchTotalLen = 0
		}

		positions[i] = q.active.pos + batchTotalLen
		recLens[i] = recLen
		recs[i] = rec
		segments[i] = q.active
		offsets[i] = q.nextOffset
		q.nextOffset++
		batchTotalLen += recLen
	}

	// --- ASYNC FAST PATH (Fsync: false) ---
	if !q.cfg.Fsync {
		for i, p := range payloads {
			msg := Message{
				Offset:    offsets[i],
				ID:        ids[i].String(),
				Timestamp: ts,
				Payload:   p,
			}
			q.cache.Put(msg)
		}
		q.active.pos += batchTotalLen
		lastOff := offsets[len(offsets)-1]
		if lastOff+1 > q.publishedOffset {
			q.publishedOffset = lastOff + 1
		}
		asyncCh := q.w.asyncChan // Capture under q.mu lock to prevent data race with q.roll()
		q.mu.Unlock()

		for i := range payloads {
			asyncCh <- asyncEntry{
				seg:    segments[i],
				rec:    recs[i],
				pos:    positions[i],
				recLen: recLens[i],
				offset: offsets[i],
			}
		}
		return offsets, nil
	}

	// --- DURABLE SYNCHRONOUS PATH (Fsync: true) ---
	seq, wake, err := q.w.pushBatch(recs, positions)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}

	q.active.pos += batchTotalLen

	for i, recLen := range recLens {
		pos := positions[i]
		seg := segments[i]
		_ = seg.writeIndex(pos, recLen)
	}
	q.mu.Unlock()

	if wake {
		select {
		case q.w.wake <- struct{}{}:
		default:
		}
	}

	if err := q.w.wait(seq); err != nil {
		return nil, err
	}

	q.mu.Lock()
	lastOff := offsets[len(offsets)-1]
	if lastOff+1 > q.publishedOffset {
		q.publishedOffset = lastOff + 1
	}
	q.mu.Unlock()

	return offsets, nil
}

func (q *Queue) readFromSegment(seg *segment, offset uint64) (Message, error) {
	pos, recLen, err := seg.readIndexEntry(offset)
	if err != nil || recLen < (minRecordSize-recordLengthSize) {
		return Message{}, ErrOffsetNotFound
	}

	var buf []byte
	if len(seg.mmapData) >= int(pos+recLen) {
		buf = seg.mmapData[pos : pos+recLen]
	} else {
		buf = make([]byte, recLen)
		if _, err := seg.logFile.ReadAt(buf, pos); err != nil {
			return Message{}, err
		}
	}

	id, ts, payload, err := decodeRecord(buf[recordLengthSize:])
	if err != nil {
		return Message{}, err
	}
	return Message{Offset: offset, ID: id, Timestamp: ts, Payload: payload}, nil
}

// Read implements Storage.Read.
func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	if offset < q.MinOffset() {
		return Message{}, ErrOffsetTruncated
	}

	// 1. Fast Path: Check in-memory ring cache first (~10 ns)
	if msg, ok := q.cache.Get(offset); ok {
		return msg, nil
	}

	// 2. Disk / MMap Path: Fall back to historical log segments
	seg, err := q.locateSegment(offset)
	if err != nil {
		return Message{}, err
	}

	return q.readFromSegment(seg, offset)
}

// ReadBatch implements Storage.ReadBatch.
func (q *Queue) ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	q.mu.RLock()
	minOff := q.minOffset
	total := q.nextOffset
	q.mu.RUnlock()

	// 1. Fast Failure: Reject requests below the minimum available offset
	if start < minOff {
		return nil, ErrOffsetTruncated
	}

	if start >= total {
		return nil, ErrOffsetNotFound
	}

	end := min(start+uint64(n), total)

	count := int(end - start)
	msgs := make([]Message, 0, count)

	var currentSeg *segment
	for off := start; off < end; off++ {
		// 2. Fast Path: Check in-memory ring cache first (~10 ns)
		if msg, ok := q.cache.Get(off); ok {
			msgs = append(msgs, msg)
			continue
		}

		// 3. Disk / MMap Path: Fall back to historical log segments
		if currentSeg == nil || off < currentSeg.baseOff {
			seg, err := q.locateSegment(off)
			if err != nil {
				if errors.Is(err, ErrOffsetTruncated) {
					return nil, ErrOffsetTruncated
				}
				break
			}
			currentSeg = seg
		}

		msg, err := q.readFromSegment(currentSeg, off)
		if err != nil {
			// Offset crossed current segment boundary, locate new segment
			seg, err := q.locateSegment(off)
			if err != nil {
				if errors.Is(err, ErrOffsetTruncated) {
					return nil, ErrOffsetTruncated
				}
				break
			}
			currentSeg = seg
			msg, err = q.readFromSegment(currentSeg, off)
			if err != nil {
				break
			}
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// LastOffset implements Storage.LastOffset.
func (q *Queue) LastOffset() uint64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.publishedOffset
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.name
}

// CommitOffset implements Storage.CommitOffset.
func (q *Queue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.offsets == nil {
		return ErrQueueNotFound
	}
	return q.offsets.Commit(q.name, group, offset)
}

// FetchOffset implements Storage.FetchOffset.
func (q *Queue) FetchOffset(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if q.offsets == nil {
		return 0, ErrQueueNotFound
	}
	return q.offsets.Fetch(q.name, group), nil
}

// Close flushes the WAL and closes all segment resources.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

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
	return firstErr
}
