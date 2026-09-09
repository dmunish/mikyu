package engine

import (
	"bytes"
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
	ErrQueueClosed     = errors.New("queue is closed")
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
	closed            bool
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
// A WAL write is never allowed to straddle a segment roll: each staged sub-batch
// belongs entirely to one segment, preserving contiguous WriteAt semantics.
func (q *Queue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}

	type stagedBatch struct {
		w         *wal
		seg       *segment
		recs      [][]byte
		positions []int64
		recLens   []int64
		offsets   []uint64
		ids       []uuid.UUID
		totalLen  int64
		startIdx  int
	}

	q.mu.Lock()
	if q.closed || q.active == nil || q.w == nil {
		q.mu.Unlock()
		return nil, ErrQueueClosed
	}
	if err := q.w.currentError(); err != nil {
		q.mu.Unlock()
		return nil, err
	}

	offsets := make([]uint64, len(payloads))
	ids := make([]uuid.UUID, len(payloads))
	recs := make([][]byte, len(payloads))
	recLens := make([]int64, len(payloads))

	// Prepare records up-front while the queue is locked, but do not mutate queue
	// positions until a complete per-segment group is ready to commit.
	ts := time.Now()
	for i, payload := range payloads {
		if int64(len(payload)) > q.cfg.MaxMessageBytes {
			q.mu.Unlock()
			return nil, ErrMessageTooLarge
		}
		id := uuid.NewV7()
		rec := encodeRecord(id, ts, payload)
		recLen := int64(len(rec))
		if recLen > q.cfg.MaxSegmentBytes {
			q.mu.Unlock()
			return nil, ErrRecordTooLarge
		}
		ids[i], recs[i], recLens[i] = id, rec, recLen
	}

	commitSync := func(b *stagedBatch) (int64, bool, error) {
		seq, wake, err := b.w.pushBatch(b.recs, b.positions)
		if err != nil {
			return 0, false, err
		}
		b.seg.pos += b.totalLen
		for i := range b.recs {
			if err := b.seg.writeIndexAt(int64(b.offsets[i]-b.seg.baseOff)*rawIndexEntrySize, b.positions[i], b.recLens[i]); err != nil {
				b.w.setError(err)
				return 0, false, err
			}
		}
		return seq, wake, nil
	}

	commitAsync := func(b *stagedBatch) error {
		for i, rec := range b.recs {
			q.cache.Put(Message{
				Offset:    b.offsets[i],
				ID:        b.ids[i].String(),
				Timestamp: ts,
				Payload:   bytes.Clone(payloads[b.startIdx+i]),
			})
			b.w.asyncChan <- asyncEntry{seg: b.seg, rec: rec, pos: b.positions[i], recLen: b.recLens[i], offset: b.offsets[i]}
		}
		b.seg.pos += b.totalLen
		last := b.offsets[len(b.offsets)-1]
		if last+1 > q.publishedOffset {
			q.publishedOffset = last + 1
		}
		return nil
	}

	var staged stagedBatch
	reset := func(start int) {
		staged = stagedBatch{w: q.w, seg: q.active, startIdx: start}
	}
	reset(0)

	for i := range payloads {
		recLen := recLens[i]
		rollRequired := q.active.pos+staged.totalLen+recLen > q.cfg.MaxSegmentBytes || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge
		if rollRequired {
			if len(staged.recs) == 0 {
				// An old/aged active segment may already be unable to accept the next
				// record. Roll before staging the record so every group fits entirely
				// inside one segment.
				if err := q.roll(); err != nil {
					q.mu.Unlock()
					return nil, err
				}
				reset(i)
				rollRequired = false
			}
		}
		if rollRequired {
			if !q.cfg.Fsync {
				if err := commitAsync(&staged); err != nil {
					q.mu.Unlock()
					return nil, err
				}
			} else {
				seq, wake, err := commitSync(&staged)
				if err != nil {
					q.mu.Unlock()
					return nil, err
				}
				q.mu.Unlock()
				if wake {
					select {
					case staged.w.wake <- struct{}{}:
					default:
					}
				}
				if err := staged.w.wait(seq); err != nil {
					return nil, err
				}
				q.mu.Lock()
				if q.closed {
					q.mu.Unlock()
					return nil, ErrQueueClosed
				}
			}
			if err := q.roll(); err != nil {
				q.mu.Unlock()
				return nil, err
			}
			reset(i)
		}

		staged.recs = append(staged.recs, recs[i])
		staged.recLens = append(staged.recLens, recLens[i])
		staged.positions = append(staged.positions, q.active.pos+staged.totalLen)
		staged.offsets = append(staged.offsets, q.nextOffset)
		staged.ids = append(staged.ids, ids[i])
		staged.totalLen += recLen
		q.nextOffset++
		offsets[i] = q.nextOffset - 1
	}

	if !q.cfg.Fsync {
		if err := commitAsync(&staged); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		q.mu.Unlock()
		return offsets, nil
	}

	seq, wake, err := commitSync(&staged)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}
	q.mu.Unlock()
	if wake {
		select {
		case staged.w.wake <- struct{}{}:
		default:
		}
	}
	if err := staged.w.wait(seq); err != nil {
		return nil, err
	}
	q.mu.Lock()
	if last := offsets[len(offsets)-1]; last+1 > q.publishedOffset {
		q.publishedOffset = last + 1
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

	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return Message{}, ErrQueueClosed
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
	if q.closed {
		q.mu.RUnlock()
		return nil, ErrQueueClosed
	}
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
	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return ErrQueueClosed
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
	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return 0, ErrQueueClosed
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
	return firstErr
}
