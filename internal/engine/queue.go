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

var (
	ErrQueueExists     = errors.New("queue with this name already exists")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrOffsetNotFound  = errors.New("offset not found")
	ErrMessageTooLarge = errors.New("message payload exceeds maximum allowed size")
	ErrRecordTooLarge  = errors.New("record size exceeds max segment size")
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

type ringCache struct {
	mu    sync.RWMutex
	slots []Message
	mask  uint64
}

func newRingCache(capacity uint64) *ringCache {
	return &ringCache{
		slots: make([]Message, capacity),
		mask:  capacity - 1,
	}
}

func (r *ringCache) Put(msg Message) {
	r.mu.Lock()
	r.slots[msg.Offset&r.mask] = msg
	r.mu.Unlock()
}

func (r *ringCache) Get(offset uint64) (Message, bool) {
	r.mu.RLock()
	msg := r.slots[offset&r.mask]
	r.mu.RUnlock()
	if msg.Offset == offset && msg.Timestamp.Unix() > 0 {
		return msg, true
	}
	return Message{}, false
}

// Queue implements Storage using a write‑ahead log and memory‑mapped segments.
type Queue struct {
	name            string
	dir             string
	cfg             Config
	active          *segment
	segments        []*segment
	nextOffset      uint64
	publishedOffset uint64
	w               *wal
	offsets         *OffsetStore
	cache           *ringCache // 16k entry in-memory tail cache for fast-path reads
	mu              sync.RWMutex
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

	q := &Queue{
		name:    name,
		dir:     dir,
		cfg:     cfg,
		offsets: store,
		cache:   newRingCache(16384),
	}
	if err := q.loadSegments(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) locateSegment(offset uint64) (*segment, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if offset >= q.publishedOffset {
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

	var totalMsgs uint64
	for i, base := range bases {
		seg, err := openSegment(q.dir, base)
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

		totalMsgs += uint64(len(rawEntries))
		q.segments = append(q.segments, seg)
		q.active = seg
	}

	q.nextOffset = totalMsgs
	q.publishedOffset = totalMsgs
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
		if err := q.active.enableMMap(); err != nil {
			return err
		}
	}

	nextOff := q.nextOffset
	seg, err := openSegment(q.dir, nextOff)
	if err != nil {
		return err
	}

	q.active = seg
	q.segments = append(q.segments, seg)
	q.w = newWAL(seg.logFile, q.cfg, int64(q.nextOffset))
	q.w.start()
	return nil
}

func (q *Queue) Append(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(payload)) > q.cfg.MaxMessageBytes {
		return 0, ErrMessageTooLarge
	}

	ts := time.Now()
	id := uuid.NewV7()
	rec := encodeRecord(id, ts, payload)
	recLen := int64(len(rec))

	q.mu.Lock()
	if recLen > q.cfg.MaxSegmentBytes {
		q.mu.Unlock()
		return 0, ErrRecordTooLarge
	}
	if q.active.pos+recLen > q.cfg.MaxSegmentBytes || time.Since(q.active.createdAt) > q.cfg.MaxSegmentAge {
		if err := q.roll(); err != nil {
			q.mu.Unlock()
			return 0, err
		}
	}

	seg := q.active
	pos := seg.pos
	seg.pos += recLen

	off := q.nextOffset
	q.nextOffset++

	// --- ASYNC FAST PATH (Fsync: false) ---
	if !q.cfg.Fsync {
		msg := Message{Offset: off, ID: id.String(), Timestamp: ts, Payload: payload}
		q.cache.Put(msg)
		if off+1 > q.publishedOffset {
			q.publishedOffset = off + 1
		}
		asyncCh := q.w.asyncChan // Capture under q.mu lock to prevent data race with q.roll()
		q.mu.Unlock()

		// Non-blocking async dispatch
		asyncCh <- asyncEntry{
			seg:    seg,
			rec:    rec,
			pos:    pos,
			recLen: recLen,
			offset: off,
		}
		return off, nil
	}

	// --- DURABLE SYNCHRONOUS PATH (Fsync: true) ---
	_ = seg.writeIndex(pos, recLen)
	seq, wake, err := q.w.push(rec, pos)
	if err != nil {
		q.mu.Unlock()
		return 0, err
	}
	q.mu.Unlock()

	if wake {
		select {
		case q.w.wake <- struct{}{}:
		default:
		}
	}

	if err := q.w.wait(seq); err != nil {
		return 0, err
	}

	q.mu.Lock()
	if off+1 > q.publishedOffset {
		q.publishedOffset = off + 1
	}
	q.mu.Unlock()

	return off, nil
}

// AppendBatch implements Storage.AppendBatch.
func (q *Queue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}

	recs := make([][]byte, len(payloads))
	positions := make([]int64, len(payloads))
	recLens := make([]int64, len(payloads))
	segments := make([]*segment, len(payloads))
	offsets := make([]uint64, len(payloads))
	ids := make([]uuid.UUID, len(payloads))
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

// Read implements Storage.Read.
func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
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

	pos, recLen, err := seg.readIndexEntry(offset)
	if err != nil || recLen < 34 {
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

	id, ts, payload, err := decodeRecord(buf[4:])
	if err != nil {
		return Message{}, err
	}
	return Message{Offset: offset, ID: id, Timestamp: ts, Payload: payload}, nil
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
	// Allow reading up to nextOffset so in-cache messages can be read immediately
	total := q.nextOffset
	q.mu.RUnlock()

	if start >= total {
		return nil, ErrOffsetNotFound
	}

	end := start + uint64(n)
	if end > total {
		end = total
	}

	count := int(end - start)
	msgs := make([]Message, 0, count)

	var currentSeg *segment
	for off := start; off < end; off++ {
		// 1. Fast Path: Check in-memory ring cache first (~10 ns)
		if msg, ok := q.cache.Get(off); ok {
			msgs = append(msgs, msg)
			continue
		}

		// 2. Disk / MMap Path: Fall back to historical log segments
		if currentSeg == nil || off < currentSeg.baseOff {
			seg, err := q.locateSegment(off)
			if err != nil {
				break
			}
			currentSeg = seg
		}

		pos, recLen, err := currentSeg.readIndexEntry(off)
		if err != nil {
			// Offset crossed current segment boundary, locate new segment
			seg, err := q.locateSegment(off)
			if err != nil {
				break
			}
			currentSeg = seg
			pos, recLen, err = currentSeg.readIndexEntry(off)
			if err != nil || recLen < 34 {
				break
			}
		}

		var buf []byte
		if len(currentSeg.mmapData) >= int(pos+recLen) {
			buf = currentSeg.mmapData[pos : pos+recLen]
		} else {
			buf = make([]byte, recLen)
			if _, err := currentSeg.logFile.ReadAt(buf, pos); err != nil {
				return msgs, err
			}
		}

		id, ts, payload, err := decodeRecord(buf[4:])
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, Message{
			Offset:    off,
			ID:        id,
			Timestamp: ts,
			Payload:   payload,
		})
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
