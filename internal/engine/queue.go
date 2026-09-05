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
	name       string
	dir        string
	cfg        Config
	active     *segment
	segments   []*segment
	index      []indexEntry
	nextOffset uint64
	w          *wal
	offsets    *OffsetStore
	mu         sync.RWMutex
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
	}
	if err := q.loadSegments(); err != nil {
		return nil, err
	}
	return q, nil
}
func (q *Queue) locateSegment(offset uint64) (*segment, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if offset >= q.nextOffset {
		return nil, ErrOffsetNotFound
	}

	// Binary search across open segments (O(log S), where S = segment count)
	idx, found := slices.BinarySearchFunc(q.segments, offset, func(seg *segment, target uint64) int {
		if target < seg.baseOff {
			return 1
		}
		return -1
	})

	if !found && idx > 0 {
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

		for _, e := range rawEntries {
			q.index = append(q.index, indexEntry{seg: seg, pos: e.pos, len: e.len})
		}
		q.segments = append(q.segments, seg)
		q.active = seg
	}

	q.nextOffset = uint64(len(q.index))
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

// Append implements Storage.Append.
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

	seq, wake, err := q.w.push(rec, pos)
	if err != nil {
		q.mu.Unlock()
		return 0, err
	}

	seg.pos += recLen
	off := q.nextOffset
	q.nextOffset++

	// Atomic index registration under primary lock
	_ = seg.writeIndex(pos, recLen)
	for uint64(len(q.index)) <= off {
		q.index = append(q.index, indexEntry{})
	}
	q.index[off] = indexEntry{seg: seg, pos: pos, len: recLen}
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
	ts := time.Now()

	q.mu.Lock()
	var batchTotalLen int64
	for i, p := range payloads {
		if int64(len(p)) > q.cfg.MaxMessageBytes {
			q.mu.Unlock()
			return nil, ErrMessageTooLarge
		}
		id := uuid.NewV7()
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

	seq, wake, err := q.w.pushBatch(recs, positions)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}

	q.active.pos += batchTotalLen

	// Atomic index registration under primary lock
	for i, recLen := range recLens {
		pos := positions[i]
		seg := segments[i]
		off := offsets[i]
		_ = seg.writeIndex(pos, recLen)
		for uint64(len(q.index)) <= off {
			q.index = append(q.index, indexEntry{})
		}
		q.index[off] = indexEntry{seg: seg, pos: pos, len: recLen}
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

	return offsets, nil
}

// Read implements Storage.Read.
func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	seg, err := q.locateSegment(offset)
	if err != nil {
		return Message{}, err
	}

	pos, recLen, err := seg.readIndexEntry(offset)
	if err != nil {
		return Message{}, ErrOffsetNotFound
	}

	var buf []byte
	if len(seg.mmapData) >= int(pos+recLen) {
		buf = seg.mmapData[pos : pos+recLen] // Zero-copy payload slice
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
		// Re-use currentSeg if off still belongs to it; locate new segment on segment boundaries
		if currentSeg == nil || off < currentSeg.baseOff {
			seg, err := q.locateSegment(off)
			if err != nil {
				break
			}
			currentSeg = seg
		}

		pos, recLen, err := currentSeg.readIndexEntry(off)
		if err != nil {
			break // Reached boundary of active pending write
		}

		var buf []byte
		if len(currentSeg.mmapData) >= int(pos+recLen) {
			buf = currentSeg.mmapData[pos : pos+recLen] // Zero-copy mmap slice
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
	return q.nextOffset
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
