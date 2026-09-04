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

type Message struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Payload   []byte
}

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

	q := &Queue{name: name, dir: dir, cfg: cfg, offsets: store}
	if err := q.loadSegments(); err != nil {
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
			_ = seg.Close()
			return err
		}

		if i < len(bases)-1 {
			if err := seg.enableMMap(); err != nil {
				_ = seg.Close()
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

func (q *Queue) roll() error {
	if err := q.w.stop(); err != nil {
		return err
	}

	if q.active != nil {
		_ = q.active.enableMMap()
	}

	nextOff := q.nextOffset
	seg, err := openSegment(q.dir, nextOff)
	if err != nil {
		return err
	}
	if _, err := seg.recover(); err != nil {
		_ = seg.Close()
		return err
	}

	q.active = seg
	q.segments = append(q.segments, seg)
	q.w = newWAL(seg.logFile, q.cfg, int64(q.nextOffset))
	q.w.start()
	return nil
}

func (q *Queue) Produce(ctx context.Context, payload []byte) (uint64, error) {
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
	if int64(len(rec)) > q.cfg.MaxSegmentBytes {
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
	q.nextOffset++
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

	offset := uint64(seq)

	q.mu.Lock()
	_ = seg.writeIndex(pos, recLen)
	for len(q.index) <= int(offset) {
		q.index = append(q.index, indexEntry{})
	}
	q.index[offset] = indexEntry{seg: seg, pos: pos, len: recLen}
	q.mu.Unlock()

	return offset, nil
}

func (q *Queue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
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
		batchTotalLen += recLen
	}

	seq, wake, err := q.w.pushBatch(recs, positions)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}

	q.active.pos += batchTotalLen
	q.nextOffset += uint64(len(payloads))
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
	firstOffset := seq - int64(len(payloads)-1)

	offsets := make([]uint64, len(payloads))
	q.mu.Lock()
	for i, recLen := range recLens {
		pos := positions[i]
		seg := segments[i]
		_ = seg.writeIndex(pos, recLen)
		off := uint64(firstOffset + int64(i))
		for len(q.index) <= int(off) {
			q.index = append(q.index, indexEntry{})
		}
		q.index[off] = indexEntry{seg: seg, pos: pos, len: recLen}
		offsets[i] = off
	}
	q.mu.Unlock()

	return offsets, nil
}

func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	q.mu.RLock()
	if offset >= uint64(len(q.index)) || q.index[offset].seg == nil {
		q.mu.RUnlock()
		return Message{}, ErrOffsetNotFound
	}
	entry := q.index[offset]

	var rec []byte
	if len(entry.seg.mmapData) >= int(entry.pos+entry.len) {
		rec = entry.seg.mmapData[entry.pos : entry.pos+entry.len]
	} else {
		rec = make([]byte, entry.len)
		if _, err := entry.seg.logFile.ReadAt(rec, entry.pos); err != nil {
			q.mu.RUnlock()
			return Message{}, err
		}
	}
	q.mu.RUnlock()

	id, ts, payload, err := decodeRecord(rec[4:])
	if err != nil {
		return Message{}, err
	}

	return Message{
		Offset:    offset,
		ID:        id,
		Timestamp: ts,
		Payload:   payload,
	}, nil
}

func (q *Queue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error) {
	if count <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	q.mu.RLock()
	total := uint64(len(q.index))
	if startOffset >= total {
		q.mu.RUnlock()
		return nil, ErrOffsetNotFound
	}

	// Clamp requested count to available records
	endOffset := startOffset + uint64(count)
	if endOffset > total {
		endOffset = total
	}

	// Snapshot index entries under a single lock acquisition
	entries := make([]indexEntry, endOffset-startOffset)
	copy(entries, q.index[startOffset:endOffset])
	q.mu.RUnlock()

	// Read and decode payloads outside the lock
	out := make([]Message, 0, len(entries))
	for i, entry := range entries {
		if entry.seg == nil {
			break // Pending write
		}

		var rec []byte
		if len(entry.seg.mmapData) >= int(entry.pos+entry.len) {
			rec = entry.seg.mmapData[entry.pos : entry.pos+entry.len]
		} else {
			rec = make([]byte, entry.len)
			if _, err := entry.seg.logFile.ReadAt(rec, entry.pos); err != nil {
				return out, err
			}
		}

		id, ts, payload, err := decodeRecord(rec[4:])
		if err != nil {
			return out, err
		}

		out = append(out, Message{
			Offset:    startOffset + uint64(i),
			ID:        id,
			Timestamp: ts,
			Payload:   payload,
		})
	}

	return out, nil
}

func (q *Queue) LastOffset() uint64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.nextOffset
}

func (q *Queue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.offsets == nil {
		return ErrQueueNotFound
	}
	return q.offsets.Commit(q.name, group, offset)
}

func (q *Queue) FetchOffset(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if q.offsets == nil {
		return 0, ErrQueueNotFound
	}
	return q.offsets.Fetch(q.name, group), nil
}

func (q *Queue) Close() error {
	flushErr := q.w.stop()
	var closeErr error
	for _, seg := range q.segments {
		if err := seg.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
