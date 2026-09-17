// Package enginetest provides an in-memory engine.Storage implementation for use in tests.
package enginetest

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// recordOverhead is the per-record fixed cost mirrored from the engine's
// minimum record size (length, CRC, id, timestamp fields).
const recordOverhead = 34

// memSegment mirrors one physical log segment of engine.Queue: a contiguous
// span of offsets capped by the queue's maxSeg, with the newest record's
// timestamp tracked for age-floor walks.
type memSegment struct {
	baseOff    uint64
	sizeBytes  int64
	lastTS     time.Time
	hasRecords bool
}

// MemStorage is an in-memory engine.Storage implementation for use in tests.
type MemStorage struct {
	mu     sync.Mutex
	name   string
	maxMsg int64
	maxSeg int64
	msgs   []engine.Record
	groups map[string]uint64
	minOff uint64
	segs   []memSegment
	closed bool
	notify chan struct{}
}

func NewMemStorage(name string, maxMsg, maxSeg int64) *MemStorage {
	return &MemStorage{
		name:   name,
		maxMsg: maxMsg,
		maxSeg: maxSeg,
		groups: make(map[string]uint64),
		notify: make(chan struct{}),
	}
}

// Fetch returns up to maxCount messages starting at offset, blocking up to maxWait.
func (m *MemStorage) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]engine.Record, error) {
	if maxCount <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(maxWait)
	for {
		m.mu.Lock()
		closed := m.closed
		minOff := m.minOff
		published := uint64(len(m.msgs))
		notify := m.notify
		m.mu.Unlock()

		if closed {
			return nil, engine.ErrQueueClosed
		}
		if offset < minOff {
			return nil, engine.ErrOffsetTruncated
		}
		if offset < published {
			m.mu.Lock()
			start := offset
			end := min(start+uint64(maxCount), published)
			msgs := append([]engine.Record(nil), m.msgs[start:end]...)
			m.mu.Unlock()
			return msgs, nil
		}
		if maxWait <= 0 {
			return nil, engine.ErrNoData
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, engine.ErrNoData
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-notify:
			timer.Stop()
		case <-timer.C:
			return nil, engine.ErrNoData
		}
	}
}

func (m *MemStorage) Name() string { return m.name }

func (m *MemStorage) broadcastLocked() {
	old := m.notify
	m.notify = make(chan struct{})
	close(old)
}

func (m *MemStorage) Append(ctx context.Context, payload []byte) (uint64, error) {
	offs, err := m.AppendBatch(ctx, [][]byte{payload})
	if err != nil {
		return 0, err
	}
	return offs[0], nil
}

func (m *MemStorage) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, engine.ErrQueueClosed
	}
	out := make([]uint64, len(payloads))
	for i, p := range payloads {
		if int64(len(p)) > m.maxMsg {
			return nil, engine.ErrMessageTooLarge
		}
		recSize := recordOverhead + int64(len(p))
		// Roll before writing when the record would not fit, mirroring
		// engine.Queue: a closed segment never exceeds the cap.
		if len(m.segs) == 0 {
			m.segs = append(m.segs, memSegment{baseOff: uint64(len(m.msgs))})
		} else if m.segs[len(m.segs)-1].sizeBytes+recSize > m.maxSeg {
			m.segs = append(m.segs, memSegment{baseOff: uint64(len(m.msgs))})
		}
		off := uint64(len(m.msgs))
		m.msgs = append(m.msgs, engine.Record{
			Offset:  off,
			Payload: append([]byte(nil), p...),
		})
		cur := &m.segs[len(m.segs)-1]
		cur.sizeBytes += recSize
		cur.lastTS = time.Now()
		cur.hasRecords = true
		out[i] = off
	}
	m.broadcastLocked()
	return out, nil
}

func (m *MemStorage) Read(ctx context.Context, off uint64) (engine.Record, error) {
	if err := ctx.Err(); err != nil {
		return engine.Record{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if off < m.minOff {
		return engine.Record{}, engine.ErrOffsetTruncated
	}
	if off >= uint64(len(m.msgs)) {
		return engine.Record{}, engine.ErrOffsetNotFound
	}
	return m.msgs[off], nil
}

func (m *MemStorage) ReadBatch(ctx context.Context, start uint64, n int) ([]engine.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if start < m.minOff {
		return nil, engine.ErrOffsetTruncated
	}
	if start >= uint64(len(m.msgs)) {
		return nil, engine.ErrOffsetNotFound
	}
	end := min(start+uint64(n), uint64(len(m.msgs)))
	return append([]engine.Record(nil), m.msgs[start:end]...), nil
}

func (m *MemStorage) LastOffset() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint64(len(m.msgs))
}

func (m *MemStorage) MinOffset() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.minOff
}

func (m *MemStorage) CommitOffset(ctx context.Context, group string, off uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[group] = off
	return nil
}

func (m *MemStorage) Committed(ctx context.Context, group string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.groups[group], nil
}

func (m *MemStorage) MaxAppendPayloadBytes() int64 {
	return m.maxMsg
}

func (m *MemStorage) Segments() []engine.SegmentInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	infos := make([]engine.SegmentInfo, 0, len(m.segs))
	for i, s := range m.segs {
		next := uint64(len(m.msgs))
		if i+1 < len(m.segs) {
			next = m.segs[i+1].baseOff
		}
		infos = append(infos, engine.SegmentInfo{
			BaseOffset: s.baseOff,
			NextOffset: next,
			SizeBytes:  s.sizeBytes,
			IsActive:   i == len(m.segs)-1,
		})
	}
	return infos
}

// FloorOlderThan mirrors engine.Queue.FloorOlderThan: closed segments are walked oldest to newest and qualify only when their last record is strictly before olderThan; the active segment is never included.
func (m *MemStorage) FloorOlderThan(ctx context.Context, olderThan time.Time) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, engine.ErrQueueClosed
	}

	floor := m.minOff
	for i, s := range m.segs {
		if i == len(m.segs)-1 {
			break // active segment
		}
		if !s.hasRecords || !s.lastTS.Before(olderThan) {
			break
		}
		floor = m.segs[i+1].baseOff
	}
	return floor, nil
}

// FloorForSize mirrors engine.Queue.FloorForSize: drop whole oldest closed segments while the total size exceeds maxBytes; never the active segment.
func (m *MemStorage) FloorForSize(ctx context.Context, maxBytes int64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, engine.ErrQueueClosed
	}
	if maxBytes <= 0 {
		return m.minOff, nil
	}

	var total int64
	for _, s := range m.segs {
		total += s.sizeBytes
	}

	floor := m.minOff
	for i, s := range m.segs {
		if i == len(m.segs)-1 || total <= maxBytes {
			break
		}
		total -= s.sizeBytes
		floor = m.segs[i+1].baseOff
	}
	return floor, nil
}

func (m *MemStorage) TruncateBefore(ctx context.Context, off uint64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if off <= m.minOff {
		return 0, nil
	}
	if off > uint64(len(m.msgs)) {
		off = uint64(len(m.msgs))
	}
	reclaimed := int64(off-m.minOff) * 32
	m.minOff = off
	return reclaimed, nil
}

var _ engine.Storage = (*MemStorage)(nil)
