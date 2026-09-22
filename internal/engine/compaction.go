package engine

import (
	"fmt"
	"log"
)

// keyIndexWarnLimit is the documented target size of the key index; exceeding
// it logs a warning and is observable through the key_index gauge.
var keyIndexWarnLimit = 1_000_000

// buildKeyIndex walks every record once, keeping the newest offset per group.
func (q *Queue) buildKeyIndex() error {
	q.keyIndex = make(map[string]uint64)
	for _, seg := range q.segments {
		count := seg.indexCount()
		for i := 0; i < count; i++ {
			off := seg.baseOff + uint64(i)
			pos, recLen, err := seg.readIndexEntry(off)
			if err != nil || recLen < minRecordSize-recordLengthSize {
				continue
			}
			rec, ok := readRecordBytes(seg, pos, recLen)
			if !ok {
				return fmt.Errorf("queue %q: reading record at offset %d during key index build", q.name, off)
			}
			_, _, group, _, err := decodeRecord(rec[recordLengthSize:])
			if err != nil {
				log.Printf("mikyu: queue %q: skipping corrupt record at offset %d during key index build", q.name, off)
				continue
			}
			if len(group) > 0 {
				q.keyIndex[string(group)] = off
			}
		}
	}
	q.warnKeyIndexSizeLocked()
	return nil
}

// warnKeyIndexSizeLocked updates the key index gauge and warns past the target.
func (q *Queue) warnKeyIndexSizeLocked() {
	q.m.keyIndexSize.Set(int64(len(q.keyIndex)))
	if len(q.keyIndex) > keyIndexWarnLimit {
		log.Printf("mikyu: queue %q key index holds %d groups (target %d); consider reducing keyed records", q.name, len(q.keyIndex), keyIndexWarnLimit)
	}
}

// supersededLocked reports whether a keyed record at off has a newer same-group record; caller holds q.mu.
func (q *Queue) supersededLocked(group []byte, off uint64) bool {
	if len(group) == 0 || q.keyIndex == nil {
		return false
	}
	return q.keyIndex[string(group)] != off
}

// superseded reports whether a keyed record was superseded by a newer same-group record.
func (q *Queue) superseded(group []byte, off uint64) bool {
	if len(group) == 0 || q.keyIndex == nil {
		return false
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.supersededLocked(group, off)
}

// compactClosedLocked physically drops superseded records from every closed segment; caller holds q.mu.
func (q *Queue) compactClosedLocked() error {
	if !q.cfg.KeyedCompaction() {
		return nil
	}
	for _, seg := range q.segments {
		if seg == q.active {
			break
		}
		if err := q.compactSegmentLocked(seg); err != nil {
			return err
		}
	}
	return nil
}

// compactSegmentLocked rewrites seg in place, dropping records superseded by a newer same-group record.
func (q *Queue) compactSegmentLocked(seg *segment) error {
	count := seg.indexCount()
	if count == 0 {
		return nil
	}

	type keepRec struct {
		slot int
		raw  []byte
	}
	var keeps []keepRec
	keptSlot := make([]bool, count)
	var evicted []uint64
	dropped := false

	for i := 0; i < count; i++ {
		off := seg.baseOff + uint64(i)
		pos, recLen, err := seg.readIndexEntry(off)
		if err != nil || recLen < minRecordSize-recordLengthSize {
			continue // already zeroed
		}
		rec, ok := readRecordBytes(seg, pos, recLen)
		if !ok {
			return fmt.Errorf("queue %q: reading record at offset %d during compaction", q.name, off)
		}
		_, _, group, payload, err := decodeRecord(rec[recordLengthSize:])
		if err != nil {
			// Corrupt records are kept: dropping one could remove a group's newest record.
			keeps = append(keeps, keepRec{slot: i, raw: rec})
			keptSlot[i] = true
			continue
		}
		g := q.cfg.CompactionPolicy.groupOf(group, payload)
		if len(g) > 0 && q.keyIndex[string(g)] != off {
			dropped = true
			evicted = append(evicted, off)
			continue
		}
		keeps = append(keeps, keepRec{slot: i, raw: rec})
		keptSlot[i] = true
	}

	if !dropped {
		return nil
	}

	// Unmap before rewriting: a live mapping would observe the file mid-rewrite
	// and the files cannot be truncated while mapped.
	if err := seg.unmap(); err != nil {
		return err
	}
	var buf []byte
	for _, k := range keeps {
		buf = append(buf, k.raw...)
	}
	if err := seg.logFile.Truncate(0); err != nil {
		return err
	}
	if _, err := seg.logFile.Write(buf); err != nil {
		return err
	}
	if err := syncData(seg.logFile); err != nil {
		return err
	}
	pos := int64(0)
	for _, k := range keeps {
		if err := seg.writeIndexAt(int64(k.slot)*rawIndexEntrySize, pos, int64(len(k.raw))); err != nil {
			return err
		}
		pos += int64(len(k.raw))
	}
	// Zero the slots of dropped records; kept slots were just rewritten and the
	// file keeps its original size, so the segment's offset span is unchanged.
	for i := 0; i < count; i++ {
		if !keptSlot[i] {
			if err := seg.writeIndexAt(int64(i)*rawIndexEntrySize, 0, 0); err != nil {
				return err
			}
		}
	}
	if err := seg.idxFile.Sync(); err != nil {
		return err
	}
	seg.pos = int64(len(buf))

	if err := seg.enableMMap(); err != nil {
		return err
	}
	q.cache.Evict(evicted...)
	return nil
}

// readRecordBytes returns the full on-disk record at pos, mapped or read.
func readRecordBytes(seg *segment, pos, recLen int64) ([]byte, bool) {
	b := make([]byte, recLen)
	if seg.mmapData != nil && len(seg.mmapData) >= int(pos+recLen) {
		copy(b, seg.mmapData[pos:pos+recLen])
		return b, true
	}
	if _, err := seg.logFile.ReadAt(b, pos); err != nil {
		return nil, false
	}
	return b, true
}
