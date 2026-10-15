package engine

import (
	"encoding/binary"
	"os"
	"slices"
	"sync"
	"time"
)

// sendfileMinPayload is the payload size above which the wire server streams payloads straight from disk.
const sendfileMinPayload = 16 << 10

// PayloadSpan describes one record's wire metadata plus, when file-backed, where the kernel can read its payload.
type PayloadSpan struct {
	Offset     uint64
	ID         string
	Timestamp  time.Time
	Group      []byte
	PayloadLen int
	Payload    []byte      // set only when the payload is inlined below the send threshold
	File       *os.File    // read handle for zero-copy sends; nil when inlined
	FileOff    int64       // payload start within File
	SendMu     *sync.Mutex // serializes position-based sends over File
}

// SpanSource resolves a contiguous record range's on-disk payload locations.
type SpanSource interface {
	PayloadSpans(start uint64, count int) ([]PayloadSpan, error)
}

// PayloadSpans resolves the longest file-backed prefix of [start, start+count), inlining sub-threshold payloads.
func (q *Queue) PayloadSpans(start uint64, count int) ([]PayloadSpan, error) {
	if count <= 0 {
		return nil, nil
	}
	q.hasReader.Store(true)
	begin := time.Now()

	q.mu.RLock()
	closed, minOff, end, segs := q.closed, q.minOffset, q.publishedOffset, q.segments
	q.mu.RUnlock()
	if closed {
		return nil, ErrQueueClosed
	}
	if q.cfg.KeyedCompaction() || q.cfg.EncryptedPayloads() {
		return nil, nil
	}
	if start < minOff {
		return nil, ErrOffsetTruncated
	}
	end = min(start+uint64(count), end)

	spans := make([]PayloadSpan, 0, int(end-start))
	maxPayload := 0
	var outBytes int64
	for off := start; off < end; off++ {
		sp, ok := payloadSpan(segs, off)
		if !ok {
			break
		}
		if sp.Payload != nil {
			outBytes += int64(len(sp.Payload))
		} else {
			outBytes += int64(sp.PayloadLen)
		}
		maxPayload = max(maxPayload, sp.PayloadLen)
		spans = append(spans, sp)
	}
	if len(spans) == 0 || maxPayload < sendfileMinPayload {
		return nil, nil
	}
	q.recordRead(len(spans), outBytes, begin)
	return spans, nil
}

// payloadSpan resolves one record's span against a segment snapshot; ok is false when it cannot be served without a copy.
func payloadSpan(segs []*segment, off uint64) (PayloadSpan, bool) {
	if len(segs) == 0 || off < segs[0].baseOff {
		return PayloadSpan{}, false
	}
	idx, _ := slices.BinarySearchFunc(segs, off, func(seg *segment, target uint64) int {
		if target < seg.baseOff {
			return 1
		}
		return -1
	})
	if idx == 0 {
		return PayloadSpan{}, false
	}
	seg := segs[idx-1]
	pos, recLen, err := seg.readIndexEntry(off)
	if err != nil || recLen < minRecordSize-recordLengthSize {
		return PayloadSpan{}, false
	}

	var hdr [minRecordSize]byte
	if !seg.readBytes(hdr[:], pos) {
		return PayloadSpan{}, false
	}
	groupLen := int(binary.BigEndian.Uint16(hdr[offGroupLen:offBody]))
	payloadLen := int(recLen) - minRecordSize - groupLen
	if payloadLen < 0 {
		return PayloadSpan{}, false
	}

	if payloadLen < sendfileMinPayload {
		body := make([]byte, recLen-recordLengthSize)
		if !seg.readBytes(body, pos+recordLengthSize) {
			return PayloadSpan{}, false
		}
		id, ts, group, payload, err := decodeRecord(body)
		if err != nil {
			return PayloadSpan{}, false
		}
		return PayloadSpan{Offset: off, ID: id, Timestamp: ts, Group: group, PayloadLen: len(payload), Payload: payload}, true
	}

	f, err := seg.sendFD()
	if err != nil {
		return PayloadSpan{}, false
	}
	var group []byte
	if groupLen > 0 {
		group = make([]byte, groupLen)
		if !seg.readBytes(group, pos+offBody) {
			return PayloadSpan{}, false
		}
	}
	var rawID [16]byte
	copy(rawID[:], hdr[offUUID:offTS])
	return PayloadSpan{
		Offset:     off,
		ID:         formatRecordID(rawID),
		Timestamp:  time.Unix(0, int64(binary.BigEndian.Uint64(hdr[offTS:offTS+8]))),
		Group:      group,
		PayloadLen: payloadLen,
		File:       f,
		FileOff:    pos + minRecordSize + int64(groupLen),
		SendMu:     &seg.sendMu,
	}, true
}
