package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"time"
)

var ErrCorruptRecord = errors.New("corrupt record")

const (
	recordLengthSize   = 4  // 4B Body Length
	recordCRCSize      = 4  // 4B CRC32
	recordIDLenSize    = 2  // 2B ID Length
	recordUUIDSize     = 16 // 16B Raw UUID
	recordTSSize       = 8  // 8B Unix Nanoseconds
	recordGroupLenSize = 2  // 2B Group Length

	recordHeaderSize = recordLengthSize + recordCRCSize                                                                        // 8B prefix before body
	minRecordSize    = recordLengthSize + recordCRCSize + recordIDLenSize + recordUUIDSize + recordTSSize + recordGroupLenSize // 36B

	offLength   = 0
	offCRC      = 4
	offIDLen    = 8
	offUUID     = 10
	offTS       = 26
	offGroupLen = 34
	offBody     = 36
)

var encPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// encodeRecord writes one record frame into dst, allocating when dst is too small.
func encodeRecord(dst []byte, id [16]byte, ts time.Time, group, payload []byte) []byte {
	rec := frameRecord(dst, id, ts, group, len(payload))
	copy(rec[offBody+len(group):], payload)
	return stampCRC(rec)
}

// frameRecord writes the header and group into dst, sizing the payload region
// to payloadLen bytes and leaving it zeroed for the caller to fill — with the
// plaintext, or with a sealed payload whose region is payloadLen = plaintext+gcmOverhead.
func frameRecord(dst []byte, id [16]byte, ts time.Time, group []byte, payloadLen int) []byte {
	bodyLen := uint32(recordCRCSize + recordIDLenSize + recordUUIDSize + recordTSSize + recordGroupLenSize + len(group) + payloadLen)
	totalLen := int(recordLengthSize) + int(bodyLen)
	if cap(dst) < totalLen {
		dst = make([]byte, totalLen)
	}
	rec := dst[:totalLen]

	binary.BigEndian.PutUint32(rec[offLength:offCRC], bodyLen)
	binary.BigEndian.PutUint16(rec[offIDLen:offUUID], recordUUIDSize)
	copy(rec[offUUID:offTS], id[:])
	binary.BigEndian.PutUint64(rec[offTS:offGroupLen], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint16(rec[offGroupLen:offBody], uint16(len(group)))
	copy(rec[offBody:], group)
	return rec
}

// stampCRC writes the body CRC; the payload region must already hold its final bytes.
func stampCRC(rec []byte) []byte {
	binary.BigEndian.PutUint32(rec[offCRC:offIDLen], crc32.ChecksumIEEE(rec[offIDLen:]))
	return rec
}

func decodeRecord(buf []byte) (id string, ts time.Time, group, payload []byte, err error) {
	if len(buf) < (minRecordSize - recordLengthSize) {
		return "", time.Time{}, nil, nil, ErrCorruptRecord
	}
	sum := binary.BigEndian.Uint32(buf[0:4])
	if crc32.ChecksumIEEE(buf[4:]) != sum {
		return "", time.Time{}, nil, nil, ErrCorruptRecord
	}
	idLen := binary.BigEndian.Uint16(buf[4:6])
	if len(buf) < (minRecordSize-recordLengthSize)+int(idLen)-recordUUIDSize {
		return "", time.Time{}, nil, nil, ErrCorruptRecord
	}
	idStart := 6
	idEnd := idStart + int(idLen)
	tsStart := idEnd
	tsEnd := tsStart + recordTSSize
	if len(buf) < tsEnd+recordGroupLenSize {
		return "", time.Time{}, nil, nil, ErrCorruptRecord
	}
	tsNano := binary.BigEndian.Uint64(buf[tsStart:tsEnd])
	groupLen := int(binary.BigEndian.Uint16(buf[tsEnd : tsEnd+recordGroupLenSize]))
	groupStart := tsEnd + recordGroupLenSize
	groupEnd := groupStart + groupLen
	if len(buf) < groupEnd {
		return "", time.Time{}, nil, nil, ErrCorruptRecord
	}

	var rawID [16]byte
	copy(rawID[:], buf[idStart:idEnd])
	if groupLen > 0 {
		group = buf[groupStart:groupEnd]
	}
	payload = buf[groupEnd:]
	return formatRecordID(rawID), time.Unix(0, int64(tsNano)), group, payload, nil
}
