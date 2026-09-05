package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"time"
	"uuid" // Go 1.27+ stdlib UUID package
)

var ErrCorruptRecord = errors.New("corrupt record")

var encPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func encodeRecord(id uuid.UUID, ts time.Time, payload []byte) []byte {
	// Frame Layout:
	// [4B Body Length][4B CRC32][2B ID Len (16)][16B Raw UUID][8B TS Nano][Payload]
	payloadLen := len(payload)
	bodyLen := uint32(4 + 2 + 16 + 8 + payloadLen) // CRC + IDLen + UUID + TS + Payload
	totalLen := 4 + bodyLen

	rec := make([]byte, totalLen)

	binary.BigEndian.PutUint32(rec[0:4], bodyLen)
	// rec[4:8] reserved for CRC32 checksum
	binary.BigEndian.PutUint16(rec[8:10], 16)
	copy(rec[10:26], id[:])
	binary.BigEndian.PutUint64(rec[26:34], uint64(ts.UnixNano()))
	copy(rec[34:], payload)

	sum := crc32.ChecksumIEEE(rec[8:])
	binary.BigEndian.PutUint32(rec[4:8], sum)

	return rec
}

func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error) {
	if len(buf) < 14 {
		return "", time.Time{}, nil, ErrCorruptRecord
	}
	sum := binary.BigEndian.Uint32(buf[0:4])
	if crc32.ChecksumIEEE(buf[4:]) != sum {
		return "", time.Time{}, nil, ErrCorruptRecord
	}
	idLen := binary.BigEndian.Uint16(buf[4:6])
	if len(buf) < 14+int(idLen) {
		return "", time.Time{}, nil, ErrCorruptRecord
	}
	idStart := 6
	idEnd := idStart + int(idLen)
	tsStart := idEnd
	tsEnd := tsStart + 8
	tsNano := binary.BigEndian.Uint64(buf[tsStart:tsEnd])

	// Convert raw 16-byte UUID back to UUID string representation on decode/read boundary
	rawID := uuid.UUID(buf[idStart:idEnd])
	return rawID.String(), time.Unix(0, int64(tsNano)), buf[tsEnd:], nil
}
