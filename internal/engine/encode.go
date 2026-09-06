package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"time"
	"uuid" // Go standard library UUID package
)

var ErrCorruptRecord = errors.New("corrupt record")

const (
	recordLengthSize = 4  // 4B Body Length
	recordCRCSize    = 4  // 4B CRC32
	recordIDLenSize  = 2  // 2B ID Length
	recordUUIDSize   = 16 // 16B Raw UUID
	recordTSSize     = 8  // 8B Unix Nanoseconds

	recordHeaderSize = recordLengthSize + recordCRCSize                                                   // 8B prefix before body
	minRecordSize    = recordLengthSize + recordCRCSize + recordIDLenSize + recordUUIDSize + recordTSSize // 34B

	offLength = 0
	offCRC    = 4
	offIDLen  = 8
	offUUID   = 10
	offTS     = 26
	offBody   = 34
)

var encPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func encodeRecord(id uuid.UUID, ts time.Time, payload []byte) []byte {
	// Frame Layout:
	// [4B Body Length][4B CRC32][2B ID Len (16)][16B Raw UUID][8B TS Nano][Payload]
	payloadLen := len(payload)
	bodyLen := uint32(recordCRCSize + recordIDLenSize + recordUUIDSize + recordTSSize + payloadLen)
	totalLen := recordLengthSize + bodyLen

	rec := make([]byte, totalLen)

	binary.BigEndian.PutUint32(rec[offLength:offCRC], bodyLen)
	// rec[offCRC:offIDLen] reserved for CRC32 checksum
	binary.BigEndian.PutUint16(rec[offIDLen:offUUID], recordUUIDSize)
	copy(rec[offUUID:offTS], id[:])
	binary.BigEndian.PutUint64(rec[offTS:offBody], uint64(ts.UnixNano()))
	copy(rec[offBody:], payload)

	sum := crc32.ChecksumIEEE(rec[offIDLen:])
	binary.BigEndian.PutUint32(rec[offCRC:offIDLen], sum)

	return rec
}

func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error) {
	if len(buf) < (minRecordSize - recordLengthSize) {
		return "", time.Time{}, nil, ErrCorruptRecord
	}
	sum := binary.BigEndian.Uint32(buf[0:4])
	if crc32.ChecksumIEEE(buf[4:]) != sum {
		return "", time.Time{}, nil, ErrCorruptRecord
	}
	idLen := binary.BigEndian.Uint16(buf[4:6])
	if len(buf) < (minRecordSize-recordLengthSize)+int(idLen)-recordUUIDSize {
		return "", time.Time{}, nil, ErrCorruptRecord
	}
	idStart := 6
	idEnd := idStart + int(idLen)
	tsStart := idEnd
	tsEnd := tsStart + recordTSSize
	tsNano := binary.BigEndian.Uint64(buf[tsStart:tsEnd])

	rawID := uuid.UUID(buf[idStart:idEnd])
	return rawID.String(), time.Unix(0, int64(tsNano)), buf[tsEnd:], nil
}
