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
	buf := encPool.Get().(*bytes.Buffer)
	buf.Reset()

	idBytes := id[:]              // Fixed 16-byte slice
	idLen := uint16(len(idBytes)) // Always 16

	// Layout: [4B Framing Length][4B CRC32][2B ID Len (16)][16B Raw UUID][8B TS Nano][M-B Payload]
	bodyLen := uint32(4 + 2 + 16 + 8 + len(payload))
	totalLen := 4 + bodyLen

	var hdr [10]byte
	binary.BigEndian.PutUint32(hdr[0:4], bodyLen)
	binary.BigEndian.PutUint16(hdr[8:10], idLen)

	buf.Write(hdr[:])
	buf.Write(idBytes)

	var tsNano [8]byte
	binary.BigEndian.PutUint64(tsNano[:], uint64(ts.UnixNano()))
	buf.Write(tsNano[:])
	buf.Write(payload)

	b := buf.Bytes()
	sum := crc32.ChecksumIEEE(b[8:])
	binary.BigEndian.PutUint32(b[4:8], sum)

	rec := make([]byte, totalLen)
	copy(rec, b)
	encPool.Put(buf)
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
