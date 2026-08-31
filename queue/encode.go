package queue

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"
)

var encPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func encodeRecord(id string, ts time.Time, payload []byte) []byte {
	buf := encPool.Get().(*bytes.Buffer)
	buf.Reset()

	idBytes := []byte(id)
	idLen := uint16(len(idBytes))

	// Record Body layout: [4B CRC32][2B ID Len][N-B ID][8B TS Nano][M-B Payload]
	bodyLen := uint32(4 + 2 + len(idBytes) + 8 + len(payload))
	totalLen := 4 + bodyLen

	var hdr [10]byte
	binary.BigEndian.PutUint32(hdr[0:4], bodyLen) // framing length prefix
	// hdr[4:8] reserved for CRC32
	binary.BigEndian.PutUint16(hdr[8:10], idLen)

	buf.Write(hdr[:])
	buf.Write(idBytes)

	var tsNano [8]byte
	binary.BigEndian.PutUint64(tsNano[:], uint64(ts.UnixNano()))
	buf.Write(tsNano[:])
	buf.Write(payload)

	b := buf.Bytes()
	// Checksum is calculated over body bytes after CRC field: idLen (2B) + id + TS (8B) + Payload
	sum := crc32.ChecksumIEEE(b[8:])
	binary.BigEndian.PutUint32(b[4:8], sum)

	// Copy to an owned return slice so buffer can safely return to pool
	rec := make([]byte, totalLen)
	copy(rec, b)
	encPool.Put(buf)
	return rec
}

func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error) {
	// buf is the record body: [4B CRC32][2B ID Len][ID][8B TS Nano][Payload]
	if len(buf) < 14 { // 4B CRC + 2B ID Len + 8B TS
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

	return string(buf[idStart:idEnd]), time.Unix(0, int64(tsNano)), buf[tsEnd:], nil
}
