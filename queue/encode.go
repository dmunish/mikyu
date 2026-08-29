package queue

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

func encodeRecord(id string, ts time.Time, payload []byte) []byte {
	idBytes := []byte(id)
	body := make([]byte, 4, 4+8+2+len(idBytes)+len(payload))

	var idLen [2]byte
	binary.BigEndian.PutUint16(idLen[:], uint16(len(idBytes)))

	var tsNano [8]byte
	binary.BigEndian.PutUint64(tsNano[:], uint64(ts.UnixNano()))

	body = append(body, idLen[:]...)
	body = append(body, idBytes...)
	body = append(body, tsNano[:]...)
	body = append(body, payload...)

	sum := crc32.ChecksumIEEE(body[4:])
	binary.BigEndian.PutUint32(body[0:4], sum)

	rec := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(body)))
	copy(rec[4:], body)
	return rec
}

func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error) {
	if len(buf) < 6 {
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
