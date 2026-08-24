package queue

import (
	"encoding/binary"
	"hash/crc32"
)

func encodeRecord(id string, payload []byte) []byte {
	idBytes := []byte(id)
	body := make([]byte, 4, 4+2+len(idBytes)+len(payload)) // checksum placeholder

	var idLen [2]byte
	binary.BigEndian.PutUint16(idLen[:], uint16(len(idBytes)))
	body = append(body, idLen[:]...)
	body = append(body, idBytes...)
	body = append(body, payload...)

	sum := crc32.ChecksumIEEE(body[4:])
	binary.BigEndian.PutUint32(body[0:4], sum)

	rec := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(body)))
	copy(rec[4:], body)
	return rec
}

func decodeRecord(buf []byte) (id string, payload []byte, err error) {
	if len(buf) < 6 {
		return "", nil, ErrCorruptRecord
	}
	sum := binary.BigEndian.Uint32(buf[0:4])
	if crc32.ChecksumIEEE(buf[4:]) != sum {
		return "", nil, ErrCorruptRecord
	}
	idLen := binary.BigEndian.Uint16(buf[4:6])
	if len(buf) < 6+int(idLen) {
		return "", nil, ErrCorruptRecord
	}
	return string(buf[6 : 6+idLen]), buf[6+idLen:], nil
}
