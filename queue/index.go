package queue

import (
	"encoding/binary"
	"io"
)

type indexEntry struct {
	pos int64 // File byte offset where length prefix starts
	len int64 // Total length of length prefix + record body
}

// rebuildIndex scans the WAL file on startup, rebuilds the index, and truncates torn writes.
func (q *Queue) rebuildIndex() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var pos int64
	for {
		var lenBuf [4]byte
		n, err := q.file.ReadAt(lenBuf[:], pos)
		if n < 4 {
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			break
		}

		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		total := int64(4) + int64(bodyLen)

		body := make([]byte, bodyLen)
		n, err = q.file.ReadAt(body, pos+4)
		if int64(n) < int64(bodyLen) {
			break // torn write tail
		}
		if err != nil && err != io.EOF {
			return err
		}

		if _, _, err := decodeRecord(body); err != nil {
			break // corrupt checksum
		}

		q.index = append(q.index, indexEntry{pos: pos, len: total})
		pos += total
	}

	q.pos = pos
	return q.file.Truncate(pos)
}
