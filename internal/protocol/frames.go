package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

var framePool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}

/*
Wire format:

 +-----------------+---------------+----------------------+
 | Length (uint32) | Opcode (u8)   | Payload              |
 | 4 bytes         | 1 byte        | (Length - 1) bytes   |
 +-----------------+---------------+----------------------+
*/

// ReadFrame reads a 4-byte big-endian length prefix followed by opcode and payload.
func ReadFrame(r *bufio.Reader) (op byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return 0, nil, errors.New("zero-length frame")
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}

	return buf[0], buf[1:], nil
}

// WriteFrame writes an opcode and payload using a pooled buffer to prevent memory allocations.
func WriteFrame(w io.Writer, op byte, payload []byte) error {
	total := uint32(1 + len(payload))

	bufPtr := framePool.Get().(*[]byte)
	buf := *bufPtr

	if uint32(len(buf)) < 4+total {
		buf = make([]byte, 4+total)
	}

	binary.BigEndian.PutUint32(buf[0:4], total)
	buf[4] = op
	copy(buf[5:], payload)

	_, err := w.Write(buf[:4+total])
	if uint32(len(*bufPtr)) >= 4+total {
		framePool.Put(bufPtr)
	}
	return err
}
