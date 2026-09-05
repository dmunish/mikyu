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

// ReadFrame reads opcode and reads payload directly into the destination slice.
func ReadFrame(r *bufio.Reader) (op byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return 0, nil, errors.New("zero-length frame")
	}

	var opBuf [1]byte
	if _, err := io.ReadFull(r, opBuf[:]); err != nil {
		return 0, nil, err
	}

	payloadLen := length - 1
	if payloadLen == 0 {
		return opBuf[0], nil, nil
	}

	payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}

	return opBuf[0], payload, nil
}

// WriteFrame writes an opcode and payload using a pooled buffer, preserving enlarged buffers.
func WriteFrame(w io.Writer, op byte, payload []byte) error {
	total := uint32(1 + len(payload))

	bufPtr := framePool.Get().(*[]byte)
	buf := *bufPtr

	if uint32(len(buf)) < 4+total {
		buf = make([]byte, 4+total)
		*bufPtr = buf // Retain expanded buffer pointer for pool reuse
	}

	binary.BigEndian.PutUint32(buf[0:4], total)
	buf[4] = op
	copy(buf[5:], payload)

	_, err := w.Write(buf[:4+total])
	framePool.Put(bufPtr) // Always return buffer pointer to pool
	return err
}
