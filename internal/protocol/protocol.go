package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	OpCreateQueue  byte = 0x01
	OpProduce      byte = 0x02
	OpRead         byte = 0x03
	OpTail         byte = 0x04
	OpCommitOffset byte = 0x05
	OpFetchOffset  byte = 0x06
	OpError        byte = 0xFF
)

const (
	ErrCodeUnknown        byte = 0x00
	ErrCodeQueueExists    byte = 0x01
	ErrCodeQueueNotFound  byte = 0x02
	ErrCodeOffsetNotFound byte = 0x03
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

// One-byte error code followed by a human-readable message.
func EncodeError(code byte, msg string) []byte {
	b := make([]byte, 1+len(msg))
	b[0] = code
	copy(b[1:], msg)
	return b
}

func DecodeError(body []byte) (code byte, msg string) {
	if len(body) == 0 {
		return ErrCodeUnknown, ""
	}
	return body[0], string(body[1:])
}
