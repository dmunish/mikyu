package tcp

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// Opcodes
const (
	OpCreateQueue byte = 0x01
	OpProduce     byte = 0x02
	OpRead        byte = 0x03
	OpTail        byte = 0x04
	OpError       byte = 0xFF
)

var ErrTruncatedBody = errors.New("truncated frame body")

/*
Wire Format:

 +-----------------+---------------+----------------------+
 | Length (uint32) | Opcode (u8)   | Payload              |
 | 4 bytes         | 1 byte        | (Length - 1) bytes   |
 +-----------------+---------------+----------------------+
*/

func readFrame(r *bufio.Reader) (byte, []byte, error) {
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

	opcode := buf[0]
	payload := buf[1:]
	return opcode, payload, nil
}

func writeFrame(w io.Writer, opcode byte, payload []byte) error {
	totalLen := uint32(1 + len(payload))
	buf := make([]byte, 4+totalLen)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	buf[4] = opcode
	copy(buf[5:], payload)

	_, err := w.Write(buf)
	return err
}

type Reader struct {
	b []byte
	i int
}

func NewReader(b []byte) *Reader {
	return &Reader{b: b}
}

func (r *Reader) ReadU32() (uint32, error) {
	if r.i+4 > len(r.b) {
		return 0, ErrTruncatedBody
	}
	v := binary.BigEndian.Uint32(r.b[r.i : r.i+4])
	r.i += 4
	return v, nil
}

func (r *Reader) ReadBytes() ([]byte, error) {
	n, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if r.i+int(n) > len(r.b) {
		return nil, ErrTruncatedBody
	}
	b := r.b[r.i : r.i+int(n)]
	r.i += int(n)
	return b, nil
}

type Buffer struct {
	b []byte
}

func (b *Buffer) WriteU32(v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	b.b = append(b.b, tmp[:]...)
}

func (b *Buffer) WriteBytes(data []byte) {
	b.WriteU32(uint32(len(data)))
	b.b = append(b.b, data...)
}

func (b *Buffer) Bytes() []byte {
	return b.b
}
