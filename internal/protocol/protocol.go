package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const (
	OpCreateQueue byte = 0x01
	OpProduce     byte = 0x02
	OpConsume     byte = 0x03
	OpError       byte = 0xFF
)

var ErrTruncatedBody = errors.New("truncated frame body")

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
	buf := make([]byte, 4+total)

	binary.BigEndian.PutUint32(buf[0:4], total)
	buf[4] = op
	copy(buf[5:], payload)

	_, err := w.Write(buf)
	return err
}

// Reader decodes values out of a frame payload in order.
type Reader struct {
	b []byte
	i int
}

func NewReader(b []byte) *Reader { return &Reader{b: b} }

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

// Buffer encodes values into a frame payload in order.
type Buffer struct{ b []byte }

func (buf *Buffer) WriteU32(v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	buf.b = append(buf.b, tmp[:]...)
}

func (buf *Buffer) WriteBytes(data []byte) {
	buf.WriteU32(uint32(len(data)))
	buf.b = append(buf.b, data...)
}

func (buf *Buffer) Bytes() []byte { return buf.b }
