package protocol

import (
	"encoding/binary"
	"errors"
)

var ErrTruncatedBody = errors.New("truncated frame body")

// Reader provides bounds-checked sequential decoding of binary frame payloads.
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

func (r *Reader) ReadU64() (uint64, error) {
	if r.i+8 > len(r.b) {
		return 0, ErrTruncatedBody
	}
	v := binary.BigEndian.Uint64(r.b[r.i : r.i+8])
	r.i += 8
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

// Buffer provides zero-allocation builder helpers for constructing protocol payload bytes.
type Buffer struct{ b []byte }

func (buf *Buffer) WriteU32(v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	buf.b = append(buf.b, tmp[:]...)
}

func (buf *Buffer) WriteU64(v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	buf.b = append(buf.b, tmp[:]...)
}

func (buf *Buffer) WriteBytes(data []byte) {
	buf.WriteU32(uint32(len(data)))
	buf.b = append(buf.b, data...)
}

func (buf *Buffer) Bytes() []byte {
	return buf.b
}
