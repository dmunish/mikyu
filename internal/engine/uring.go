package engine

import (
	"errors"
	"os"
)

// errUringUnsupported reports that the host has no usable io_uring.
var errUringUnsupported = errors.New("io_uring unavailable")

// Ring is a per-data-directory io_uring submission ring; NewRing returns nil on hosts without io_uring.
type Ring struct{ ring *uringRing }

// NewRing opens an io_uring ring, or nil when the host cannot provide one.
func NewRing() *Ring {
	r, err := newUringRing()
	if err != nil {
		return nil
	}
	return &Ring{ring: r}
}

// Close tears the ring down; call it only after every sink registered with it has closed.
func (r *Ring) Close() error {
	if r == nil || r.ring == nil {
		return nil
	}
	return r.ring.Close()
}

type uringOpKind uint8

const (
	uringOpWrite uringOpKind = iota
	uringOpFsync
	uringOpNop
)

// uringOp is one ring submission; fields unused by Kind are ignored. Submit
// stamps Sink with the submitting handle's id, and the completion echoes
// Sink<<32 | Tag back in user_data.
type uringOp struct {
	Kind     uringOpKind
	File     *os.File
	Buf      []byte
	Off      int64
	DataSync bool
	Link     bool
	Drain    bool
	Sink     uint32
	Tag      uint32
}
