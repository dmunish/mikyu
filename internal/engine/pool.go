package engine

import "sync"

// maxIndexKeepBytes caps the size of index-write buffers the pool retains.
const maxIndexKeepBytes = 1 << 20

// pooledBuf boxes a reusable byte buffer so sync.Pool hands back the same backing array.
type pooledBuf struct{ b []byte }

var bufPool sync.Pool

// getBuf takes a pooled buffer with room for at least n bytes.
func getBuf(n int) *pooledBuf {
	pb, _ := bufPool.Get().(*pooledBuf)
	if pb == nil {
		pb = &pooledBuf{}
	}
	if cap(pb.b) < n {
		pb.b = make([]byte, n)
	} else {
		pb.b = pb.b[:n]
	}
	return pb
}

// release returns the buffer to the pool unless it grew past maxKeep bytes.
func (pb *pooledBuf) release(maxKeep int) {
	if cap(pb.b) > maxKeep {
		return
	}
	pb.b = pb.b[:0]
	bufPool.Put(pb)
}
