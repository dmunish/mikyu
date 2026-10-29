//go:build linux

package iow

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A failed enter must retire the ring before any further publication, so the
// SQEs it left behind can never be consumed by a later submitter.
func TestSubmitRetiresTheRingAfterEnterFailure(t *testing.T) {
	r := newRing()
	if r == nil {
		t.Skip("io_uring is unavailable")
	}
	// Closing the ring fd makes every enter fail without touching the data files
	// a writer submits to. The reaper may observe the failure first; both paths
	// retire the same gate, and neither may publish again.
	require.NoError(t, unix.Close(r.fd))

	op := []op{{Kind: opNop, Tag: 1, Sink: uringSinkIDNone}}
	require.ErrorIs(t, r.submit(op), ErrRingBroken)
	require.ErrorIs(t, r.submit(op), ErrRingBroken)
	require.True(t, r.gate.isBroken())
}

// A ring that can no longer be entered must still deliver the completions it
// already owns; drain and dispatch do that without any syscall.
func TestBrokenRingStillDeliversCompletions(t *testing.T) {
	cqes := make([]rawCQE, 4)
	head, tail := new(uint32), new(uint32)
	r := &ring{
		entries: 4,
		cqHead:  head,
		cqTail:  tail,
		cqMask:  3,
		cqBase:  unsafe.Pointer(&cqes[0]),
	}
	h := r.register()
	done := make(chan error, 1)
	require.NoError(t, h.tags.register(1, 1, func(err error) { done <- err }))

	cqes[0] = rawCQE{userData: uint64(h.id)<<32 | 1}
	atomic.StoreUint32(tail, 1)

	out := make([]rawCQE, 4)
	n := r.drain(out)
	require.Equal(t, 1, n)
	for i := range n {
		r.dispatch(out[i])
	}
	require.NoError(t, <-done)
}

func TestDrainCopiesReadyCompletions(t *testing.T) {
	cqes := make([]rawCQE, 4)
	head, tail := new(uint32), new(uint32)
	r := &ring{
		entries: 4,
		cqHead:  head,
		cqTail:  tail,
		cqMask:  3,
		cqBase:  unsafe.Pointer(&cqes[0]),
	}

	cqes[0] = rawCQE{userData: 7, res: 12}
	cqes[1] = rawCQE{userData: 8, res: -5}
	atomic.StoreUint32(tail, 2)

	out := make([]rawCQE, 4)
	require.Equal(t, 2, r.drain(out))
	require.Equal(t, uint64(7), out[0].userData)
	require.Equal(t, int64(12), int64(out[0].res))
	require.Equal(t, uint64(8), out[1].userData)
	require.Equal(t, int32(-5), out[1].res)
	require.Equal(t, uint32(2), atomic.LoadUint32(head))
	require.Zero(t, r.drain(out))
}