//go:build linux

package iow

import (
	"testing"

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