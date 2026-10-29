package iow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRingGateRetiresOnFirstFailure(t *testing.T) {
	var g ringGate
	cause := errors.New("ebadf")
	runs := 0

	err := g.run(func() error { runs++; return cause })
	require.ErrorIs(t, err, ErrRingBroken)
	require.ErrorIs(t, err, cause)
	require.True(t, g.isBroken())

	err = g.run(func() error { runs++; return nil })
	require.ErrorIs(t, err, ErrRingBroken)
	require.ErrorIs(t, err, cause)
	require.Equal(t, 1, runs, "a retired gate must not run another publication")

	require.ErrorIs(t, g.fail(errors.New("later")), ErrRingBroken)
	require.Equal(t, 1, runs)
}

func TestRingGateStaysOpenWhilePublicationsSucceed(t *testing.T) {
	var g ringGate
	require.NoError(t, g.run(func() error { return nil }))
	require.NoError(t, g.run(func() error { return nil }))
	require.False(t, g.isBroken())
}