package node

import (
	"testing"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessPoolSharesNode(t *testing.T) {
	dir := t.TempDir()

	n1, r1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	n2, r2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	assert.Same(t, n1, n2)

	// Release one ref; node stays alive and usable.
	require.NoError(t, r1())
	require.NoError(t, n2.CreateQueue(t.Context(), "q", spec.Spec{}))

	// Release the last ref; node closes.
	require.NoError(t, r2())

	// Reacquire: fresh node.
	n3, r3, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = r3() }()
	assert.NotSame(t, n1, n3)
}

func TestReleaseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	_, r, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	require.NoError(t, r())
	require.NoError(t, r())
}

func TestAcquireDefaultsMismatch(t *testing.T) {
	dir := t.TempDir()
	n1, r1, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = r1() }()

	seven := 7
	_, _, err = Acquire(dir, spec.Spec{Engine: spec.EngineSpec{BatchSize: &seven}})
	require.ErrorIs(t, err, ErrSessionDefaultsMismatch)
	require.Contains(t, err.Error(), dir)

	// Identical defaults share the node.
	n2, r2, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	assert.Same(t, n1, n2)
	require.NoError(t, r2())
}
