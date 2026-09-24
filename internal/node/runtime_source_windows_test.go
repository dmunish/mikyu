//go:build windows

package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRuntimeSourcePlatform asserts the Windows readers return live values.
func TestRuntimeSourcePlatform(t *testing.T) {
	require.Positive(t, readProcessRSS())
	require.Positive(t, readProcessVSZ())
	require.Positive(t, readProcessThreads())
	require.Positive(t, readProcessFDs())
	require.GreaterOrEqual(t, readProcessUserSeconds(), 0.0)
	require.GreaterOrEqual(t, readProcessSysSeconds(), 0.0)
}
