//go:build unix

package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRuntimeSourcePlatform asserts the POSIX readers report non-negative values.
func TestRuntimeSourcePlatform(t *testing.T) {
	require.GreaterOrEqual(t, readProcessRSS(), int64(0))
	require.GreaterOrEqual(t, readProcessVSZ(), int64(0))
	require.GreaterOrEqual(t, readProcessThreads(), int64(0))
	require.GreaterOrEqual(t, readProcessFDs(), int64(0))
	require.GreaterOrEqual(t, readProcessUserSeconds(), 0.0)
	require.GreaterOrEqual(t, readProcessSysSeconds(), 0.0)
}
