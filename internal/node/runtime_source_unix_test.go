//go:build !windows

package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRuntimeSourcePlatform asserts the OS readers report non-negative values.
func TestRuntimeSourcePlatform(t *testing.T) {
	require.GreaterOrEqual(t, readProcessRSS(), int64(0))
	require.GreaterOrEqual(t, readProcessCPU(), 0.0)
}
