package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mustCreateLog(t *testing.T, c Client, name string, opts ...QueueOption) {
	t.Helper()
	_, err := c.CreateLogQueue(t.Context(), name, opts...)
	require.NoError(t, err)
}

func mustCreateTask(t *testing.T, c Client, name string, opts ...QueueOption) {
	t.Helper()
	_, err := c.CreateTaskQueue(t.Context(), name, opts...)
	require.NoError(t, err)
}