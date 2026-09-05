package mikyu_test

import (
	"context"
	"sync"
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedClientConcurrentLogQueue(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateQueue(context.TODO(), "concurrent-q")
	require.NoError(t, err)

	const goroutines = 10
	var wg sync.WaitGroup
	queues := make([]mikyu.LogQueue, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lq, err := client.LogQueue("concurrent-q")
			assert.NoError(t, err)
			queues[idx] = lq
		}(i)
	}
	wg.Wait()

	// All returned handles must point to the exact same LogQueue instance pointer
	first := queues[0]
	for i := 1; i < goroutines; i++ {
		assert.Same(t, first, queues[i], "concurrent LogQueue() returned duplicate instances!")
	}
}
