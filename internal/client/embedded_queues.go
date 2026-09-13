package client

import (
	"context"

	"github.com/dmunish/mikyu/internal/queue/logqueue"
)

// LogQueue returns a cached handle to an existing log queue.
func (c *embeddedClient) LogQueue(name string) (logqueue.ClientLogQueue, error) {
	lq, err := c.node.LogQueue(name)
	if err != nil {
		return nil, err
	}
	return lq, nil
}

// TaskQueue returns a cached handle to an existing task queue.
func (c *embeddedClient) TaskQueue(name string) (TaskQueue, error) {
	tq, err := c.node.TaskQueue(name)
	if err != nil {
		return nil, err
	}
	return tq, nil
}

// Truncate manually truncates log segments before minOffset.
func (c *embeddedClient) Truncate(ctx context.Context, name string, minOffset uint64) (int64, error) {
	return c.node.Truncate(ctx, name, minOffset)
}
