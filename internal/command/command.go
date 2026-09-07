package command

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
)

// ResolvePayload converts string input, @file paths, or stdin (-) into payload bytes.
func ResolvePayload(input string) ([]byte, error) {
	if input == "-" {
		return io.ReadAll(os.Stdin)
	}
	if strings.HasPrefix(input, "@") {
		path := strings.TrimPrefix(input, "@")
		return os.ReadFile(path)
	}
	return []byte(input), nil
}

// List returns the names of all active queues.
func List(ctx context.Context, client mikyu.Client) ([]string, error) {
	return client.ListQueues(ctx)
}

// Create initializes a new log queue with the specified name.
func Create(ctx context.Context, client mikyu.Client, name string) error {
	return client.CreateQueue(ctx, name)
}

// Delete removes a queue and its stored data. If confirmFn is provided, deletion occurs only if confirmFn returns true.
func Delete(ctx context.Context, client mikyu.Client, name string, confirmFn func() (bool, error)) (executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return false, err
		}
	}
	if err := client.DeleteQueue(ctx, name); err != nil {
		return true, err
	}
	return true, nil
}

// Produce sends payload input to the queue and returns the produced message offset and payload size in bytes.
func Produce(ctx context.Context, client mikyu.Client, name string, input string) (offset uint64, payloadLen int, err error) {
	payload, err := ResolvePayload(input)
	if err != nil {
		return 0, 0, err
	}
	q, err := client.LogQueue(name)
	if err != nil {
		return 0, 0, err
	}
	off, err := q.Produce(ctx, payload)
	if err != nil {
		return 0, 0, err
	}
	return off, len(payload), nil
}

// Read retrieves the message at a specific logical offset.
func Read(ctx context.Context, client mikyu.Client, name string, offset uint64) (mikyu.Message, error) {
	q, err := client.LogQueue(name)
	if err != nil {
		return mikyu.Message{}, err
	}
	return q.Read(ctx, offset)
}

// Tail streams records continuously starting from startOffset and calls onMessage for each message.
func Tail(ctx context.Context, client mikyu.Client, name string, startOffset uint64, onMessage func(msg mikyu.Message) error) error {
	q, err := client.LogQueue(name)
	if err != nil {
		return err
	}
	for msg, err := range q.Tail(ctx, startOffset) {
		if err != nil {
			return err
		}
		if onMessage != nil {
			if err := onMessage(msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// Stats retrieves engine storage metrics.
func Stats(ctx context.Context, client mikyu.Client) (mikyu.SystemStats, error) {
	return client.Stats(ctx)
}

// Compact truncates physical log segments for a queue before minOffset. If confirmFn is provided, truncation occurs only if confirmFn returns true.
func Compact(ctx context.Context, client mikyu.Client, name string, minOffset uint64, confirmFn func() (bool, error)) (reclaimed int64, executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return 0, false, err
		}
	}
	reclaimed, err = client.TruncateQueue(ctx, name, minOffset)
	if err != nil {
		return 0, true, err
	}
	return reclaimed, true, nil
}

// Shutdown sends an OpShutdown frame to the daemon process. If confirmFn is provided, shutdown occurs only if confirmFn returns true.
func Shutdown(target string, confirmFn func() (bool, error)) (executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return false, err
		}
	}
	if err := daemon.SendShutdownSignal(target); err != nil {
		return true, err
	}
	return true, nil
}
