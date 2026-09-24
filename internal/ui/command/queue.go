package command

import (
	"context"
	"fmt"

	"github.com/dmunish/mikyu"
)

// List returns every queue's name and kind.
func List(ctx context.Context, client mikyu.Client) ([]mikyu.QueueInfo, error) {
	return client.ListQueues(ctx)
}

// KindFromName parses a queue kind from "log" or "task".
func KindFromName(name string) (mikyu.QueueKind, error) {
	switch name {
	case "log":
		return mikyu.QueueKindLog, nil
	case "task":
		return mikyu.QueueKindTask, nil
	default:
		return 0, fmt.Errorf("unknown queue kind %q", name)
	}
}

// Create declares a queue of the given kind from a sparse set of options.
func Create(ctx context.Context, client mikyu.Client, kind mikyu.QueueKind, name string, opts ...mikyu.QueueOption) error {
	switch kind {
	case mikyu.QueueKindTask:
		_, err := client.CreateTaskQueue(ctx, name, opts...)
		return err
	default:
		_, err := client.CreateLogQueue(ctx, name, opts...)
		return err
	}
}

// Update applies a sparse patch of mutable fields to a queue.
func Update(ctx context.Context, client mikyu.Client, name string, opts ...mikyu.QueueOption) error {
	return client.UpdateQueue(ctx, name, opts...)
}

// Describe returns a queue's stored spec, effective spec, and defaults.
func Describe(ctx context.Context, client mikyu.Client, name string) (mikyu.QueueDescription, error) {
	return client.DescribeQueue(ctx, name)
}

// TightensRetention reports whether patch narrows a retention bound below what current already allows.
func TightensRetention(current, patch mikyu.Spec) bool {
	if next := patch.Retention.MaxAge; next != nil {
		if cur := current.Retention.MaxAge; cur == nil || next.Duration() < cur.Duration() {
			return true
		}
	}
	if next := patch.Retention.MaxBytes; next != nil {
		if cur := current.Retention.MaxBytes; cur == nil || *next < *cur {
			return true
		}
	}
	return false
}

// Delete removes a queue and its stored data, gated by confirmFn when provided.
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
