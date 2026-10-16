package mikyu

import "errors"

// The Is helpers group sentinels that callers usually treat the same
// way. Each one documents exactly which errors it matches, and all of
// them use errors.Is, so wrapping anywhere in the chain is respected.
// A nil error matches nothing.

// IsNotFound reports whether err means "this thing does not exist".
// It matches ErrQueueNotFound, ErrTaskNotFound, and
// ErrDeadLetterNotFound. It deliberately does not match
// ErrOffsetNotFound: a missing queue is a naming problem, while a
// missing offset is usually a reading-order problem.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrQueueNotFound) ||
		errors.Is(err, ErrTaskNotFound) ||
		errors.Is(err, ErrDeadLetterNotFound)
}

// IsStale reports whether err means "someone else owns this now". It
// matches ErrTaskStale and ErrDeadLetterStale, the two errors that
// come from losing a race for a task.
func IsStale(err error) bool {
	return errors.Is(err, ErrTaskStale) || errors.Is(err, ErrDeadLetterStale)
}

// IsFull reports whether err means "the queue is at a bound". It
// matches ErrQueueFull.
func IsFull(err error) bool { return errors.Is(err, ErrQueueFull) }

// IsClosed reports whether err means "this queue is shut down". It
// matches ErrQueueClosed and ErrTaskClosed.
func IsClosed(err error) bool {
	return errors.Is(err, ErrQueueClosed) || errors.Is(err, ErrTaskClosed)
}

// IsTooLarge reports whether err means "this payload cannot fit". It
// matches ErrMessageTooLarge and ErrRecordTooLarge.
func IsTooLarge(err error) bool {
	return errors.Is(err, ErrMessageTooLarge) || errors.Is(err, ErrRecordTooLarge)
}

// IsCorrupt reports whether err means "stored data failed
// validation". It matches ErrCorruptRecord and ErrCheckpointCorrupt.
func IsCorrupt(err error) bool {
	return errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrCheckpointCorrupt)
}
