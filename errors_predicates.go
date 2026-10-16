package mikyu

import "errors"

func IsNotFound(err error) bool {
	return errors.Is(err, ErrQueueNotFound) ||
		errors.Is(err, ErrTaskNotFound) ||
		errors.Is(err, ErrDeadLetterNotFound)
}

func IsStale(err error) bool {
	return errors.Is(err, ErrTaskStale) || errors.Is(err, ErrDeadLetterStale)
}

func IsFull(err error) bool { return errors.Is(err, ErrQueueFull) }

func IsClosed(err error) bool {
	return errors.Is(err, ErrQueueClosed) || errors.Is(err, ErrTaskClosed)
}

func IsTooLarge(err error) bool {
	return errors.Is(err, ErrMessageTooLarge) || errors.Is(err, ErrRecordTooLarge)
}

func IsCorrupt(err error) bool {
	return errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrCheckpointCorrupt)
}
