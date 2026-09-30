package bench

import (
	"errors"
	"fmt"

	"github.com/dmunish/mikyu/internal/bench/result"
)

var (
	// ErrAborted reports a run cut short by cancellation or a failed check.
	ErrAborted = errors.New("bench run aborted")
	// ErrVerify reports a payload that did not match the one the seed promised.
	ErrVerify = errors.New("payload verification failed")
	// ErrCorruptResult reports an envelope this build cannot read.
	ErrCorruptResult = errors.New("bench result is unreadable")
	// ErrCompareMismatch reports runs that do not share a mode.
	ErrCompareMismatch = errors.New("bench runs cannot be compared")
	// ErrUsage reports a bad flag, mode, or workload combination.
	ErrUsage = errors.New("bench usage error")
	// ErrNotARun reports a directory without an envelope in it.
	ErrNotARun = result.ErrNotARun
	// ErrEnvelopeVersion reports an envelope whose schema this build does not know.
	ErrEnvelopeVersion = result.ErrEnvelopeVersion
	// errDaemonStart reports a spawned daemon that never accepted connections.
	errDaemonStart = errors.New("daemon did not accept connections")
)

// VerifyError carries the offending offset and why the payload did not match.
type VerifyError struct {
	Offset uint64
	Reason string
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("verify at offset %d: %s", e.Offset, e.Reason)
}

// Unwrap ties every VerifyError to ErrVerify so errors.Is matches.
func (e *VerifyError) Unwrap() error { return ErrVerify }

// errRule reports one violated workload rule against the offending value.
func errRule(key, rule string, got any) error {
	return fmt.Errorf("%w: %s must be %s, got %v", ErrUsage, key, rule, got)
}
