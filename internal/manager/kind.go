// Package manager owns a Mikyu data directory and its kind-partitioned queue roots.
package manager

import "errors"

// ErrWrongKind reports a queue addressed through the wrong driver.
var ErrWrongKind = errors.New("queue kind mismatch")

// Kind identifies a queue's structural type: a queue's parent directory is its kind.
type Kind uint8

const (
	// KindLog marks queues under log/, read through the log driver.
	KindLog Kind = 1
	// KindTask marks queues under task/, read through the task driver.
	KindTask Kind = 2
)

// String returns the directory name of the kind.
func (k Kind) String() string {
	switch k {
	case KindLog:
		return "log"
	case KindTask:
		return "task"
	default:
		return "unknown"
	}
}

// Valid reports whether k names a real queue kind.
func (k Kind) Valid() bool { return k == KindLog || k == KindTask }

// MarshalText renders a kind as its directory name.
func (k Kind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

// UnmarshalText parses a kind from its directory name.
func (k *Kind) UnmarshalText(text []byte) error {
	switch string(text) {
	case "log":
		*k = KindLog
	case "task":
		*k = KindTask
	default:
		return ErrWrongKind
	}
	return nil
}

// Key is the identity of one queue: a bare name under one kind.
type Key struct {
	Name string
	Kind Kind
}

// QueueInfo is one queue as the filesystem reports it.
type QueueInfo struct {
	Name string `json:"name"`
	Kind Kind   `json:"kind"`
}

// Label renders a key as its kind-qualified path, e.g. "log/events".
func (k Key) Label() string { return k.Kind.String() + "/" + k.Name }
