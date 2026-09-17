package node

import "github.com/dmunish/mikyu/internal/manager"

// Kind identifies a queue's structural type; a queue's parent directory is its kind.
type Kind = manager.Kind

const (
	// KindLog marks queues under log/.
	KindLog = manager.KindLog
	// KindTask marks queues under task/.
	KindTask = manager.KindTask
)

// QueueInfo is one queue's name and its kind.
type QueueInfo = manager.QueueInfo

// ErrWrongKind reports a queue addressed through the wrong driver.
var ErrWrongKind = manager.ErrWrongKind