package mikyu

import (
	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

// ClientOption configures Open or Dial. Options are plain values, so a
// caller can build a slice of them once and reuse it.
type ClientOption = client.Option

// QueueOption sets one field of a queue's spec. Each option names its
// field, so a patch is sparse by construction: only what you set is
// sent. Pass them to OpenLog, OpenTask, CreateLog, CreateTask, or
// UpdateQueue.
type QueueOption = client.QueueOption

// AppendOption configures one record being appended to a log queue.
type AppendOption = logqueue.AppendOption

// EnqueueOption configures one task being enqueued.
type EnqueueOption = taskqueue.EnqueueOption

// NackOption configures one nack's retry delay.
type NackOption = taskqueue.NackOption
