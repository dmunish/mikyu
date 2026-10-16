package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/node"
)

type QueueKind = node.Kind

const (
	QueueKindLog  = node.KindLog
	QueueKindTask = node.KindTask
)

type Record struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Group     []byte
	Payload   []byte
}

type Ref interface{ taskRef() }

type TaskRef = Ref

type Task struct {
	ID          string
	Payload     []byte
	Attempts    uint32
	MaxAttempts uint32
	Lease       time.Time
	ScheduledAt time.Time
	DeliveryID  string
	Offset      uint64
	Key         []byte
	Stalled     uint32
}

func (Task) taskRef() {}

type TaskID string

func (TaskID) taskRef() {}

type QueueInfo struct {
	Name string
	Kind QueueKind
}

type QueueDescription struct {
	Name      string
	Kind      QueueKind
	Spec      Spec
	Derived   Spec
	Effective Spec
	Defaults  Spec
}
