package mikyu

import (
	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

type ClientOption = client.Option

type QueueOption = client.QueueOption

type AppendOption = logqueue.AppendOption

type EnqueueOption = taskqueue.EnqueueOption

type NackOption = taskqueue.NackOption
