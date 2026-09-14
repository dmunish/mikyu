package client

import (
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
)

// QueueDescription is the full truth about one queue: its stored spec, its effective spec (stored over defaults over built-in), and the session defaults it inherits from.
type QueueDescription struct {
	Spec      spec.Spec `json:"spec"`
	Effective spec.Spec `json:"effective"`
	Defaults  spec.Spec `json:"defaults"`
}

func toClientDescription(d node.QueueDescription) QueueDescription {
	return QueueDescription{
		Spec:      d.Spec,
		Effective: d.Effective,
		Defaults:  d.Defaults,
	}
}
