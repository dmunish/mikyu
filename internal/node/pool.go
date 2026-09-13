package node

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dmunish/mikyu/internal/spec"
)

// ErrSessionDefaultsMismatch: same dir opened twice with different session defaults.
var ErrSessionDefaultsMismatch = errors.New("process already opened this data directory with different session defaults")

type nodeEntry struct {
	node *Node
	refs int
}

var (
	poolMu  sync.Mutex
	poolMap = map[string]*nodeEntry{}
)

// Acquire returns a reference-counted Node for dir.
func Acquire(dir string, defaults spec.Spec) (*Node, func() error, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}

	poolMu.Lock()
	if e, ok := poolMap[abs]; ok {
		if !spec.Equal(e.node.defaults, defaults) {
			poolMu.Unlock()
			return nil, nil, fmt.Errorf("%w (dir %s)", ErrSessionDefaultsMismatch, abs)
		}
		e.refs++
		poolMu.Unlock()
		return e.node, releaseFunc(abs), nil
	}
	n, err := openNode(abs, defaults)
	if err != nil {
		poolMu.Unlock()
		return nil, nil, err
	}
	poolMap[abs] = &nodeEntry{node: n, refs: 1}
	poolMu.Unlock()
	return n, releaseFunc(abs), nil
}

func releaseFunc(abs string) func() error {
	var (
		once     sync.Once
		closeErr error
	)
	return func() error {
		once.Do(func() {
			poolMu.Lock()
			e, ok := poolMap[abs]
			if !ok {
				poolMu.Unlock()
				return
			}
			e.refs--
			if e.refs > 0 {
				poolMu.Unlock()
				return
			}
			delete(poolMap, abs)
			poolMu.Unlock()
			closeErr = e.node.closeInternal()
		})
		return closeErr
	}
}
