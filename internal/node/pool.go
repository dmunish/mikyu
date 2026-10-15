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

// acquireOptions carries the process-level settings a freshly opened node builds its services from.
type acquireOptions struct {
	metrics MetricsConfig
	keyFile string
}

// AcquireOption configures how a freshly opened node builds its runtime services.
type AcquireOption func(*acquireOptions)

// WithMetrics overrides the collector settings for a newly opened node.
func WithMetrics(cfg MetricsConfig) AcquireOption {
	return func(dst *acquireOptions) { dst.metrics = cfg }
}

// WithEncryptionKeyFile names the master-key file that unseals encryption: aes queues.
func WithEncryptionKeyFile(path string) AcquireOption {
	return func(dst *acquireOptions) { dst.keyFile = path }
}

// MetricsFromSession extracts the collector settings from a resolved session.
func MetricsFromSession(s spec.Session) MetricsConfig {
	return MetricsConfig{
		Enabled:    s.Metrics.Enabled,
		Interval:   s.Metrics.Interval.Duration(),
		History:    s.Metrics.History,
		Histograms: s.Metrics.Histograms,
	}
}

// EncryptionKeyFileFromSession extracts the at-rest master-key source from a resolved session.
func EncryptionKeyFileFromSession(s spec.Session) string {
	return s.Encryption.KeyFile
}

// Acquire returns a reference-counted Node for dir.
func Acquire(dir string, defaults spec.Spec, opts ...AcquireOption) (*Node, func() error, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	if err := spec.ValidateCreate(defaults); err != nil {
		return nil, nil, err
	}

	ao := acquireOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&ao)
		}
	}

	poolMu.Lock()
	if e, ok := poolMap[abs]; ok {
		if !spec.Equal(e.node.defaults, defaults) || e.node.keyFile != ao.keyFile {
			poolMu.Unlock()
			return nil, nil, fmt.Errorf("%w (dir %s)", ErrSessionDefaultsMismatch, abs)
		}
		e.refs++
		poolMu.Unlock()
		return e.node, releaseFunc(abs), nil
	}
	n, err := openNode(abs, defaults, ao)
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
