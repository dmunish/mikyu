//go:build darwin

package node

import (
	"os"
	"sync"
	"time"
)

// rssUnitBytes converts getrusage's Maxrss unit (bytes on Darwin) to bytes.
const rssUnitBytes int64 = 1

// readProcessVSZ is unavailable without cgo on Darwin.
func readProcessVSZ() int64 { return 0 }

// readProcessThreads is unavailable without cgo on Darwin.
func readProcessThreads() int64 { return 0 }

// fdCache throttles /dev/fd reads to one scan per ttl.
var fdCache struct {
	mu sync.Mutex
	at time.Time
	n  int64
}

// readProcessFDs counts open descriptors through /dev/fd, cached for five seconds.
func readProcessFDs() int64 {
	fdCache.mu.Lock()
	if time.Since(fdCache.at) < 5*time.Second {
		n := fdCache.n
		fdCache.mu.Unlock()
		return n
	}
	fdCache.mu.Unlock()

	entries, err := os.ReadDir("/dev/fd")
	n := int64(0)
	if err == nil {
		n = int64(len(entries))
	}
	fdCache.mu.Lock()
	fdCache.at = time.Now()
	fdCache.n = n
	fdCache.mu.Unlock()
	return n
}
