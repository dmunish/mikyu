package metrics

import (
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// lockWaitBounds are the lock-wait latency buckets, in seconds.
var lockWaitBounds = []float64{
	1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 2e-3, 5e-3, 1e-2, 5e-2, 1e-1,
}

// LockViewSource is an observed lock that reports contention rows to the snapshot.
type LockViewSource interface {
	// Name is the observer's base name; it keys the process-wide set.
	Name() string
	// LockViews returns one row per observed mode, ready for display.
	LockViews() []LockView
}

// LockObserver times contention on a mutex, charging only the contended path.
type LockObserver struct {
	name        string
	mu          sync.Mutex
	waits       atomic.Int64
	maxBits     atomic.Uint64
	hist        *Histogram
}

// NewLockObserver returns an observer named name; empty bounds select the defaults.
func NewLockObserver(name string, bounds []float64) *LockObserver {
	if len(bounds) == 0 {
		bounds = lockWaitBounds
	}
	return &LockObserver{name: name, hist: NewHistogram(bounds)}
}

// Name returns the observer's base name.
func (l *LockObserver) Name() string {
	if l == nil {
		return ""
	}
	return l.name
}

// Lock acquires the mutex, timing the call only when it contends.
func (l *LockObserver) Lock() {
	if l == nil {
		return
	}
	if l.mu.TryLock() {
		return
	}
	start := time.Now()
	l.mu.Lock()
	chargeWait(&l.waits, &l.maxBits, l.hist, time.Since(start))
}

// Unlock releases the mutex.
func (l *LockObserver) Unlock() {
	if l == nil {
		return
	}
	l.mu.Unlock()
}

// Snapshot returns the lock's contention view.
func (l *LockObserver) Snapshot() LockView {
	if l == nil {
		return LockView{}
	}
	return lockView(l.name, &l.waits, &l.maxBits, l.hist)
}

// LockViews returns the lock's single contention row.
func (l *LockObserver) LockViews() []LockView {
	if l == nil {
		return nil
	}
	return []LockView{l.Snapshot()}
}

// RWLockObserver times write and read contention on an RWMutex separately.
type RWLockObserver struct {
	name         string
	mu           sync.RWMutex
	wwaits       atomic.Int64
	wmax         atomic.Uint64
	whist        *Histogram
	rwaits       atomic.Int64
	rmax         atomic.Uint64
	rhist        *Histogram
}

// NewRWLockObserver returns an RW observer named name; empty bounds select the defaults.
func NewRWLockObserver(name string, bounds []float64) *RWLockObserver {
	if len(bounds) == 0 {
		bounds = lockWaitBounds
	}
	return &RWLockObserver{name: name, whist: NewHistogram(bounds), rhist: NewHistogram(bounds)}
}

// Name returns the observer's base name.
func (l *RWLockObserver) Name() string {
	if l == nil {
		return ""
	}
	return l.name
}

// Lock acquires the write lock, timing the call only when it contends.
func (l *RWLockObserver) Lock() {
	if l == nil {
		return
	}
	if l.mu.TryLock() {
		return
	}
	start := time.Now()
	l.mu.Lock()
	chargeWait(&l.wwaits, &l.wmax, l.whist, time.Since(start))
}

// Unlock releases the write lock.
func (l *RWLockObserver) Unlock() {
	if l == nil {
		return
	}
	l.mu.Unlock()
}

// RLock acquires the read lock, timing the call only when a writer blocks it.
func (l *RWLockObserver) RLock() {
	if l == nil {
		return
	}
	if l.mu.TryRLock() {
		return
	}
	start := time.Now()
	l.mu.RLock()
	chargeWait(&l.rwaits, &l.rmax, l.rhist, time.Since(start))
}

// RUnlock releases the read lock.
func (l *RWLockObserver) RUnlock() {
	if l == nil {
		return
	}
	l.mu.RUnlock()
}

// WriteView returns the write-contention row, named for the lock itself.
func (l *RWLockObserver) WriteView() LockView {
	if l == nil {
		return LockView{}
	}
	return lockView(l.name, &l.wwaits, &l.wmax, l.whist)
}

// ReadView returns the read-contention row, named for the lock's read mode.
func (l *RWLockObserver) ReadView() LockView {
	if l == nil {
		return LockView{}
	}
	return lockView(l.name+".read", &l.rwaits, &l.rmax, l.rhist)
}

// LockViews returns the write row followed by the read row.
func (l *RWLockObserver) LockViews() []LockView {
	if l == nil {
		return nil
	}
	return []LockView{l.WriteView(), l.ReadView()}
}

var (
	observedMu sync.Mutex
	observed   = map[string]LockViewSource{}
)

// RegisterLockObserver adds src to the process-wide set, replacing the same name.
func RegisterLockObserver(src LockViewSource) {
	if src == nil {
		return
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	observed[src.Name()] = src
}

// SnapshotLockObservers returns every registered lock's rows, sorted by name.
func SnapshotLockObservers() []LockView {
	observedMu.Lock()
	sources := make([]LockViewSource, 0, len(observed))
	for _, src := range observed {
		sources = append(sources, src)
	}
	observedMu.Unlock()

	if len(sources) == 0 {
		return nil
	}
	views := make([]LockView, 0, len(sources))
	for _, src := range sources {
		views = append(views, src.LockViews()...)
	}
	slices.SortFunc(views, func(a, b LockView) int { return strings.Compare(a.Name, b.Name) })
	return views
}

// chargeWait records one contended acquisition; waits are timed in float seconds
// because a sub-resolution wait truncates to zero nanoseconds on some platforms.
func chargeWait(waits *atomic.Int64, maxBits *atomic.Uint64, hist *Histogram, d time.Duration) {
	waits.Add(1)
	hist.Observe(d.Seconds())
	raiseMax(maxBits, d.Seconds())
}

// raiseMax stores seconds as the longest wait when it exceeds the stored maximum.
func raiseMax(maxBits *atomic.Uint64, seconds float64) {
	for {
		cur := maxBits.Load()
		if seconds <= math.Float64frombits(cur) || maxBits.CompareAndSwap(cur, math.Float64bits(seconds)) {
			return
		}
	}
}

// lockView assembles one contention row from its instruments.
func lockView(name string, waits *atomic.Int64, maxBits *atomic.Uint64, hist *Histogram) LockView {
	snap := hist.Snapshot()
	return LockView{
		Name:        name,
		Waits:       waits.Load(),
		TotalWaitMs: snap.Sum * 1e3,
		MaxWaitMs:   math.Float64frombits(maxBits.Load()) * 1e3,
		P50WaitMs:   snap.Percentile(0.50) * 1e3,
		P99WaitMs:   snap.Percentile(0.99) * 1e3,
	}
}
