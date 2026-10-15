package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockObserverUncontended(t *testing.T) {
	l := NewLockObserver("test.mu", nil)
	for range 10_000 {
		l.Lock()
		l.Unlock()
	}
	view := l.Snapshot()
	assert.Equal(t, "test.mu", view.Name)
	assert.Zero(t, view.Waits)
	assert.Zero(t, view.TotalWaitMs)
	assert.Zero(t, view.MaxWaitMs)
}

func TestLockObserverContended(t *testing.T) {
	l := NewLockObserver("test.mu", nil)
	// A held millisecond long guarantees the other contenders block on it.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				l.Lock()
				time.Sleep(time.Millisecond)
				l.Unlock()
			}
		})
	}
	wg.Wait()

	view := l.Snapshot()
	assert.Positive(t, view.Waits)
	assert.Positive(t, view.TotalWaitMs)
	assert.Positive(t, view.P99WaitMs)
	assert.Positive(t, view.MaxWaitMs)
}

// TestLockObserverChargesWait: a wait long enough to measure is charged to the row.
func TestLockObserverChargesWait(t *testing.T) {
	l := NewLockObserver("test.mu", nil)
	held := make(chan struct{})
	go func() {
		l.Lock()
		close(held)
		time.Sleep(20 * time.Millisecond)
		l.Unlock()
	}()
	<-held
	l.Lock()
	l.Unlock()

	view := l.Snapshot()
	assert.EqualValues(t, 1, view.Waits)
	// A 20ms hold must be measured as a real wait, not rounded to sub-resolution zero.
	assert.Greater(t, view.MaxWaitMs, 5.0, "max wait must reflect the 20ms hold")
	assert.Less(t, view.MaxWaitMs, 200.0, "max wait must not absorb unrelated test time")
}

func TestLockObserverConcurrentSnapshot(t *testing.T) {
	l := NewLockObserver("test.mu", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 500 {
			_ = l.Snapshot()
		}
	}()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 500 {
				l.Lock()
				l.Unlock()
			}
		})
	}
	wg.Wait()
	<-done
	// The contract here is only that the race detector stays quiet; whether
	// these short critical sections happened to collide is not asserted.
}

func TestLockObserverMaxMonotonic(t *testing.T) {
	l := NewLockObserver("test.mu", nil)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 2000 {
				l.Lock()
				l.Unlock()
			}
		})
	}
	prev := int64(-1)
	for range 200 {
		got := int64(l.Snapshot().MaxWaitMs * 1e6)
		assert.GreaterOrEqual(t, got, prev)
		prev = got
	}
	wg.Wait()
}

func TestLockObserverNilSafe(t *testing.T) {
	var l *LockObserver
	require.NotPanics(t, func() {
		l.Lock()
		l.Unlock()
		l = NewLockObserver("test.mu", nil)
	})
	assert.Equal(t, LockView{}, (*LockObserver)(nil).Snapshot())
	assert.Nil(t, (*LockObserver)(nil).LockViews())

	var rw *RWLockObserver
	require.NotPanics(t, func() {
		rw.Lock()
		rw.Unlock()
		rw.RLock()
		rw.RUnlock()
	})
	assert.Equal(t, LockView{}, (*RWLockObserver)(nil).WriteView())
	assert.Equal(t, LockView{}, (*RWLockObserver)(nil).ReadView())
	assert.Nil(t, (*RWLockObserver)(nil).LockViews())
}

func TestRWLockObserver(t *testing.T) {
	l := NewRWLockObserver("test.rw", nil)

	// A reader arriving at a held write lock is charged to the read row.
	l.Lock()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		l.RLock()
		l.RUnlock()
	}()
	time.Sleep(50 * time.Millisecond)
	l.Unlock()
	<-readerDone

	// A writer arriving at a held read lock is charged to the write row.
	l.RLock()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		l.Lock()
		l.Unlock()
	}()
	time.Sleep(50 * time.Millisecond)
	l.RUnlock()
	<-writerDone

	views := l.LockViews()
	require.Len(t, views, 2)
	assert.Equal(t, "test.rw", views[0].Name)
	assert.Equal(t, "test.rw.read", views[1].Name)
	assert.Positive(t, views[0].Waits)
	assert.Positive(t, views[1].Waits)
}

func TestSnapshotLockObserversSortedByName(t *testing.T) {
	RegisterLockObserver(NewLockObserver("zzz.mu", nil))
	RegisterLockObserver(NewRWLockObserver("aaa.rw", nil))
	t.Cleanup(func() {
		observedMu.Lock()
		defer observedMu.Unlock()
		delete(observed, "zzz.mu")
		delete(observed, "aaa.rw")
	})

	views := SnapshotLockObservers()
	names := make([]string, 0, len(views))
	for _, v := range views {
		names = append(names, v.Name)
	}
	assert.IsIncreasing(t, names)
	assert.Contains(t, names, "zzz.mu")
	assert.Contains(t, names, "aaa.rw")
	assert.Contains(t, names, "aaa.rw.read")
}

func TestRegisterLockObserverReplacesSameName(t *testing.T) {
	first := NewLockObserver("dup.mu", nil)
	second := NewLockObserver("dup.mu", nil)
	RegisterLockObserver(first)
	t.Cleanup(func() {
		observedMu.Lock()
		defer observedMu.Unlock()
		delete(observed, "dup.mu")
	})
	RegisterLockObserver(second)

	count := 0
	for _, v := range SnapshotLockObservers() {
		if v.Name == "dup.mu" {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

func TestRegisterLockObserverNilIgnored(t *testing.T) {
	before := len(SnapshotLockObservers())
	RegisterLockObserver(nil)
	RegisterLockObserver((*LockObserver)(nil))
	assert.Len(t, SnapshotLockObservers(), before)
}

func TestCollectorIncludesLockObservers(t *testing.T) {
	RegisterLockObserver(NewLockObserver("snapshot.mu", nil))
	t.Cleanup(func() {
		observedMu.Lock()
		defer observedMu.Unlock()
		delete(observed, "snapshot.mu")
	})

	c := New(NewRegistry(Config{}), Config{Interval: time.Hour})
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	var found bool
	for _, v := range snap.Locks.Observers {
		if v.Name == "snapshot.mu" {
			found = true
		}
	}
	assert.True(t, found, "collector must publish registered lock observers")
}
