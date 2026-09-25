package metrics

import (
	"sync"
	"testing"
	"time"
)

func BenchmarkCounterInc(b *testing.B) {
	var c Counter
	for b.Loop() {
		c.Inc()
	}
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := NewHistogram([]float64{1e-6, 1e-5, 1e-4, 1e-3, 1e-2})
	for b.Loop() {
		h.Observe(5e-5)
	}
}

func BenchmarkCollectorSnapshot(b *testing.B) {
	reg := NewRegistry(Config{})
	scope := reg.Scope("q")
	scope.Counter("appends").Add(1)
	scope.Gauge("messages").Set(1)
	c := New(reg, Config{Interval: time.Hour, History: 8, Histograms: true})
	ctx := b.Context()
	for b.Loop() {
		_, _ = c.Snapshot(ctx)
	}
}

// BenchmarkMutexUncontended is the baseline the observers are measured against.
func BenchmarkMutexUncontended(b *testing.B) {
	var mu sync.Mutex
	for b.Loop() {
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkLockObserverUncontended(b *testing.B) {
	l := NewLockObserver("bench.mu", nil)
	for b.Loop() {
		l.Lock()
		l.Unlock()
	}
}

func BenchmarkLockObserverContended(b *testing.B) {
	l := NewLockObserver("bench.mu", nil)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			l.Lock()
			l.Unlock()
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	b.Cleanup(func() {
		close(stop)
		<-done
	})
	for b.Loop() {
		l.Lock()
		l.Unlock()
	}
}