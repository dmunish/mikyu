package metrics

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectorTicksAtInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := NewRegistry(Config{})
		c := New(reg, Config{Interval: 100 * time.Millisecond, History: 10, Histograms: true})
		c.Start(t.Context())
		synctest.Sleep(350 * time.Millisecond)
		synctest.Wait()
		got := len(c.History())
		require.GreaterOrEqual(t, got, 3)
		require.LessOrEqual(t, got, 4)
		c.Stop()
	})
}

func TestCollectorSubscribeDropOnSlow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := NewRegistry(Config{})
		c := New(reg, Config{Interval: 10 * time.Millisecond, History: 100})
		ch, cancel := c.Subscribe()
		defer cancel()
		c.Start(t.Context())
		synctest.Sleep(100 * time.Millisecond)
		synctest.Wait()
		assert.LessOrEqual(t, len(ch), 1, "slow subscriber buffers at most one snapshot")
		c.Stop()
	})
}

func TestCollectorStopIdempotent(t *testing.T) {
	reg := NewRegistry(Config{})
	c := New(reg, Config{Interval: time.Hour})
	c.Stop()
	c.Start(t.Context())
	c.Stop()
	c.Stop()
}

func TestCollectorDisabled(t *testing.T) {
	reg := NewRegistry(Config{})
	c := New(reg, Config{Disabled: true})
	c.Start(t.Context())
	ch, cancel := c.Subscribe()
	defer cancel()
	_, ok := <-ch
	assert.False(t, ok, "disabled collector returns a closed subscription")
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	assert.True(t, snap.Disabled)
}

func TestCollectorMergesScopes(t *testing.T) {
	reg := NewRegistry(Config{})
	reg.Scope("b").Counter("appends").Add(2)
	reg.Scope("a").Gauge("messages").Set(5)
	c := New(reg, Config{Interval: time.Hour, Histograms: true})
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.Queues, 2)
	assert.Equal(t, "a", snap.Queues[0].Name)
	assert.EqualValues(t, 5, snap.Queues[0].Gauges["messages"])
	assert.Equal(t, "b", snap.Queues[1].Name)
	assert.EqualValues(t, 2, snap.Queues[1].Counters["appends"])
	assert.Equal(t, 2, snap.Totals.Queues)
	assert.EqualValues(t, 5, snap.Totals.Messages)
}

func TestCollectorRunsSources(t *testing.T) {
	reg := NewRegistry(Config{})
	src := SourceFunc(func(_ context.Context, dst *Snapshot) {
		dst.Runtime.Goroutines = 42
		dst.Queues = append(dst.Queues, QueueView{Name: "from-source", Kind: "log"})
	})
	c := New(reg, Config{Interval: time.Hour}, src)
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, snap.Runtime.Goroutines)
	q, ok := snap.Queue("from-source")
	require.True(t, ok)
	assert.Equal(t, "log", q.Kind)
}

func TestCollectorRates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := NewRegistry(Config{})
		scope := reg.Scope("q")
		c := New(reg, Config{Interval: time.Hour})

		scope.Counter("appends").Add(10)
		first, err := c.Snapshot(t.Context())
		require.NoError(t, err)
		firstQ, ok := first.Queue("q")
		require.True(t, ok)
		require.Zero(t, firstQ.Rates.AppendsPerSec)

		synctest.Sleep(time.Second)

		scope.Counter("appends").Add(10)
		second, err := c.Snapshot(t.Context())
		require.NoError(t, err)
		q, ok := second.Queue("q")
		require.True(t, ok)
		assert.InDelta(t, 10, q.Rates.AppendsPerSec, 1e-9)
	})
}
