package metrics

import (
	"context"
	"encoding/json"
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

func TestMergeScopesTaskGauges(t *testing.T) {
	reg := NewRegistry(Config{})
	scope := reg.Scope("jobs")
	scope.Gauge("active").Set(3)
	scope.Gauge("in_flight").Set(1)
	scope.Gauge("scheduled").Set(2)
	scope.Gauge("dead_letters").Set(2)
	scope.Gauge("parked_dequeue").Set(1)

	c := New(reg, Config{Interval: time.Hour})
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	qv, ok := snap.Queue("jobs")
	require.True(t, ok)
	assert.EqualValues(t, 3, qv.Tasks.Active)
	assert.EqualValues(t, 1, qv.Tasks.InFlight)
	assert.EqualValues(t, 2, qv.Tasks.Scheduled)
	assert.EqualValues(t, 2, qv.Tasks.DeadLetters)
	assert.EqualValues(t, 1, qv.Gauges["parked_dequeue"])
	for _, name := range []string{"active", "in_flight", "scheduled", "dead_letters"} {
		assert.NotContains(t, qv.Gauges, name)
	}
}

func TestMergeScopesPartialTaskGauges(t *testing.T) {
	reg := NewRegistry(Config{})
	reg.Scope("jobs").Gauge("active").Set(4)

	c := New(reg, Config{Interval: time.Hour})
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	qv, ok := snap.Queue("jobs")
	require.True(t, ok)
	assert.EqualValues(t, 4, qv.Tasks.Active)
	assert.Zero(t, qv.Tasks.InFlight)
	assert.Zero(t, qv.Tasks.CheckpointLag)

	data, err := json.Marshal(qv)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"active":4`)
	assert.NotContains(t, string(data), `"in_flight":4`)
}
