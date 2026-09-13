package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScopeCreateOnFirstUse(t *testing.T) {
	reg := NewRegistry(Config{})
	s := reg.Scope("q")
	require.Same(t, s, reg.Scope("q"), "scope is cached")
	assert.Equal(t, "q", s.Name())
}

func TestScopeInstrumentsAreStable(t *testing.T) {
	reg := NewRegistry(Config{})
	s := reg.Scope("q")
	c := s.Counter("appends")
	c.Inc()
	assert.Same(t, c, s.Counter("appends"))
	assert.EqualValues(t, 1, s.Counter("appends").Load())

	g := s.Gauge("messages")
	g.Set(7)
	assert.Same(t, g, s.Gauge("messages"))

	h := s.Histogram("op", []float64{1, 2})
	h.Observe(1.5)
	assert.Same(t, h, s.Histogram("op", []float64{9}))
	assert.EqualValues(t, 1, h.Snapshot().Total)
}

func TestScopeNilSafe(t *testing.T) {
	var s *Scope
	assert.NotPanics(t, func() {
		s.Counter("x").Inc()
		s.Gauge("y").Set(1)
		s.Histogram("z", nil).Observe(1)
	})
	assert.Equal(t, "", s.Name())
	assert.Zero(t, s.Counter("x").Load())
	assert.Zero(t, s.Gauge("y").Load())
	assert.Zero(t, s.Histogram("z", nil).Snapshot().Total)
}

func TestRegistryScopesOrdered(t *testing.T) {
	reg := NewRegistry(Config{})
	reg.Scope("gamma")
	reg.Scope("alpha")
	reg.Scope("beta")
	var names []string
	for s := range reg.Scopes() {
		names = append(names, s.Name())
	}
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, names)
}
