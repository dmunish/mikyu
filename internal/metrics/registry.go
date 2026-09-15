package metrics

import (
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
)

// Scope is a named attachment point for instruments.
type Scope struct {
	name       string
	mu         sync.Mutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

// Registry owns every scope and the collector configuration.
type Registry struct {
	mu     sync.RWMutex
	cfg    Config
	scopes map[string]*Scope
}

// NewRegistry creates an empty registry.
func NewRegistry(cfg Config) *Registry {
	return &Registry{cfg: cfg, scopes: make(map[string]*Scope)}
}

// Config returns the registry's configuration.
func (r *Registry) Config() Config { return r.cfg }

// Scope returns the named scope, creating it on first use.
func (r *Registry) Scope(name string) *Scope {
	r.mu.RLock()
	s := r.scopes[name]
	r.mu.RUnlock()
	if s != nil {
		return s
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if s = r.scopes[name]; s != nil {
		return s
	}
	s = &Scope{
		name:       name,
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
	r.scopes[name] = s
	return s
}

// Scopes yields every scope in name order.
func (r *Registry) Scopes() iter.Seq[*Scope] {
	r.mu.RLock()
	scopes := slices.Collect(maps.Values(r.scopes))
	r.mu.RUnlock()
	slices.SortFunc(scopes, func(a, b *Scope) int {
		return strings.Compare(a.name, b.name)
	})
	return slices.Values(scopes)
}

// Name returns the scope's name.
func (s *Scope) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

// Counter returns the named counter, creating it on first use.
func (s *Scope) Counter(name string) *Counter {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.counters[name]
	if c == nil {
		c = &Counter{}
		s.counters[name] = c
	}
	return c
}

// Gauge returns the named gauge, creating it on first use.
func (s *Scope) Gauge(name string) *Gauge {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gauges[name]
	if g == nil {
		g = &Gauge{}
		s.gauges[name] = g
	}
	return g
}

// Histogram returns the named histogram, creating it with bounds on first use.
func (s *Scope) Histogram(name string, bounds []float64) *Histogram {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.histograms[name]
	if h == nil {
		h = NewHistogram(bounds)
		s.histograms[name] = h
	}
	return h
}

// read copies every instrument reading held by the scope.
func (s *Scope) read() (map[string]int64, map[string]int64, map[string]HistogramSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	counters := make(map[string]int64, len(s.counters))
	gauges := make(map[string]int64, len(s.gauges))
	histograms := make(map[string]HistogramSnapshot, len(s.histograms))

	for name, c := range s.counters {
		counters[name] = c.Load()
	}
	for name, g := range s.gauges {
		gauges[name] = g.Load()
	}
	for name, h := range s.histograms {
		histograms[name] = h.Snapshot()
	}
	return counters, gauges, histograms
}
