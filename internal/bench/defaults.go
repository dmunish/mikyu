package bench

import "github.com/dmunish/mikyu/internal/spec"

// defaultSpecRows are the queue parameters a run overrides, keyed and valued as the table declares them.
var defaultSpecRows = [][2]string{
	{"durability", "async"},
	{"max_segment", "268435456"},
	{"max_cache", "134217728"},
	{"max_flush_bytes", "4194304"},
}

// DefaultSpec is the queue configuration a run starts from: batched appends on a background flush, sized for sustained traffic.
func DefaultSpec() spec.Spec {
	var s spec.Spec
	for _, row := range defaultSpecRows {
		p, ok := spec.Lookup(row[0])
		if !ok {
			panic("bench: unknown default queue parameter " + row[0])
		}
		v, err := p.Parse(row[1])
		if err != nil {
			panic("bench: unparseable default for " + row[0])
		}
		p.Apply(&s, v)
	}
	return s
}
