package metrics

import "context"

// Source is a pull-based producer of snapshot fields.
type Source interface {
	Collect(ctx context.Context, dst *Snapshot)
}

// SourceFunc adapts a function to Source.
type SourceFunc func(context.Context, *Snapshot)

// Collect calls f.
func (f SourceFunc) Collect(ctx context.Context, dst *Snapshot) { f(ctx, dst) }
