package cli

import (
	"github.com/dmunish/mikyu/internal/ui/command"
)

// handleMetrics renders a telemetry snapshot in the selected output mode.
func (in *invocation) handleMetrics(args []string) error {
	var opts command.MetricsOptions
	fs := command.MetricsFlagSet(&opts)
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("metrics: %v", err)
	}
	if len(rest) > 0 {
		return in.app.usage("usage: mikyu metrics [--detail] [--queue <name>]... [--watch] [--json]")
	}
	return command.RunMetrics(in.ctx, in.client, in.app.Stdout, opts)
}
