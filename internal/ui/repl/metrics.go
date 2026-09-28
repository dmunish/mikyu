package repl

import (
	"os"

	"github.com/dmunish/mikyu/internal/ui/command"
)

// handleMetrics renders a telemetry snapshot, watching until interrupted when asked.
func (s *replSession) handleMetrics(args []string) {
	var opts command.MetricsOptions
	fs := command.MetricsFlagSet(&opts)
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("metrics [flags] (" + err.Error() + ")")
		return
	}
	if len(rest) > 0 {
		s.usage("metrics [--detail] [--queue <name>]... [--watch] [--json]")
		return
	}
	opts.Width = command.GetTerminalWidth()
	if err := command.RunMetrics(s.ctx, s.client, os.Stdout, opts); err != nil {
		s.fail(err)
	}
}
