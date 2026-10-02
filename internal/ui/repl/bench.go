package repl

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/dmunish/mikyu/internal/ui/command"
)

func init() {
	replHandlers["bench"] = (*replSession).handleBench
	replHandlers["bench run"] = (*replSession).handleBenchRun
	replHandlers["bench show"] = (*replSession).handleBenchShow
	replHandlers["bench compare"] = (*replSession).handleBenchCompare
	replHandlers["bench plot"] = (*replSession).handleBenchPlot
}

// handleBench prints the benchmark subcommand list.
func (s *replSession) handleBench(_ []string) {
	v, ok := command.LookupVerb("bench")
	if !ok {
		return
	}
	_ = command.WriteVerb(os.Stdout, v, command.RenderOptions{Mode: command.ModeREPL, Color: true})
}

// handleBenchRun measures a workload, aborting on Ctrl+C, and remembers the run directory.
func (s *replSession) handleBenchRun(args []string) {
	ctx, stop := signal.NotifyContext(s.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	dir, err := command.RunBench(ctx, args, os.Stdout, os.Stderr, s.session)
	if err != nil {
		s.fail(err)
		return
	}
	s.lastBenchRun = dir
}

// benchDir resolves a run directory from args, falling back to the last run.
func (s *replSession) benchDir(args []string) string {
	if len(args) == 1 {
		return args[0]
	}
	return s.lastBenchRun
}

func (s *replSession) handleBenchShow(args []string) {
	dir := s.benchDir(args)
	if dir == "" {
		s.usage("bench show <run-dir>")
		return
	}
	if err := command.ShowRun(os.Stdout, dir); err != nil {
		s.fail(err)
	}
}

func (s *replSession) handleBenchCompare(args []string) {
	if len(args) < 2 {
		s.usage("bench compare <dir> <dir> [dir...]")
		return
	}
	if err := command.CompareRuns(os.Stdout, args); err != nil {
		s.fail(err)
	}
}

func (s *replSession) handleBenchPlot(args []string) {
	var metrics []string
	rest, err := command.ParseAnywhere(command.PlotFlagSet(&metrics), args)
	if err != nil {
		s.usage("bench plot <run-dir> [--metric <name>] (" + err.Error() + ")")
		return
	}
	dir := s.benchDir(rest)
	if dir == "" {
		s.usage("bench plot <run-dir> [--metric <name>]")
		return
	}
	if err := command.PlotRun(os.Stdout, dir, metrics); err != nil {
		s.fail(err)
	}
}
