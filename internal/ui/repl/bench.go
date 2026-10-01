package repl

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/ui/command"
)

func init() {
	replHandlers["bench"] = (*replSession).handleBench
	replHandlers["bench produce"] = (*replSession).handleBenchProduce
	replHandlers["bench consume"] = (*replSession).handleBenchConsume
	replHandlers["bench mix"] = (*replSession).handleBenchMix
	replHandlers["bench recovery"] = (*replSession).handleBenchRecovery
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

// runBench runs one benchmark mode, aborting on Ctrl+C, and remembers the run directory.
func (s *replSession) runBench(mode bench.Mode, args []string) {
	ctx, stop := signal.NotifyContext(s.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	dir, err := command.RunBench(ctx, mode, args, os.Stdout, os.Stderr, s.session)
	if err != nil {
		s.fail(err)
		return
	}
	s.lastBenchRun = dir
}

func (s *replSession) handleBenchProduce(args []string) { s.runBench(bench.ModeProduce, args) }

func (s *replSession) handleBenchConsume(args []string) { s.runBench(bench.ModeConsume, args) }

func (s *replSession) handleBenchMix(args []string) { s.runBench(bench.ModeMix, args) }

func (s *replSession) handleBenchRecovery(args []string) { s.runBench(bench.ModeRecovery, args) }

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
