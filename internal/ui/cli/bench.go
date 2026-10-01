package cli

import (
	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/ui/command"
)

func init() {
	handlers["bench"] = (*invocation).handleBench
	handlers["bench produce"] = (*invocation).handleBenchProduce
	handlers["bench consume"] = (*invocation).handleBenchConsume
	handlers["bench mix"] = (*invocation).handleBenchMix
	handlers["bench recovery"] = (*invocation).handleBenchRecovery
	handlers["bench show"] = (*invocation).handleBenchShow
	handlers["bench compare"] = (*invocation).handleBenchCompare
	handlers["bench plot"] = (*invocation).handleBenchPlot
}

// handleBench prints the benchmark subcommand list.
func (in *invocation) handleBench(_ []string) error {
	v, ok := command.LookupVerb("bench")
	if !ok {
		return nil
	}
	return command.WriteVerb(in.app.Stdout, v, command.RenderOptions{Mode: command.ModeCLI, Color: isWriterTerminal(in.app.Stdout)})
}

// runBench runs one benchmark mode and reports its run directory.
func (in *invocation) runBench(mode bench.Mode, args []string) error {
	_, err := command.RunBench(in.ctx, mode, args, in.app.Stdout, in.app.Stderr, in.session)
	return err
}

func (in *invocation) handleBenchProduce(args []string) error {
	return in.runBench(bench.ModeProduce, args)
}

func (in *invocation) handleBenchConsume(args []string) error {
	return in.runBench(bench.ModeConsume, args)
}

func (in *invocation) handleBenchMix(args []string) error {
	return in.runBench(bench.ModeMix, args)
}

func (in *invocation) handleBenchRecovery(args []string) error {
	return in.runBench(bench.ModeRecovery, args)
}

func (in *invocation) handleBenchShow(args []string) error {
	if len(args) != 1 {
		return in.app.usage("usage: mikyu bench show <run-dir>")
	}
	return command.ShowRun(in.app.Stdout, args[0])
}

func (in *invocation) handleBenchCompare(args []string) error {
	if len(args) < 2 {
		return in.app.usage("usage: mikyu bench compare <dir> <dir> [dir...]")
	}
	return command.CompareRuns(in.app.Stdout, args)
}

func (in *invocation) handleBenchPlot(args []string) error {
	var metrics []string
	rest, err := command.ParseAnywhere(command.PlotFlagSet(&metrics), args)
	if err != nil {
		return in.app.usage("bench plot: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu bench plot <run-dir> [--metric <name>]...")
	}
	return command.PlotRun(in.app.Stdout, rest[0], metrics)
}
