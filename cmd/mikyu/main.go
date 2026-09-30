package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/ui/cli"
	"github.com/dmunish/mikyu/internal/ui/repl"
)

const defaultTarget = "tcp://127.0.0.1:7420"

func main() {
	err := run(os.Args[1:])
	if err != nil && !errors.Is(err, daemon.ErrUsage) {
		// ErrUsage details (with usage text) were already written to stderr
		// by the failing command; everything else gets the standard line.
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	os.Exit(exitCode(err))
}

// exitCode maps run's result to a process exit code: 0 success, 1 general
// error, 2 command-line usage error, 3 a failed payload verification.
func exitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, daemon.ErrUsage), errors.Is(err, bench.ErrUsage):
		return 2
	}
	var verify *bench.VerifyError
	if errors.As(err, &verify) {
		return 3
	}
	return 1
}

func run(args []string) error {
	if len(args) == 0 {
		return repl.RunREPL(defaultTarget, nil)
	}

	switch args[0] {
	case "version", "--version":
		fmt.Printf("mikyu (spec version %d)\n", mikyu.SpecVersion)
		return nil
	case "daemon", "server":
		return daemon.Run(args[1:])
	case "bench-child":
		return bench.RunChild(args[1:])
	case "shell", "repl":
		return repl.RunREPL(defaultTarget, args[1:])
	case "cli":
		return cli.RunCLI(defaultTarget, args[1:])
	default:
		return cli.RunCLI(defaultTarget, args)
	}
}
