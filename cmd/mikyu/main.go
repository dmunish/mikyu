package main

import (
	"fmt"
	"os"

	"github.com/dmunish/mikyu/internal/cli"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/repl"
)

const defaultTarget = "tcp://127.0.0.1:7420"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		repl.RunREPL(defaultTarget)
		return nil
	}

	switch args[0] {
	case "daemon", "server":
		configPath := ""
		if len(args) > 1 {
			configPath = args[1]
		}
		return daemon.Run(configPath)
	case "shell", "repl":
		repl.RunREPL(defaultTarget)
		return nil
	case "cli":
		cli.RunCLI(defaultTarget, args[1:])
		return nil
	default:
		cli.RunCLI(defaultTarget, args)
		return nil
	}
}
