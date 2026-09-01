package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"charm.land/log/v2"
	"github.com/dmunish/mikyu/internal/config"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/server"
)

func main() {
	targetFlag := flag.String("target", "tcp://127.0.0.1:7420", "Daemon target address")
	configFlag := flag.String("config", "", "Path to mikyu.yaml configuration file")
	flag.Usage = printUsage

	// Handle bare binary execution (e.g. double-click in GUI or bare terminal run)
	if len(os.Args) == 1 {
		RunREPL(*targetFlag)
		return
	}

	subCmd := os.Args[1]

	switch subCmd {
	case "daemon", "server":
		runDaemonServer(*configFlag)

	case "shell", "repl":
		RunREPL(*targetFlag)

	case "tui", "dashboard":
		fmt.Println("Connecting TUI dashboard to active Mikyu daemon...")

	case "help", "-h", "--help":
		printUsage()

	default:
		// Route one-shot CLI commands (list, create, produce, read, tail, stats, shutdown)
		RunCLI(*targetFlag, os.Args[1:])
	}
}

func runDaemonServer(configPath string) {
	cfg, pathUsed, err := config.Load(configPath)
	if err != nil {
		log.Fatal("Failed to load configuration", "err", err)
	}

	log.Info("Starting Mikyu Message Queue Daemon...", "version", "0.1.0")
	log.Info("Loaded configuration", "path", pathUsed)
	log.Info("Initializing storage engine...", "data_dir", cfg.Server.DataDir)

	engineCfg := config.ToEngineConfig(cfg.Engine)
	mgr, err := engine.OpenManager(cfg.Server.DataDir, engineCfg)
	if err != nil {
		log.Fatal("Storage engine failed to start", "err", err)
	}

	// Reconcile declarative queues specified in mikyu.yaml
	for _, qCfg := range cfg.Queues {
		qEngineCfg := config.ToQueueEngineConfig(cfg.Engine, qCfg)
		if _, err := mgr.CreateQueue(qCfg.Name, qEngineCfg); err == nil {
			log.Info("Reconciled declarative queue", "queue", qCfg.Name)
		}
	}

	srv := server.New(mgr)
	log.Info("Daemon listening for client connections", "target", cfg.Server.Target)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("Received termination signal, stopping daemon...", "signal", sig)
		_ = mgr.Close()
		srv.Stop()
		os.Exit(0)
	}()

	if err := srv.ListenAndServe(cfg.Server.Target); err != nil {
		log.Fatal("Daemon stopped unexpectedly", "err", err)
	}
}

func printUsage() {
	fmt.Print(`mikyu - Unified Message Queue Daemon, REPL & Operator Tool

Usage:
  mikyu                      Auto-start daemon in background (if needed) & open REPL
  mikyu shell                Open interactive REPL session
  mikyu daemon               Start server daemon in foreground mode
  mikyu produce <q> <msg|@f> Produce text, file payload (@file.json), or stdin (-)
  mikyu read <queue> <off>   Read record at specific logical offset
  mikyu tail <queue> [off]   Stream live records continuously
  mikyu list                 List all active queues
  mikyu stats                Display engine storage metrics
  mikyu shutdown             Stop the background daemon process
`)
}
