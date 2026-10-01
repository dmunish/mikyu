package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu"
)

const (
	runsFileName = ".bench-runs"
	runsTmpName  = ".bench-runs.tmp"
)

// benchRuns is the on-disk ownership record for the runs sharing a data directory.
type benchRuns struct {
	Runs []runEntry `json:"runs"`
}

// runEntry records one run's queues and any daemon it owns.
type runEntry struct {
	RunID     string     `json:"run_id"`
	StartedAt time.Time  `json:"started_at"`
	Queues    []queueRef `json:"queues"`
	DaemonPID int        `json:"daemon_pid,omitempty"`
}

// queueRef names one queue and its structural kind.
type queueRef struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// acquire registers a run in <dir>/.bench-runs after reconciling stale entries.
func acquire(ctx context.Context, client mikyu.Client, dir, runID string, queues []queueRef, daemonPID int, cleanupStale bool, log func(string)) (func() error, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if cleanupStale {
		if err := reconcileStale(ctx, client, dir, log); err != nil {
			return nil, err
		}
	}
	runs, err := readRuns(dir)
	if err != nil {
		return nil, err
	}
	runs.Runs = append(runs.Runs, runEntry{RunID: runID, StartedAt: time.Now(), Queues: queues, DaemonPID: daemonPID})
	if err := writeRuns(dir, runs); err != nil {
		return nil, err
	}
	return func() error { return removeRun(dir, runID) }, nil
}

// reconcileStale removes queues and daemons recorded by previous runs through the supplied client.
func reconcileStale(ctx context.Context, client mikyu.Client, dir string, log func(string)) error {
	runs, err := readRuns(dir)
	if err != nil || len(runs.Runs) == 0 {
		return err
	}
	for _, entry := range runs.Runs {
		for _, q := range entry.Queues {
			err := client.DeleteQueue(ctx, q.Name)
			switch {
			case err == nil:
				logf(log, "cleaned: queue %s (stale)", q.Name)
			case errors.Is(err, mikyu.ErrQueueNotFound), errors.Is(err, mikyu.ErrWrongKind):
				_ = err
			default:
				logf(log, "cleanup: queue %s: %v", q.Name, err)
			}
		}
		if entry.DaemonPID != 0 {
			if err := terminatePID(entry.DaemonPID); err != nil {
				logf(log, "cleanup: daemon pid %d: %v", entry.DaemonPID, err)
			} else {
				logf(log, "cleaned: daemon pid %d (stale)", entry.DaemonPID)
			}
		}
	}
	runs.Runs = nil
	return writeRuns(dir, runs)
}

// killStaleDaemons terminates any daemon recorded in <dir>/.bench-runs so the directory lock is free.
func killStaleDaemons(dir string, log func(string)) error {
	runs, err := readRuns(dir)
	if err != nil {
		return err
	}
	for _, entry := range runs.Runs {
		if entry.DaemonPID == 0 {
			continue
		}
		if err := terminatePID(entry.DaemonPID); err != nil {
			logf(log, "stale daemon pid %d: %v", entry.DaemonPID, err)
			continue
		}
		logf(log, "killed stale daemon pid %d", entry.DaemonPID)
	}
	return nil
}

// removeRun drops one run's entry, leaving the others untouched.
func removeRun(dir, runID string) error {
	runs, err := readRuns(dir)
	if err != nil {
		return err
	}
	runs.Runs = slices.DeleteFunc(runs.Runs, func(e runEntry) bool { return e.RunID == runID })
	return writeRuns(dir, runs)
}

// NewRunID returns a run identifier unique to this process.
func NewRunID() string { return generateRunID() }

// generateRunID returns a run identifier unique to this process.
func generateRunID() string {
	for {
		now := time.Now().UnixNano()
		prev := lastRunID.Load()
		if now <= prev {
			now = prev + 1
		}
		if lastRunID.CompareAndSwap(prev, now) {
			return "bench-" + strconv.FormatInt(now, 10)
		}
	}
}

// lastRunID makes run identifiers strictly increasing even when the clock is coarse.
var lastRunID atomic.Int64

// queueNames returns the queue names a run owns.
func queueNames(runID string, count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", runID, i)
	}
	return names
}

// readRuns loads the ownership record; a missing file reads as an empty record.
func readRuns(dir string) (benchRuns, error) {
	data, err := os.ReadFile(filepath.Join(dir, runsFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return benchRuns{}, nil
		}
		return benchRuns{}, err
	}
	var runs benchRuns
	if err := json.Unmarshal(data, &runs); err != nil {
		return benchRuns{}, err
	}
	return runs, nil
}

// writeRuns replaces the ownership record atomically through a temp file and rename.
func writeRuns(dir string, runs benchRuns) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(runs, "", "  ")
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	f, err := root.OpenFile(runsTmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		_ = root.Remove(runsTmpName)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = root.Remove(runsTmpName)
		return err
	}
	if err := f.Close(); err != nil {
		_ = root.Remove(runsTmpName)
		return err
	}
	return root.Rename(runsTmpName, runsFileName)
}

// logf forwards a formatted message when the caller supplied a logger.
func logf(log func(string), format string, args ...any) {
	if log != nil {
		log(fmt.Sprintf(format, args...))
	}
}
