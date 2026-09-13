package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
)

// App is one CLI process surface: the streams it reads and writes, plus the confirmation hook tests replace.
type App struct {
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Confirm func(prompt string) (bool, error)
}

// RunCLI runs one command and reports its outcome to the process streams.
func RunCLI(target string, args []string) error {
	return (&App{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr}).Run(target, args)
}

// invocation carries one verb's resolved client and global flags.
type invocation struct {
	app    *App
	ctx    context.Context
	client mikyu.Client
	dir    string
	target string
}

// handlers maps every verb to its implementation.
var handlers = map[string]func(*invocation, []string) error{
	"list":     (*invocation).handleList,
	"create":   (*invocation).handleCreate,
	"update":   (*invocation).handleUpdate,
	"describe": (*invocation).handleDescribe,
	"delete":   (*invocation).handleDelete,
	"append":   (*invocation).handleAppend,
	"read":     (*invocation).handleRead,
	"tail":     (*invocation).handleTail,
	"truncate": (*invocation).handleTruncate,

	"enqueue":  (*invocation).handleEnqueue,
	"dequeue":  (*invocation).handleDequeue,
	"ack":      (*invocation).handleAck,
	"nack":     (*invocation).handleNack,
	"extend":   (*invocation).handleExtend,
	"dlq":      (*invocation).handleDLQ,
	"dlq-ack":  (*invocation).handleDLQAck,
	"info":     (*invocation).handleInfo,
	"shutdown": (*invocation).handleShutdown,
}

// Run parses the global flags, resolves the client, and dispatches one verb.
func (a *App) Run(target string, args []string) error {
	global, rest, err := command.ParseGlobal(target, args)
	if err != nil {
		return a.usage("%v", err)
	}
	if global.Help {
		a.printUsage()
		return nil
	}
	if len(rest) == 0 {
		a.printUsage()
		return a.usage("no command given")
	}

	verb, args := rest[0], rest[1:]
	handler, ok := handlers[verb]
	if !ok {
		a.printUsage()
		return a.usage("unknown command: %s", verb)
	}
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			a.printVerbUsage(verb)
			return nil
		}
	}

	client, err := command.OpenClient(global.Session)
	if err != nil {
		return err
	}
	defer client.Close()

	in := &invocation{
		app:    a,
		ctx:    context.Background(),
		client: client,
		dir:    global.Session.DataDir,
		target: global.Session.Target,
	}
	return handler(in, args)
}

// usage reports a command-line mistake and marks it as exit code 2.
func (a *App) usage(format string, args ...any) error {
	fmt.Fprintf(a.Stderr, format+"\n", args...)
	return daemon.ErrUsage
}

// confirm asks before an irreversible change, unless -y/--yes skipped it.
func (a *App) confirm(prompt string, skip bool) (bool, error) {
	if skip {
		return true, nil
	}
	if a.Confirm != nil {
		return a.Confirm(prompt)
	}
	if !isTerminal(os.Stdin) {
		return false, errors.New("cannot prompt in non-interactive mode; use -y/--yes")
	}
	fmt.Fprint(a.Stdout, prompt)
	var answer string
	if _, err := fmt.Fscanln(a.Stdin, &answer); err != nil {
		return false, err
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	return answer == "y" || answer == "yes", nil
}

func (a *App) printUsage() {
	fmt.Fprintf(a.Stdout, "Usage: mikyu [--dir <path> | --target <uri>] <command> [args]\n\n")
	for _, kind := range []command.VerbKind{command.KindLog, command.KindTask, command.KindAdmin} {
		fmt.Fprintf(a.Stdout, "%s commands:\n", strings.ToUpper(kind.String()[:1])+kind.String()[1:])
		for _, v := range command.VerbsOfKind(kind) {
			line := v.Name
			if v.Args != "" {
				line += " " + v.Args
			}
			fmt.Fprintf(a.Stdout, "  %-42s %s\n", line, v.Summary)
		}
		fmt.Fprintln(a.Stdout)
	}
	fmt.Fprintf(a.Stdout, "Global flags:\n")
	fmt.Fprintf(a.Stdout, "  %-24s %s\n", "--dir <path>", "run embedded against a data directory")
	for _, p := range spec.Params(spec.LayerSession) {
		fmt.Fprintf(a.Stdout, "  %-24s %s\n", "--"+p.FlagName+" <"+p.Kind.String()+">", p.Path)
	}
}

// printVerbUsage lists a verb's flags, which come from spec.Table.
func (a *App) printVerbUsage(verb string) {
	v, _ := command.LookupVerb(verb)
	fmt.Fprintf(a.Stdout, "Usage: mikyu %s %s\n\n%s\n", v.Name, v.Args, v.Summary)

	var fs interface {
		SetOutput(io.Writer)
		PrintDefaults()
	}
	switch verb {
	case "create":
		var s spec.Spec
		fs = command.QueueFlagSet(verb, &s)
	case "update":
		var s spec.Spec
		fs = command.QueuePatchFlagSet(verb, &s)
	case "enqueue":
		fs = command.EnqueueFlagSet(verb, &command.EnqueueOptions{})
	case "nack":
		var retry time.Duration
		var explicit bool
		fs = command.NackFlagSet(verb, &retry, &explicit)
	default:
		return
	}
	fmt.Fprintln(a.Stdout, "\nFlags:")
	fs.SetOutput(a.Stdout)
	fs.PrintDefaults()
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// newFlagSet returns a silent flag set for the presentation flags that are not
// spec parameters (--json, -y, --timeout).
func newFlagSet(name string) *flag.FlagSet { return command.NewFlagSet(name) }

func (in *invocation) handleList(args []string) error {
	if len(args) > 0 {
		return in.app.usage("usage: mikyu list")
	}
	queues, err := command.List(in.ctx, in.client)
	if err != nil {
		return err
	}
	for _, q := range queues {
		fmt.Fprintln(in.app.Stdout, q)
	}
	return nil
}

func (in *invocation) handleCreate(args []string) error {
	var patch spec.Spec
	rest, err := command.ParseAnywhere(command.QueueFlagSet("create", &patch), args)
	if err != nil {
		return in.app.usage("create: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu create [flags] <queue>")
	}
	if err := command.Create(in.ctx, in.client, rest[0], command.SpecOption(patch)); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "created queue %q\n", rest[0])
	return nil
}

func (in *invocation) handleUpdate(args []string) error {
	var patch spec.Spec
	var yes bool
	fs := command.QueuePatchFlagSet("update", &patch)
	fs.BoolVar(&yes, "y", false, "skip the retention-shrink confirmation")
	fs.BoolVar(&yes, "yes", false, "skip the retention-shrink confirmation")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("update: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu update [flags] <queue>")
	}
	name := rest[0]

	if patch.Retention.Age != nil || patch.Retention.Bytes != nil {
		current, err := command.Describe(in.ctx, in.client, name)
		if err != nil {
			return err
		}
		if command.TightensRetention(current.Effective, patch) {
			ok, err := in.app.confirm(fmt.Sprintf("shrink retention for queue %q? [y/N]: ", name), yes)
			if err != nil {
				return err
			}
			if !ok {
				fmt.Fprintln(in.app.Stdout, "aborted")
				return nil
			}
		}
	}

	if err := command.Update(in.ctx, in.client, name, command.SpecOption(patch)); err != nil {
		if errors.Is(err, mikyu.ErrImmutableField) {
			return in.app.usage("error: %v", err)
		}
		return err
	}
	fmt.Fprintf(in.app.Stdout, "updated queue %q\n", name)
	return nil
}

func (in *invocation) handleDescribe(args []string) error {
	var asJSON bool
	fs := newFlagSet("describe")
	fs.BoolVar(&asJSON, "json", false, "emit the machine-readable description")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("describe: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu describe <queue> [--json]")
	}
	d, err := command.Describe(in.ctx, in.client, rest[0])
	if err != nil {
		return err
	}
	if asJSON {
		data, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(in.app.Stdout, string(data))
		return nil
	}
	stored, _ := json.Marshal(d.Spec)
	effective, _ := json.Marshal(d.Effective)
	defaults, _ := json.Marshal(d.Defaults)
	fmt.Fprintf(in.app.Stdout, "queue:     %s\n", rest[0])
	fmt.Fprintf(in.app.Stdout, "stored:    %s\n", stored)
	fmt.Fprintf(in.app.Stdout, "effective: %s\n", effective)
	fmt.Fprintf(in.app.Stdout, "defaults:  %s\n", defaults)
	fmt.Fprintf(in.app.Stdout, "messages=%d segments=%d size=%d last_offset=%d min_offset=%d\n",
		d.QueueStats.MessageCount, d.QueueStats.SegmentCount, d.QueueStats.SizeBytes,
		d.QueueStats.LastOffset, d.QueueStats.MinOffset)
	return nil
}

func (in *invocation) handleDelete(args []string) error {
	var yes bool
	fs := newFlagSet("delete")
	fs.BoolVar(&yes, "y", false, "skip the confirmation")
	fs.BoolVar(&yes, "yes", false, "skip the confirmation")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("delete: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu delete <queue> [-y]")
	}
	name := rest[0]

	executed, err := command.Delete(in.ctx, in.client, name, in.confirmFn(fmt.Sprintf("delete queue %q? [y/N]: ", name), yes))
	if err != nil {
		return err
	}
	if !executed {
		fmt.Fprintln(in.app.Stdout, "aborted")
		return nil
	}
	fmt.Fprintf(in.app.Stdout, "deleted queue %q\n", name)
	return nil
}

func (in *invocation) handleAppend(args []string) error {
	if len(args) != 2 {
		return in.app.usage("usage: mikyu append <queue> <payload|@file|->")
	}
	offset, _, err := command.Append(in.ctx, in.client, args[0], args[1])
	if err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "appended at offset %d\n", offset)
	return nil
}

func (in *invocation) handleRead(args []string) error {
	if len(args) != 2 {
		return in.app.usage("usage: mikyu read <queue> <offset>")
	}
	offset, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return in.app.usage("invalid offset: %v", err)
	}
	msg, err := command.Read(in.ctx, in.client, args[0], offset)
	if err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "offset=%d id=%s ts=%s payload=%s\n",
		msg.Offset, msg.ID, msg.Timestamp.Format(time.RFC3339Nano), string(msg.Payload))
	return nil
}

func (in *invocation) handleTail(args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return in.app.usage("usage: mikyu tail <queue> [offset]")
	}
	var start uint64
	if len(args) == 2 {
		var err error
		start, err = strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return in.app.usage("invalid offset: %v", err)
		}
	}

	err := command.Tail(in.ctx, in.client, args[0], start, func(msg mikyu.Message) error {
		prefix := fmt.Sprintf("offset=%d id=%s ts=%s payload=", msg.Offset, msg.ID, msg.Timestamp.Format(time.RFC3339Nano))
		termWidth := command.GetTerminalWidth()
		avail := -1
		if termWidth > 0 {
			avail = termWidth - len(prefix)
		}
		fmt.Fprintf(in.app.Stdout, "%s%s\n", prefix, command.FormatSingleLinePayload(msg.Payload, avail))
		return nil
	})
	return err
}

func (in *invocation) handleTruncate(args []string) error {
	var yes bool
	fs := newFlagSet("truncate")
	fs.BoolVar(&yes, "y", false, "skip the confirmation")
	fs.BoolVar(&yes, "yes", false, "skip the confirmation")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("truncate: %v", err)
	}
	if len(rest) < 1 || len(rest) > 2 {
		return in.app.usage("usage: mikyu truncate <queue> [offset] [-y]")
	}
	name := rest[0]
	var minOffset uint64
	if len(rest) == 2 {
		minOffset, err = strconv.ParseUint(rest[1], 10, 64)
		if err != nil {
			return in.app.usage("invalid offset: %v", err)
		}
	}

	prompt := fmt.Sprintf("truncate physical log segments for queue %q? [y/N]: ", name)
	if minOffset > 0 {
		prompt = fmt.Sprintf("truncate physical log segments before offset %d for queue %q? [y/N]: ", minOffset, name)
	}

	reclaimed, executed, err := command.Truncate(in.ctx, in.client, name, minOffset, in.confirmFn(prompt, yes))
	if err != nil {
		return err
	}
	if !executed {
		fmt.Fprintln(in.app.Stdout, "aborted")
		return nil
	}
	fmt.Fprintf(in.app.Stdout, "truncated queue %q (%d bytes reclaimed)\n", name, reclaimed)
	return nil
}

func (in *invocation) handleInfo(args []string) error {
	var asJSON bool
	fs := newFlagSet("info")
	fs.BoolVar(&asJSON, "json", false, "emit the machine-readable info payload")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("info: %v", err)
	}
	if len(rest) != 0 {
		return in.app.usage("usage: mikyu info [--json]")
	}
	info, err := command.Info(in.ctx, in.client)
	if err != nil {
		return err
	}
	if asJSON {
		data, err := json.MarshalIndent(info, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(in.app.Stdout, string(data))
		return nil
	}
	fmt.Fprintf(in.app.Stdout, "version: %s\n", info.Version)
	fmt.Fprintf(in.app.Stdout, "uptime: %s\n", time.Duration(info.UptimeNs))
	fmt.Fprintf(in.app.Stdout, "data_dir: %s\n", info.DataDir)
	fmt.Fprintf(in.app.Stdout, "active_queues: %d\n", info.ActiveQueues)
	fmt.Fprintf(in.app.Stdout, "total_bytes: %d\n", info.TotalBytes)
	for name, qs := range info.Queues {
		fmt.Fprintf(in.app.Stdout, "queue %s: messages=%d, segments=%d, size=%d, last_offset=%d, min_offset=%d\n",
			name, qs.MessageCount, qs.SegmentCount, qs.SizeBytes, qs.LastOffset, qs.MinOffset)
	}
	return nil
}

func (in *invocation) handleShutdown(args []string) error {
	var yes bool
	fs := newFlagSet("shutdown")
	fs.BoolVar(&yes, "y", false, "skip the confirmation")
	fs.BoolVar(&yes, "yes", false, "skip the confirmation")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("shutdown: %v", err)
	}
	if len(rest) != 0 {
		return in.app.usage("usage: mikyu shutdown [-y]")
	}
	if in.dir != "" {
		return in.app.usage("shutdown needs a daemon: --dir runs embedded and has no daemon to stop")
	}

	executed, err := command.Shutdown(in.target, in.confirmFn("shutdown daemon? [y/N]: ", yes))
	if err != nil {
		return err
	}
	if !executed {
		fmt.Fprintln(in.app.Stdout, "aborted")
		return nil
	}
	fmt.Fprintln(in.app.Stdout, "shutdown signal sent")
	return nil
}

// confirmFn returns the confirmation callback for command helpers: nil when the
// prompt was skipped with -y.
func (in *invocation) confirmFn(prompt string, skip bool) func() (bool, error) {
	if skip {
		return nil
	}
	return func() (bool, error) { return in.app.confirm(prompt, false) }
}
