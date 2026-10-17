package cli

import (
	"context"
	"encoding/json"
	"errors"
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

// App is one CLI process surface: streams plus a replaceable confirmation hook.
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
	app     *App
	ctx     context.Context
	client  mikyu.Client
	dir     string
	target  string
	session spec.Session
}

// handlers maps every dispatched verb to its implementation.
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
	"archive":  (*invocation).handleArchive,
	"metrics":  (*invocation).handleMetrics,
	"login":    (*invocation).handleLogin,
	"logout":   (*invocation).handleLogout,
}

// Run parses the global flags, resolves the client, and dispatches one verb.
func (a *App) Run(target string, args []string) error {
	global, rest, err := command.ParseGlobal(target, args)
	if err != nil {
		return a.usage("%v", err)
	}
	opts := command.RenderOptions{
		Mode:  command.ModeCLI,
		Color: isWriterTerminal(a.Stdout),
	}

	if global.Help || len(rest) == 0 {
		_ = command.WriteIndex(a.Stdout, opts)
		if global.Help {
			return nil
		}
		return a.usage("no command given")
	}

	verb, vargs := rest[0], rest[1:]

	v, path, vargs, ok := command.ResolveVerb(verb, vargs)
	if !ok {
		_ = command.WriteIndex(a.Stdout, opts)
		return a.usage("unknown command: %s", verb)
	}

	// `help` alone prints the index.
	if path == "help" {
		return command.WriteIndex(a.Stdout, opts)
	}

	// Per-verb help is revealed by --help / -h only.
	if command.HasHelpFlag(vargs) {
		return command.WriteVerb(a.Stdout, v, opts)
	}

	// A parent verb with children that was given an unknown child reports it.
	if len(v.Subs) > 0 && path == v.Name && len(vargs) > 0 && !strings.HasPrefix(vargs[0], "-") {
		_ = command.WriteIndex(a.Stdout, opts)
		return a.usage("unknown subcommand: %s %s", v.Name, vargs[0])
	}

	handler, ok := handlers[path]
	if !ok {
		_ = command.WriteIndex(a.Stdout, opts)
		return a.usage("unknown command: %s", verb)
	}

	in := &invocation{
		app:     a,
		ctx:     context.Background(),
		dir:     global.Session.DataDir,
		target:  global.Session.Target,
		session: global.Session,
	}
	if !v.Local {
		client, err := command.OpenClient(global.Session)
		if err != nil {
			if errors.Is(err, mikyu.ErrLocked) {
				return fmt.Errorf("%w (use --target to connect to the running daemon)", err)
			}
			return err
		}
		defer client.Close()
		in.client = client
	}

	err = handler(in, vargs)
	if errors.Is(err, mikyu.ErrLocked) {
		return fmt.Errorf("%w (use --target to connect to the running daemon)", err)
	}
	return err
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

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func isWriterTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && isTerminal(f)
}

// confirmFn returns the confirmation callback for command helpers: nil
// when the prompt was skipped with -y.
func (in *invocation) confirmFn(prompt string, skip bool) func() (bool, error) {
	if skip {
		return nil
	}
	return func() (bool, error) { return in.app.confirm(prompt, false) }
}

// ---- queue verbs ----

func (in *invocation) handleList(args []string) error {
	if len(args) > 0 {
		return in.app.usage("usage: mikyu list")
	}
	queues, err := command.List(in.ctx, in.client)
	if err != nil {
		return err
	}
	for _, q := range queues {
		fmt.Fprintf(in.app.Stdout, "%s\t%s\n", q.Name, q.Kind)
	}
	return nil
}

func (in *invocation) handleCreate(args []string) error {
	var patch spec.Spec
	var typeName string
	fs := command.QueueFlagSet("create", &patch)
	fs.StringVar(&typeName, "type", "", "queue kind: log or task (required)")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("create: %v", err)
	}
	if len(rest) != 1 {
		return in.app.usage("usage: mikyu create --type log|task [flags] <queue>")
	}
	if typeName == "" {
		return in.app.usage("create: --type log|task is required")
	}
	kind, err := command.KindFromName(typeName)
	if err != nil {
		return in.app.usage("create: %v", err)
	}
	if err := command.Create(in.ctx, in.client, kind, rest[0], command.SpecOption(patch)); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "created queue %q (%s)\n", rest[0], kind)
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

	if patch.Retention.MaxAge != nil || patch.Retention.MaxBytes != nil {
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
	fs := command.NewFlagSet("describe")
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
	derived, _ := json.Marshal(d.Derived)
	effective, _ := json.Marshal(d.Effective)
	defaults, _ := json.Marshal(d.Defaults)
	compaction := "off"
	if d.Effective.Storage.Compaction != nil && *d.Effective.Storage.Compaction == spec.CompactionKeys {
		compaction = "on"
	}
	fmt.Fprintf(in.app.Stdout, "queue:      %s (%s)\n", rest[0], d.Kind)
	fmt.Fprintf(in.app.Stdout, "compaction: %s\n", compaction)
	fmt.Fprintf(in.app.Stdout, "stored:     %s\n", stored)
	fmt.Fprintf(in.app.Stdout, "derived:    %s\n", derived)
	fmt.Fprintf(in.app.Stdout, "effective:  %s\n", effective)
	fmt.Fprintf(in.app.Stdout, "defaults:   %s\n", defaults)
	return nil
}

func (in *invocation) handleDelete(args []string) error {
	var yes bool
	fs := command.NewFlagSet("delete")
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

	executed, err := command.Delete(in.ctx, in.client, name,
		in.confirmFn(fmt.Sprintf("delete queue %q? [y/N]: ", name), yes))
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

// ---- log verbs ----

func (in *invocation) handleAppend(args []string) error {
	var key string
	fs := command.NewFlagSet("append")
	fs.StringVar(&key, "key", "", "the subject key this record belongs to")

	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		return in.app.usage("append: %v", err)
	}
	if len(rest) != 2 {
		return in.app.usage("usage: mikyu append [--key <key>] <queue> <payload|@file|->")
	}
	offset, _, err := command.Append(in.ctx, in.client, rest[0], rest[1], []byte(key))
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
	return command.Tail(in.ctx, in.client, args[0], start, func(msg mikyu.Record) error {
		prefix := fmt.Sprintf("offset=%d id=%s ts=%s payload=", msg.Offset, msg.ID, msg.Timestamp.Format(time.RFC3339Nano))
		termWidth := command.GetTerminalWidth()
		avail := -1
		if termWidth > 0 {
			avail = termWidth - len(prefix)
		}
		fmt.Fprintf(in.app.Stdout, "%s%s\n", prefix, command.FormatSingleLinePayload(msg.Payload, avail))
		return nil
	})
}

func (in *invocation) handleTruncate(args []string) error {
	var yes bool
	fs := command.NewFlagSet("truncate")
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

// ---- admin verbs ----

func (in *invocation) handleLogin(args []string) error {
	fs := command.NewFlagSet("login")
	var target string
	fs.StringVar(&target, "target", in.target, "target to authenticate against")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil || len(rest) != 0 {
		return in.app.usage("usage: mikyu login --target <uri>")
	}
	if err := command.Login(in.ctx, target); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "stored token for %s\n", target)
	return nil
}

func (in *invocation) handleLogout(args []string) error {
	fs := command.NewFlagSet("logout")
	var target string
	fs.StringVar(&target, "target", in.target, "target to forget")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil || len(rest) != 0 {
		return in.app.usage("usage: mikyu logout --target <uri>")
	}
	if err := command.Logout(target); err != nil {
		return err
	}
	fmt.Fprintf(in.app.Stdout, "forgot token for %s\n", target)
	return nil
}

