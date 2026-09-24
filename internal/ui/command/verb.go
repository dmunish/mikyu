package command

// VerbKind groups verbs in help output and dispatch.
type VerbKind int

const (
	KindSession VerbKind = iota
	KindLog
	KindTask
	KindAdmin
)

// String renders the kind name used as a section header.
func (k VerbKind) String() string {
	switch k {
	case KindSession:
		return "session"
	case KindLog:
		return "log"
	case KindTask:
		return "task"
	case KindAdmin:
		return "admin"
	default:
		return "unknown"
	}
}

// Mode is a bitmask of the surfaces a verb is exposed on.
type Mode uint8

const (
	ModeCLI Mode = 1 << iota
	ModeREPL
)

// FlagGroup selects which spec slice a verb renders in its FLAGS section.
type FlagGroup uint8

const (
	FlagsNone FlagGroup = iota
	FlagsQueueCreate
	FlagsQueuePatch
	FlagsEnqueue
	FlagsNack
)

// FlagDoc describes a presentation flag that is not a spec parameter.
type FlagDoc struct {
	Name    string
	Kind    string
	Doc     string
	Default string
}

// Example is one invocation shown on a verb's detail page.
type Example struct {
	Invocation string
}

// Verb is one row of the command registry: the single source of truth for dispatch, help, and the naming-law tests.
type Verb struct {
	Name        string
	Args        string
	Summary     string
	Long        string
	Usage       []string
	Examples    []Example
	Kind        VerbKind
	Modes       Mode
	Flags       FlagGroup
	ExtraFlags  []FlagDoc
	Aliases     []string
	Intercepted bool
}

// Verbs is the canonical verb table. Slice order is render order.
var Verbs = []Verb{
	// session
	{
		Name: "help", Args: "", Kind: KindSession,
		Modes: ModeCLI | ModeREPL, Intercepted: true,
		Summary: "Show the command index",
		Long: "Prints every command grouped by kind. Run `<verb> --help` for a " +
			"command's usage, examples, and flags.",
		Usage: []string{"help"},
	},
	{
		Name: "use", Args: "<queue>", Kind: KindSession, Modes: ModeREPL,
		Summary: "Set the active queue for this session",
		Long: "Every command that takes a queue name falls back to the active queue " +
			"when the name is omitted. `use -` clears the selection.",
		Usage: []string{"use <queue>", "use -"},
		Examples: []Example{
			{Invocation: "use events"},
			{Invocation: "use -"},
		},
	},
	{
		Name: "exit", Args: "", Kind: KindSession, Modes: ModeREPL,
		Aliases: []string{"quit"},
		Summary: "Close the interactive shell",
		Long:    "Alias: quit.",
		Usage:   []string{"exit"},
	},

	// log
	{
		Name: "list", Args: "", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "List every queue",
		Long:    "Prints one `<name> <kind>` line per queue, sorted by name.",
		Usage:   []string{"list"},
	},
	{
		Name: "create", Args: "<queue>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Create a log or task queue",
		Long: "The queue's kind is structural: it lives under `log/` or `task/` in the " +
			"data directory, and a name may exist under both kinds without collision. " +
			"Task queues reject `storage.compaction`.",
		Usage: []string{
			"create --type log  [flags] <queue>",
			"create --type task [flags] <queue>",
		},
		Examples: []Example{
			{Invocation: "create --type log events"},
			{Invocation: "create --type task jobs --lease 60s"},
		},
		ExtraFlags: []FlagDoc{
			{Name: "type", Kind: "string", Doc: "Queue kind: log or task"},
		},
		Flags: FlagsQueueCreate,
	},
	{
		Name: "update", Args: "<queue>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Change a mutable queue parameter",
		Long: "Only mutable parameters may change after create. Touching a create-only " +
			"parameter (durability, encryption, compaction, max_segment, max_payload) " +
			"returns ErrImmutableField. Shrinking retention prompts before applying.",
		Usage: []string{"update [flags] <queue>"},
		Examples: []Example{
			{Invocation: "update events --max-retention-age 24h"},
			{Invocation: "update events --max-retention-age 1h --yes"},
		},
		ExtraFlags: []FlagDoc{
			{Name: "yes", Kind: "bool", Doc: "Skip the retention-shrink confirmation"},
		},
		Flags: FlagsQueuePatch,
	},
	{
		Name: "describe", Args: "<queue>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Show a queue's stored, effective, and default specs",
		Long: "Stored is what queues.json holds, effective is stored merged over session " +
			"defaults and built-ins, and defaults is the session layer. Live storage " +
			"stats are observed through `metrics`, not here.",
		Usage: []string{"describe <queue> [--json]"},
		ExtraFlags: []FlagDoc{
			{Name: "json", Kind: "bool", Doc: "Emit the machine-readable description"},
		},
	},
	{
		Name: "delete", Args: "<queue>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Delete a queue and its directory",
		Long:    "Removes the queue's directory and its queues.json entry. Irreversible.",
		Usage:   []string{"delete <queue> [--yes]"},
		ExtraFlags: []FlagDoc{
			{Name: "yes", Kind: "bool", Doc: "Skip the confirmation"},
		},
	},
	{
		Name: "append", Args: "<queue> <payload>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Append one record to a log queue",
		Long: "Durable before ack by default; set `--durability async` for a small crash " +
			"window at lower latency. The returned offset is crash-safe. The payload " +
			"may be a literal string, `@path` to read a file, or `-` to read stdin.",
		Usage: []string{"append [--key <key>] <queue> <payload|@file|->"},
		Examples: []Example{
			{Invocation: "append events \"hello world\""},
			{Invocation: "append events @event.json"},
		},
		ExtraFlags: []FlagDoc{
			{Name: "key", Kind: "string", Doc: "Subject key this record belongs to"},
		},
	},
	{
		Name: "read", Args: "<queue> <offset>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Read the record at one offset",
		Long: "Returns the record's id, timestamp, group, and payload. A compacted " +
			"offset returns ErrRecordCompacted.",
		Usage: []string{"read <queue> <offset>"},
	},
	{
		Name: "tail", Args: "<queue>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Follow a log queue from an offset",
		Long: "Streams records continuously, blocking up to 30 s per poll. Ctrl+C " +
			"stops the stream cleanly.",
		Usage: []string{"tail <queue> [offset]"},
		Examples: []Example{
			{Invocation: "tail events"},
			{Invocation: "tail events 42"},
		},
	},
	{
		Name: "truncate", Args: "<queue>", Kind: KindLog, Modes: ModeCLI | ModeREPL,
		Summary: "Drop closed log segments below an offset",
		Long: "Only closed segments are dropped; the active segment is never " +
			"truncated. Zero reclaims as much as retention and any consumer-group " +
			"floors permit. Irreversible.",
		Usage: []string{"truncate <queue> [offset] [--yes]"},
		ExtraFlags: []FlagDoc{
			{Name: "yes", Kind: "bool", Doc: "Skip the confirmation"},
		},
	},

	// task
	{
		Name: "enqueue", Args: "<queue> <payload>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Enqueue a task",
		Long: "The payload may be a literal string, `@path` to read a file, or `-` to " +
			"read stdin. Per-call flags override the queue's defaults.",
		Usage: []string{"enqueue [flags] <queue> <payload|@file|->"},
		Examples: []Example{
			{Invocation: "enqueue jobs \"send email\""},
			{Invocation: "enqueue jobs @job.json --scheduled-at 2030-01-01T00:00:00Z"},
		},
		ExtraFlags: []FlagDoc{
			{Name: "key", Kind: "string", Doc: "Subject key this task belongs to"},
		},
		Flags: FlagsEnqueue,
	},
	{
		Name: "dequeue", Args: "<queue>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Lease the next due task",
		Long: "Blocks up to --timeout for a due task. `--timeout -1` waits " +
			"indefinitely. `--json` emits a task record that feeds straight into ack, " +
			"nack, extend, and dlq-ack.",
		Usage: []string{"dequeue <queue> [--timeout 30s] [--json]"},
		Examples: []Example{
			{Invocation: "dequeue jobs"},
			{Invocation: "dequeue jobs --json > task.json"},
		},
		ExtraFlags: []FlagDoc{
			{Name: "timeout", Kind: "duration", Doc: "How long to wait for a due task", Default: "30s"},
			{Name: "json", Kind: "bool", Doc: "Emit the task record as JSON"},
		},
	},
	{
		Name: "ack", Args: "<queue> <task>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Acknowledge a task",
		Long: "Passing a full task record (from `dequeue --json`) enforces the strict " +
			"lease check: a task whose offset, attempt count, or delivery ID no longer " +
			"matches is rejected with ErrTaskStale. A bare ID skips the strict check.",
		Usage: []string{"ack <queue> <task|json|id>"},
	},
	{
		Name: "nack", Args: "<queue> <task>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Return a task for redelivery",
		Long: "Without --retry-after, the server computes exponential backoff " +
			"`1s × 2^(attempts-1)`, capped at 5 min. On the final attempt the task is " +
			"dead-lettered.",
		Usage: []string{"nack <queue> <task|json|id> [--retry-after 5s]"},
		Flags: FlagsNack,
	},
	{
		Name: "extend", Args: "<queue> <task>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Extend a task lease forward",
		Long: "Only extends lease forward, never shortens it, and never " +
			"increments the attempt count.",
		Usage: []string{"extend <queue> <task|json|id> <timeout>"},
		Examples: []Example{
			{Invocation: "extend jobs task.json 5m"},
		},
	},
	{
		Name: "dlq", Args: "<queue>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "List dead-lettered tasks",
		Long:    "Dead-letter entries are durable per queue and bounded by `task.max_dlq`.",
		Usage:   []string{"dlq <queue> [--json]"},
		ExtraFlags: []FlagDoc{
			{Name: "json", Kind: "bool", Doc: "Emit the dead-letter records as JSON"},
		},
	},
	{
		Name: "dlq-ack", Args: "<queue> <task>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Remove a dead-lettered task",
		Long: "Permanently drops the task from the queue's dead-letter list. Passing " +
			"a full record enforces the stale-check; a bare ID does not.",
		Usage: []string{"dlq-ack <queue> <task|json|id>"},
	},
	{
		Name: "archive", Args: "<task>", Kind: KindTask, Modes: ModeCLI | ModeREPL,
		Summary: "Move a task to its archive log",
		Long: "Copies the task's payload, key, attempts, and stalled count into " +
			"`<queue>-archive`, a log queue created lazily on first use. The source " +
			"task is removed from its in-flight or dead-letter state.",
		Usage: []string{"archive <task|json|id> --queue <name>"},
		ExtraFlags: []FlagDoc{
			{Name: "queue", Kind: "string", Doc: "Queue the task came from"},
		},
	},

	// admin
	{
		Name: "metrics", Args: "", Kind: KindAdmin, Modes: ModeCLI | ModeREPL,
		Summary: "Show a live telemetry snapshot",
		Long: "`--watch` streams snapshots until interrupted. The embedded client and " +
			"the daemon expose the same metrics.Snapshot.",
		Usage: []string{"metrics [--json] [--watch] [--queue name]"},
		ExtraFlags: []FlagDoc{
			{Name: "json", Kind: "bool", Doc: "Emit the snapshot as JSON"},
			{Name: "watch", Kind: "bool", Doc: "Stream snapshots until interrupted"},
			{Name: "queue", Kind: "string", Doc: "Restrict output to one queue"},
		},
	},
	{
		Name: "shutdown", Args: "", Kind: KindAdmin, Modes: ModeCLI | ModeREPL,
		Summary: "Stop the running daemon",
		Long: "Sends an OpShutdown frame; the daemon drains the server and exits. " +
			"Fails when the current process is embedded (`--dir`) and has no daemon to stop.",
		Usage: []string{"shutdown [--yes]"},
		ExtraFlags: []FlagDoc{
			{Name: "yes", Kind: "bool", Doc: "Skip the confirmation"},
		},
	},
	{
		Name: "login", Args: "", Kind: KindAdmin, Modes: ModeCLI | ModeREPL,
		Summary: "Store a token for a target",
		Long: "Prompts for a token on stdin and verifies it against the target before " +
			"saving. Stored at the user config directory, mode 0600.",
		Usage: []string{"login [--target <uri>]"},
		ExtraFlags: []FlagDoc{
			{Name: "target", Kind: "string", Doc: "Target to authenticate against"},
		},
	},
	{
		Name: "logout", Args: "", Kind: KindAdmin, Modes: ModeCLI | ModeREPL,
		Summary: "Forget a stored token",
		Long:    "Missing entries are a no-op.",
		Usage:   []string{"logout [--target <uri>]"},
		ExtraFlags: []FlagDoc{
			{Name: "target", Kind: "string", Doc: "Target to forget"},
		},
	},
}

// LookupVerb finds a verb by name or alias.
func LookupVerb(name string) (Verb, bool) {
	for _, v := range Verbs {
		if v.Name == name {
			return v, true
		}
		for _, a := range v.Aliases {
			if a == name {
				return v, true
			}
		}
	}
	return Verb{}, false
}

// VerbNames returns every verb name in table order.
func VerbNames() []string {
	names := make([]string, 0, len(Verbs))
	for _, v := range Verbs {
		names = append(names, v.Name)
	}
	return names
}

// VerbsOfKind returns the verbs of one kind in table order.
func VerbsOfKind(kind VerbKind) []Verb {
	var out []Verb
	for _, v := range Verbs {
		if v.Kind == kind {
			out = append(out, v)
		}
	}
	return out
}

// verbsInMode returns the verbs of one kind visible in mode, in table order.
func verbsInMode(kind VerbKind, mode Mode) []Verb {
	var out []Verb
	for _, v := range Verbs {
		if v.Kind != kind {
			continue
		}
		if v.Modes&mode == 0 {
			continue
		}
		out = append(out, v)
	}
	return out
}

// HasVerb reports whether name is a known verb or alias.
func HasVerb(name string) bool {
	_, ok := LookupVerb(name)
	return ok
}
