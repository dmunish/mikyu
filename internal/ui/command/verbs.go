package command

// VerbKind groups verbs in help output and dispatch.
type VerbKind int

const (
	KindLog VerbKind = iota
	KindTask
	KindAdmin
)

func (k VerbKind) String() string {
	switch k {
	case KindLog:
		return "log"
	case KindTask:
		return "task"
	default:
		return "admin"
	}
}

// Verb is one verb of the mikyu CLI and REPL.
type Verb struct {
	Name    string
	Args    string
	Summary string
	Kind    VerbKind
}

// Verbs is the canonical verb table.
var Verbs = []Verb{
	{Name: "list", Args: "", Summary: "List queues", Kind: KindLog},
	{Name: "create", Args: "[flags] <queue>", Summary: "Create a queue", Kind: KindLog},
	{Name: "update", Args: "[flags] <queue>", Summary: "Update a queue's mutable spec", Kind: KindLog},
	{Name: "describe", Args: "<queue> [--json]", Summary: "Show stored and effective spec", Kind: KindLog},
	{Name: "delete", Args: "<queue> [-y]", Summary: "Delete a queue and its data", Kind: KindLog},
	{Name: "append", Args: "<queue> <payload|@file|->", Summary: "Append a record to a queue", Kind: KindLog},
	{Name: "read", Args: "<queue> <offset>", Summary: "Read the record at an offset", Kind: KindLog},
	{Name: "tail", Args: "<queue> [offset]", Summary: "Stream records continuously", Kind: KindLog},
	{Name: "truncate", Args: "<queue> [offset] [-y]", Summary: "Drop log segments before an offset", Kind: KindLog},

	{Name: "enqueue", Args: "[flags] <queue> <payload|@file|->", Summary: "Enqueue a task", Kind: KindTask},
	{Name: "dequeue", Args: "<queue> [--timeout 30s] [--json]", Summary: "Lease the next due task", Kind: KindTask},
	{Name: "ack", Args: "<queue> <task|json|id>", Summary: "Acknowledge a task", Kind: KindTask},
	{Name: "nack", Args: "<queue> <task|json|id> [--retry-after 5s]", Summary: "Return a task for redelivery", Kind: KindTask},
	{Name: "extend", Args: "<queue> <task|json|id> <timeout>", Summary: "Extend a task lease", Kind: KindTask},
	{Name: "dlq", Args: "<queue> [--json]", Summary: "List dead-lettered tasks", Kind: KindTask},
	{Name: "dlq-ack", Args: "<queue> <task|json|id>", Summary: "Remove a dead-lettered task", Kind: KindTask},

	{Name: "metrics", Args: "[--json] [--watch] [--queue name]", Summary: "Show live node and queue metrics", Kind: KindAdmin},
	{Name: "shutdown", Args: "[-y]", Summary: "Stop the daemon", Kind: KindAdmin},
	{Name: "login", Args: "--target <uri>", Summary: "Store a token for a target", Kind: KindAdmin},
	{Name: "logout", Args: "--target <uri>", Summary: "Forget a stored token", Kind: KindAdmin},
}

// LookupVerb finds a verb by name.
func LookupVerb(name string) (Verb, bool) {
	for _, v := range Verbs {
		if v.Name == name {
			return v, true
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
