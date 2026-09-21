package command

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// Global carries the front-end session flags plus the help request.
type Global struct {
	Session spec.Session
	Help    bool
}

// SessionFlags returns the global flag set every verb accepts: one flag
// per session parameter, plus --dir as the short spelling of data_dir.
func SessionFlags(sess *spec.Session, help *bool) *flag.FlagSet {
	fs := NewFlagSet("mikyu")
	for _, p := range spec.ParamsInScope(spec.LayerSession, spec.ScopeClient, spec.ScopeBoth) {
		fs.Var(&sessionValue{param: p, session: sess}, p.FlagName, p.Doc)
	}
	if p, ok := spec.Lookup("data_dir"); ok {
		fs.Var(&sessionValue{param: p, session: sess}, "dir", "data directory (embedded mode)")
	}
	if help != nil {
		fs.BoolVar(help, "h", false, "show help")
		fs.BoolVar(help, "help", false, "show help")
	}
	return fs
}

// ParseGlobal consumes the session flags that precede a verb and returns
// the remaining arguments.
func ParseGlobal(target string, args []string) (Global, []string, error) {
	g := Global{Session: spec.BuiltinSession()}
	g.Session.Target = target

	fs := SessionFlags(&g.Session, &g.Help)
	leading := leadingFlagTokens(fs, args)
	if err := fs.Parse(leading); err != nil {
		return Global{}, nil, err
	}
	return g, args[len(leading):], nil
}

// leadingFlagTokens returns the flag tokens that precede the first
// positional argument, so a verb's own flags are left for the verb.
// Both "--flag value" and "--flag=value" are accepted.
func leadingFlagTokens(fs *flag.FlagSet, args []string) []string {
	var flags []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			break
		}
		token := strings.TrimLeft(arg, "-")
		inline := strings.Contains(token, "=")
		if eq := strings.IndexByte(token, '='); eq >= 0 {
			token = token[:eq]
		}
		f := fs.Lookup(token)
		if f == nil {
			break
		}
		flags = append(flags, arg)
		if inline {
			continue
		}
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			continue
		}
		if i+1 >= len(args) {
			break
		}
		i++
		flags = append(flags, args[i])
	}
	return flags
}

// ParseAnywhere parses fs over args, accepting flags before or after
// positionals and both "--flag value" and "--flag=value" forms.
func ParseAnywhere(fs *flag.FlagSet, args []string) ([]string, error) {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		inlineValue := ""
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			name, inlineValue = name[:eq], name[eq+1:]
		}
		f := fs.Lookup(name)
		if f == nil {
			key := strings.ReplaceAll(name, "-", "_")
			if spec.IsCreateOnly(key) {
				return nil, fmt.Errorf("%s is create-only and cannot be changed after create", key)
			}
			return nil, fmt.Errorf("unknown flag: %s", arg)
		}
		flags = append(flags, arg)
		if inlineValue != "" {
			continue
		}
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("flag needs an argument: %s", arg)
		}
		i++
		flags = append(flags, args[i])
	}
	if err := fs.Parse(flags); err != nil {
		return nil, err
	}
	return positional, nil
}

// HasHelpFlag reports whether args contain a per-verb help request before
// any "--" terminator.
func HasHelpFlag(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "-h" || a == "--help" {
			return true
		}
	}
	return false
}

// NewFlagSet returns a silent flag set for presentation flags that are
// not spec parameters (--json, -y, --timeout).
func NewFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// QueueFlagSet returns a flag set over every queue-scoped parameter,
// writing into s.
func QueueFlagSet(name string, s *spec.Spec) *flag.FlagSet {
	return queueFlagSet(name, s, spec.Params(spec.LayerSpec))
}

// QueuePatchFlagSet returns a flag set over the mutable queue parameters
// only, which is what an update patch may contain.
func QueuePatchFlagSet(name string, s *spec.Spec) *flag.FlagSet {
	return queueFlagSet(name, s, spec.Updatable())
}

func queueFlagSet(name string, s *spec.Spec, params []spec.Param) *flag.FlagSet {
	fs := NewFlagSet(name)
	for _, p := range params {
		value := &paramValue{param: p, spec: s}
		if p.Kind == spec.KindBool {
			fs.Var(&boolParamValue{*value}, p.FlagName, p.Doc)
			continue
		}
		fs.Var(value, p.FlagName, p.Doc)
	}
	return fs
}

// EnqueueFlagSet returns the per-call flag set for enqueue, writing into o.
func EnqueueFlagSet(name string, o *EnqueueOptions) *flag.FlagSet {
	fs := NewFlagSet(name)
	fs.Var(&valueFunc{
		kind: spec.KindString,
		str:  func() string { return string(o.Key) },
		set:  func(v spec.Value) { o.Key = []byte(v.Text()) },
	}, "key", "the subject key this task record belongs to")

	for _, p := range spec.Params(spec.LayerCall) {
		switch p.Key {
		case "visibility":
			fs.Var(&valueFunc{
				kind: p.Kind,
				str:  func() string { return o.Visibility.String() },
				set:  func(v spec.Value) { o.Visibility = v.Duration() },
			}, p.FlagName, p.Doc)
		case "max_attempts":
			fs.Var(&valueFunc{
				kind: p.Kind,
				str:  func() string { return fmt.Sprint(o.MaxAttempts) },
				set:  func(v spec.Value) { o.MaxAttempts = v.Uint32() },
			}, p.FlagName, p.Doc)
		case "scheduled_at":
			fs.Var(&valueFunc{
				kind: p.Kind,
				str:  func() string { return o.ScheduledAt.Format(time.RFC3339) },
				set:  func(v spec.Value) { o.ScheduledAt = v.Time() },
			}, p.FlagName, p.Doc)
		}
	}
	return fs
}

// NackFlagSet returns the per-call flag set for nack, writing into
// retryAfter and setting explicit when --retry-after is given.
func NackFlagSet(name string, retryAfter *time.Duration, explicit *bool) *flag.FlagSet {
	fs := NewFlagSet(name)
	for _, p := range spec.Params(spec.LayerCall) {
		if p.Key != "retry_after" {
			continue
		}
		fs.Var(&valueFunc{
			kind: p.Kind,
			str:  func() string { return retryAfter.String() },
			set: func(v spec.Value) {
				*retryAfter = v.Duration()
				*explicit = true
			},
		}, p.FlagName, p.Doc)
	}
	return fs
}

// sessionValue binds one session-scoped parameter to a flag.Value.
type sessionValue struct {
	param   spec.Param
	session *spec.Session
}

func (v *sessionValue) String() string {
	if v == nil || v.session == nil {
		return ""
	}
	switch v.param.Key {
	case "target":
		return v.session.Target
	case "data_dir":
		return v.session.DataDir
	case "tls_enabled":
		return strconv.FormatBool(v.session.TLSEnabled)
	case "tls_cert_file":
		return v.session.TLSCertFile
	case "tls_key_file":
		return v.session.TLSKeyFile
	case "tls_client_ca_file":
		return v.session.TLSClientCaFile
	case "max_conns":
		return strconv.Itoa(v.session.MaxConns)
	case "metrics_enabled":
		return strconv.FormatBool(v.session.MetricsEnabled)
	case "metrics_interval":
		return v.session.MetricsInterval.Duration().String()
	case "metrics_history":
		return strconv.Itoa(v.session.MetricsHistory)
	case "metrics_listen":
		return v.session.MetricsListen
	case "metrics_per_queue_prometheus":
		return strconv.FormatBool(v.session.MetricsPerQueuePrometheus)
	case "metrics_histograms":
		return strconv.FormatBool(v.session.MetricsHistograms)
	case "auth_method":
		return v.session.AuthMethod
	case "auth_token_file":
		return v.session.AuthTokenFile
	case "auth_token":
		return "<redacted>"
	}
	return ""
}

func (v *sessionValue) Set(text string) error {
	val, err := v.param.Parse(text)
	if err != nil {
		return err
	}
	v.param.ApplySession(v.session, val)
	return nil
}

// paramValue binds one queue-scoped parameter to a flag.Value.
type paramValue struct {
	param spec.Param
	spec  *spec.Spec
}

func (v *paramValue) String() string {
	if v == nil || v.spec == nil {
		return ""
	}
	if val, ok := v.spec.Field(v.param); ok {
		return val.String()
	}
	return ""
}

func (v *paramValue) Set(text string) error {
	val, err := v.param.Parse(text)
	if err != nil {
		return err
	}
	v.param.Apply(v.spec, val)
	return nil
}

// boolParamValue lets a bool parameter stand alone as a bare flag.
type boolParamValue struct{ paramValue }

func (v *boolParamValue) IsBoolFlag() bool { return true }

// valueFunc binds a spec kind to a destination setter as a flag.Value.
type valueFunc struct {
	kind spec.Kind
	str  func() string
	set  func(spec.Value)
}

func (v *valueFunc) String() string {
	if v == nil || v.str == nil {
		return ""
	}
	return v.str()
}

func (v *valueFunc) Set(text string) error {
	val, err := spec.ParseValue(v.kind, text)
	if err != nil {
		return err
	}
	v.set(val)
	return nil
}
