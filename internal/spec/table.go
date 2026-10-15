package spec

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Kind is a parameter's value type.
type Kind int

const (
	KindUnknown Kind = iota
	KindInt
	KindUint32
	KindDuration
	KindBytes
	KindBool
	KindString
	KindTime
	KindEnum
	KindFloat
)

func (k Kind) String() string {
	switch k {
	case KindInt:
		return "int"
	case KindUint32:
		return "uint32"
	case KindDuration:
		return "duration"
	case KindBytes:
		return "bytes"
	case KindBool:
		return "bool"
	case KindString:
		return "string"
	case KindTime:
		return "timestamp"
	case KindEnum:
		return "enum"
	case KindFloat:
		return "float"
	default:
		return "unknown"
	}
}

// Layer is where a parameter may be set.
type Layer int

const (
	LayerUnspecified Layer = iota
	LayerCall
	LayerSpec
	LayerSession
)

func (l Layer) String() string {
	switch l {
	case LayerCall:
		return "call"
	case LayerSpec:
		return "spec"
	case LayerSession:
		return "session"
	default:
		return "unknown"
	}
}

// Mutability says whether an update patch may change a parameter after create.
type Mutability int

const (
	MutUnspecified Mutability = iota
	MutMutable
	MutCreateOnly
	MutNotApplicable
)

// String renders the mutability for schema and wire output.
func (m Mutability) String() string {
	switch m {
	case MutMutable:
		return "mutable"
	case MutCreateOnly:
		return "create_only"
	case MutNotApplicable:
		return "not_applicable"
	default:
		return "unspecified"
	}
}

// MarshalJSON renders the kind as its string form so the wire shape is language-neutral.
func (k Kind) MarshalJSON() ([]byte, error) { return json.Marshal(k.String()) }

// UnmarshalJSON decodes the string form produced by MarshalJSON.
func (k *Kind) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	for i := KindInt; i <= KindFloat; i++ {
		if i.String() == s {
			*k = i
			return nil
		}
	}
	return fmt.Errorf("%w: unknown kind %q", ErrInvalidConfig, s)
}

// MarshalJSON renders the layer as its string form so the wire shape is language-neutral.
func (l Layer) MarshalJSON() ([]byte, error) { return json.Marshal(l.String()) }

// UnmarshalJSON decodes the string form produced by MarshalJSON.
func (l *Layer) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	for i := LayerCall; i <= LayerSession; i++ {
		if i.String() == s {
			*l = i
			return nil
		}
	}
	return fmt.Errorf("%w: unknown layer %q", ErrInvalidConfig, s)
}

// MarshalJSON renders the mutability as its string form so the wire shape is language-neutral.
func (m Mutability) MarshalJSON() ([]byte, error) { return json.Marshal(m.String()) }

// UnmarshalJSON decodes the string form produced by MarshalJSON.
func (m *Mutability) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	for i := MutMutable; i <= MutNotApplicable; i++ {
		if i.String() == s {
			*m = i
			return nil
		}
	}
	return fmt.Errorf("%w: unknown mutability %q", ErrInvalidConfig, s)
}

// Scope says which side of a session boundary a parameter configures.
type Scope int

const (
	ScopeUnspecified Scope = iota
	ScopeClient
	ScopeServer
	ScopeBoth
)

// String renders the scope for schema and wire output.
func (s Scope) String() string {
	switch s {
	case ScopeClient:
		return "client"
	case ScopeServer:
		return "server"
	case ScopeBoth:
		return "both"
	default:
		return "unspecified"
	}
}

// MarshalJSON renders the scope as its string form so the wire shape is language-neutral.
func (s Scope) MarshalJSON() ([]byte, error) { return json.Marshal(s.String()) }

// UnmarshalJSON decodes the string form produced by MarshalJSON.
func (s *Scope) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	for i := ScopeUnspecified; i <= ScopeBoth; i++ {
		if i.String() == text {
			*s = i
			return nil
		}
	}
	return fmt.Errorf("%w: unknown scope %q", ErrInvalidConfig, text)
}

// Param is one entry of the parameter table: the single source of truth for how a configuration value is named and set in every layer that can reach it.
type Param struct {
	Key        string     `json:"key"`
	Path       string     `json:"path"`
	Kind       Kind       `json:"kind"`
	Allowed    []string   `json:"allowed,omitempty"`
	Layer      Layer      `json:"layer"`
	Scope      Scope      `json:"scope"`
	Mutability Mutability `json:"mutability"`
	EnvName    string     `json:"env_name"`
	FlagName   string     `json:"flag_name"`
	OptionName string     `json:"option_name"`
	Doc        string     `json:"doc"`

	Apply        func(*Spec, Value)    `json:"-"`
	ApplySession func(*Session, Value) `json:"-"`
}

func (p Param) String() string { return p.Key }

// Parse converts flag or env text into a Value of this parameter's kind, checking enum values against the allowed set.
func (p Param) Parse(text string) (Value, error) {
	v, err := ParseValue(p.Kind, text)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %s: %w", ErrInvalidConfig, p.Key, err)
	}
	if p.Kind == KindEnum && len(p.Allowed) > 0 && !slices.Contains(p.Allowed, v.Enum()) {
		return Value{}, fmt.Errorf("%w: %s must be one of %s, got %q", ErrInvalidConfig, p.Key, strings.Join(p.Allowed, "|"), v.Enum())
	}
	return v, nil
}

// specParam declares a queue-scoped parameter grouped under the group named in path.
func specParam(path, key string, kind Kind, mut Mutability, allowed []string, doc string) Param {
	return Param{
		Key:        key,
		Path:       path,
		Kind:       kind,
		Allowed:    allowed,
		Layer:      LayerSpec,
		Mutability: mut,
		EnvName:    envName(key),
		FlagName:   flagName(key),
		OptionName: optionName(key),
		Doc:        doc,
	}
}

// sessionParam declares a process-only parameter living in the server block at path.
func sessionParam(path, key string, kind Kind, scope Scope, doc string) Param {
	return Param{
		Key:        key,
		Path:       path,
		Kind:       kind,
		Layer:      LayerSession,
		Scope:      scope,
		Mutability: MutNotApplicable,
		EnvName:    envName(key),
		FlagName:   flagName(key),
		OptionName: optionName(key),
		Doc:        doc,
	}
}

// callParam declares an ephemeral per-call parameter.
func callParam(key string, kind Kind, doc string) Param {
	return Param{
		Key:        key,
		Path:       key,
		Kind:       kind,
		Layer:      LayerCall,
		Mutability: MutNotApplicable,
		FlagName:   flagName(key),
		OptionName: optionName(key),
		Doc:        doc,
	}
}

// specSetters and sessionSetters bind each canonical key to its field. The
// compiler checks the kind of every assignment.
var specSetters = map[string]func(*Spec, Value){
	"durability":          func(s *Spec, v Value) { s.Storage.Durability = new(Durability(v.Enum())) },
	"encryption":          func(s *Spec, v Value) { s.Storage.Encryption = new(Encryption(v.Enum())) },
	"compaction":          func(s *Spec, v Value) { s.Storage.Compaction = new(Compaction(v.Enum())) },
	"max_segment":         func(s *Spec, v Value) { s.Storage.MaxSegment = new(v.Bytes()) },
	"max_segment_age":     func(s *Spec, v Value) { s.Storage.MaxSegmentAge = new(Duration(v.Duration())) },
	"max_payload":         func(s *Spec, v Value) { s.Storage.MaxPayload = new(v.Bytes()) },
	"max_cache":           func(s *Spec, v Value) { s.Storage.MaxCache = new(v.Bytes()) },
	"max_flush_bytes":     func(s *Spec, v Value) { s.Flush.MaxBytes = new(v.Bytes()) },
	"max_flush_wait":      func(s *Spec, v Value) { s.Flush.MaxWait = new(Duration(v.Duration())) },
	"max_retention_age":   func(s *Spec, v Value) { s.Retention.MaxAge = new(Duration(v.Duration())) },
	"max_retention_bytes": func(s *Spec, v Value) { s.Retention.MaxBytes = new(v.Bytes()) },
	"lease":               func(s *Spec, v Value) { s.Task.Lease = new(Duration(v.Duration())) },
	"max_attempts":        func(s *Spec, v Value) { s.Task.MaxAttempts = new(v.Uint32()) },
	"max_in_flight":       func(s *Spec, v Value) { s.Task.MaxInFlight = new(v.Uint32()) },
	"max_dead_letters":    func(s *Spec, v Value) { s.Task.MaxDeadLetters = new(v.Uint32()) },
}

var sessionSetters = map[string]func(*Session, Value){
	"target":                       func(s *Session, v Value) { s.Target = v.Text() },
	"data_dir":                     func(s *Session, v Value) { s.DataDir = v.Text() },
	"max_connections":              func(s *Session, v Value) { s.MaxConnections = v.Int() },
	"tls_enabled":                  func(s *Session, v Value) { s.TLS.Enabled = v.Bool() },
	"tls_cert":                     func(s *Session, v Value) { s.TLS.Cert = v.Text() },
	"tls_key":                      func(s *Session, v Value) { s.TLS.Key = v.Text() },
	"tls_client_ca":                func(s *Session, v Value) { s.TLS.ClientCA = v.Text() },
	"auth_method":                  func(s *Session, v Value) { s.Auth.Method = v.Text() },
	"auth_token":                   func(s *Session, v Value) { s.Auth.Token = v.Text() },
	"auth_token_file":              func(s *Session, v Value) { s.Auth.TokenFile = v.Text() },
	"encryption_key_file":          func(s *Session, v Value) { s.Encryption.KeyFile = v.Text() },
	"metrics_enabled":              func(s *Session, v Value) { s.Metrics.Enabled = v.Bool() },
	"metrics_interval":             func(s *Session, v Value) { s.Metrics.Interval = Duration(v.Duration()) },
	"metrics_history":              func(s *Session, v Value) { s.Metrics.History = v.Int() },
	"metrics_listen":               func(s *Session, v Value) { s.Metrics.Listen = v.Text() },
	"metrics_per_queue_prometheus": func(s *Session, v Value) { s.Metrics.PerQueuePrometheus = v.Bool() },
	"metrics_histograms":           func(s *Session, v Value) { s.Metrics.Histograms = v.Bool() },
}

// getter reads one Spec field so presence lives beside the canonical keys.
func getter[T any](field func(*Spec) *T, value func(T) Value) func(*Spec) (Value, bool) {
	return func(s *Spec) (Value, bool) {
		p := field(s)
		if p == nil {
			return Value{}, false
		}
		return value(*p), true
	}
}

func enumGetter[T ~string](field func(*Spec) *T) func(*Spec) (Value, bool) {
	return getter(field, func(v T) Value { return NewEnum(string(v)) })
}

var specGetters = map[string]func(*Spec) (Value, bool){
	"durability":          enumGetter(func(s *Spec) *Durability { return s.Storage.Durability }),
	"encryption":          enumGetter(func(s *Spec) *Encryption { return s.Storage.Encryption }),
	"compaction":          enumGetter(func(s *Spec) *Compaction { return s.Storage.Compaction }),
	"max_segment":         getter(func(s *Spec) *int64 { return s.Storage.MaxSegment }, NewBytes),
	"max_segment_age":     getter(func(s *Spec) *Duration { return s.Storage.MaxSegmentAge }, NewSpecDuration),
	"max_payload":         getter(func(s *Spec) *int64 { return s.Storage.MaxPayload }, NewBytes),
	"max_cache":           getter(func(s *Spec) *int64 { return s.Storage.MaxCache }, NewBytes),
	"max_flush_bytes":     getter(func(s *Spec) *int64 { return s.Flush.MaxBytes }, NewBytes),
	"max_flush_wait":      getter(func(s *Spec) *Duration { return s.Flush.MaxWait }, NewSpecDuration),
	"max_retention_age":   getter(func(s *Spec) *Duration { return s.Retention.MaxAge }, NewSpecDuration),
	"max_retention_bytes": getter(func(s *Spec) *int64 { return s.Retention.MaxBytes }, NewBytes),
	"lease":               getter(func(s *Spec) *Duration { return s.Task.Lease }, NewSpecDuration),
	"max_attempts":        getter(func(s *Spec) *uint32 { return s.Task.MaxAttempts }, NewUint32),
	"max_in_flight":       getter(func(s *Spec) *uint32 { return s.Task.MaxInFlight }, NewUint32),
	"max_dead_letters":    getter(func(s *Spec) *uint32 { return s.Task.MaxDeadLetters }, NewUint32),
}

// Field returns the value p holds in s and whether s sets it.
func (s Spec) Field(p Param) (Value, bool) {
	get, ok := specGetters[p.Key]
	if !ok {
		panic("spec: " + p.Key + " is not a spec-layer parameter")
	}
	return get(&s)
}

// Default returns p's built-in value, or (Value{}, false) for non-spec layers.
func (p Param) Default() (Value, bool) {
	if p.Layer != LayerSpec {
		return Value{}, false
	}
	return Builtin().Field(p)
}

// Equal compares spec-layer fields with pointer presence significant.
func Equal(a, b Spec) bool {
	for _, p := range Params(LayerSpec) {
		av, aok := a.Field(p)
		bv, bok := b.Field(p)
		if aok != bok || (aok && av != bv) {
			return false
		}
	}
	return true
}

// Table is every tunable parameter in the system.
var Table = buildTable()

func buildTable() []Param {
	table := []Param{
		specParam("storage.durability", "durability", KindEnum, MutCreateOnly,
			[]string{string(DurabilitySync), string(DurabilityAsync)},
			"Ack policy: sync fsyncs before the offset returns, async flushes in the background"),
		specParam("storage.encryption", "encryption", KindEnum, MutCreateOnly,
			[]string{string(EncryptionNone), string(EncryptionOS), string(EncryptionAES)},
			"At-rest protection: none, os directories, or aes-sealed payloads (create-only)"),
		specParam("storage.compaction", "compaction", KindEnum, MutCreateOnly,
			[]string{string(CompactionNone), string(CompactionKeys)},
			"Drop superseded keys when a segment closes"),
		specParam("storage.max_segment", "max_segment", KindBytes, MutCreateOnly, nil,
			"Bytes per segment file"),
		specParam("storage.max_segment_age", "max_segment_age", KindDuration, MutMutable, nil,
			"Maximum age before a segment rolls"),
		specParam("storage.max_payload", "max_payload", KindBytes, MutCreateOnly, nil,
			"Largest payload an append accepts"),
		specParam("storage.max_cache", "max_cache", KindBytes, MutMutable, nil,
			"Tail cache budget in bytes"),
		specParam("flush.max_bytes", "max_flush_bytes", KindBytes, MutMutable, nil,
			"Flush once the pending WAL reaches this many bytes"),
		specParam("flush.max_wait", "max_flush_wait", KindDuration, MutMutable, nil,
			"Longest a batch waits before flushing"),
		specParam("retention.max_age", "max_retention_age", KindDuration, MutMutable, nil,
			"Drop closed segments older than this; 0 keeps forever"),
		specParam("retention.max_bytes", "max_retention_bytes", KindBytes, MutMutable, nil,
			"Keep at most this many bytes of history; 0 keeps forever"),
		specParam("task.lease", "lease", KindDuration, MutMutable, nil,
			"Lease length assigned to a dequeued task"),
		specParam("task.max_attempts", "max_attempts", KindUint32, MutMutable, nil,
			"Deliveries before a task is dead-lettered"),
		specParam("task.max_in_flight", "max_in_flight", KindUint32, MutMutable, nil,
			"Concurrent-lease upper bound"),
		specParam("task.max_dead_letters", "max_dead_letters", KindUint32, MutMutable, nil,
			"Dead-letter upper bound"),

		callParam("lease", KindDuration,
			"Lease length for this task only"),
		callParam("max_attempts", KindUint32,
			"Attempt budget for this task only"),
		callParam("scheduled_at", KindTime,
			"Delay the first delivery until this RFC3339 time"),
		callParam("retry_after", KindDuration,
			"Delay before this task becomes available again"),

		sessionParam("server.target", "target", KindString, ScopeBoth,
			"Daemon target (tcp://, unix://, pipe://)"),
		sessionParam("server.data_dir", "data_dir", KindString, ScopeBoth,
			"Data directory for embedded mode"),
		sessionParam("server.max_connections", "max_connections", KindInt, ScopeBoth,
			"Connection pool size"),
		sessionParam("server.tls.enabled", "tls_enabled", KindBool, ScopeBoth,
			"Wrap the wire in TLS 1.3"),
		sessionParam("server.tls.cert", "tls_cert", KindString, ScopeBoth,
			"TLS server certificate file"),
		sessionParam("server.tls.key", "tls_key", KindString, ScopeBoth,
			"TLS server key file"),
		sessionParam("server.tls.client_ca", "tls_client_ca", KindString, ScopeServer,
			"Client CA bundle for mTLS"),
		sessionParam("server.auth.method", "auth_method", KindString, ScopeServer,
			"One of none, none-insecure, token, mtls"),
		sessionParam("server.auth.token", "auth_token", KindString, ScopeBoth,
			"Token to present; the token file wins"),
		sessionParam("server.auth.token_file", "auth_token_file", KindString, ScopeBoth,
			"File to read the token from"),
		sessionParam("server.encryption.key_file", "encryption_key_file", KindString, ScopeServer,
			"File of hex AES-256 master keys, one per line; the last line wraps new data keys"),
		sessionParam("server.metrics.enabled", "metrics_enabled", KindBool, ScopeBoth,
			"Enable the collector"),
		sessionParam("server.metrics.interval", "metrics_interval", KindDuration, ScopeBoth,
			"Collector tick interval"),
		sessionParam("server.metrics.history", "metrics_history", KindInt, ScopeBoth,
			"Snapshots kept in the tick ring"),
		sessionParam("server.metrics.listen", "metrics_listen", KindString, ScopeServer,
			"HTTP bind address for /metrics and /api/*"),
		sessionParam("server.metrics.per_queue_prometheus", "metrics_per_queue_prometheus", KindBool, ScopeServer,
			"Emit per-queue labeled Prometheus series"),
		sessionParam("server.metrics.histograms", "metrics_histograms", KindBool, ScopeBoth,
			"Include histograms in snapshots"),
	}

	for i := range table {
		p := &table[i]
		switch p.Layer {
		case LayerSpec:
			p.Apply = specSetters[p.Key]
		case LayerSession:
			p.ApplySession = sessionSetters[p.Key]
		}
	}
	return table
}

// Lookup finds a parameter by canonical key.
func Lookup(key string) (Param, bool) {
	for _, p := range Table {
		if p.Key == key {
			return p, true
		}
	}
	return Param{}, false
}

// Params returns the table entries on layer, in table order.
func Params(layer Layer) []Param {
	var out []Param
	for _, p := range Table {
		if p.Layer == layer {
			out = append(out, p)
		}
	}
	return out
}

// ParamsInScope returns the table entries on layer whose scope is one of scopes.
func ParamsInScope(layer Layer, scopes ...Scope) []Param {
	var out []Param
	for _, p := range Table {
		if p.Layer == layer && slices.Contains(scopes, p.Scope) {
			out = append(out, p)
		}
	}
	return out
}

// Updatable returns the spec-layer parameters an update patch may set.
func Updatable() []Param {
	var out []Param
	for _, p := range Params(LayerSpec) {
		if p.Mutability == MutMutable {
			out = append(out, p)
		}
	}
	return out
}

// IsCreateOnly reports whether key is accepted at create and rejected by update.
func IsCreateOnly(key string) bool {
	p, ok := Lookup(key)
	return ok && p.Mutability == MutCreateOnly
}

func envName(key string) string { return "MIKYU_" + strings.ToUpper(key) }

func flagName(key string) string { return strings.ReplaceAll(key, "_", "-") }

func optionName(key string) string { return "With" + pascal(key) }

func pascal(s string) string {
	var b strings.Builder
	for w := range strings.SplitSeq(s, "_") {
		if initialism, ok := initialisms[w]; ok {
			b.WriteString(initialism)
			continue
		}
		b.WriteString(strings.ToUpper(w[:1]))
		b.WriteString(w[1:])
	}
	return b.String()
}

// initialisms keeps generated option names in Go's capitalization style.
var initialisms = map[string]string{"tls": "TLS", "ca": "CA"}

// Value is a kind-tagged parameter value, as parsed from a flag, env var, or option.
type Value struct {
	kind Kind
	num  int64
	str  string
	t    time.Time
	f    float64
}

func NewInt(n int) Value                { return Value{kind: KindInt, num: int64(n)} }
func NewUint32(n uint32) Value          { return Value{kind: KindUint32, num: int64(n)} }
func NewDuration(d time.Duration) Value { return Value{kind: KindDuration, num: int64(d)} }
func NewBytes(b int64) Value            { return Value{kind: KindBytes, num: b} }
func NewBool(b bool) Value              { return Value{kind: KindBool, num: boolNum(b)} }
func NewString(s string) Value          { return Value{kind: KindString, str: s} }
func NewEnum(s string) Value            { return Value{kind: KindEnum, str: s} }
func NewFloat(f float64) Value          { return Value{kind: KindFloat, f: f} }
func NewSpecDuration(d Duration) Value  { return NewDuration(d.Duration()) }

// NewTime strips any monotonic reading so Values compare and print the same way they were built.
func NewTime(t time.Time) Value { return Value{kind: KindTime, t: t.Round(0).UTC()} }

func (v Value) Kind() Kind              { return v.kind }
func (v Value) Int() int                { return int(v.numOf(KindInt)) }
func (v Value) Uint32() uint32          { return uint32(v.numOf(KindUint32)) }
func (v Value) Duration() time.Duration { return time.Duration(v.numOf(KindDuration)) }
func (v Value) Bytes() int64            { return v.numOf(KindBytes) }
func (v Value) Bool() bool              { return v.numOf(KindBool) != 0 }
func (v Value) Text() string            { return v.strOf(KindString) }
func (v Value) Enum() string            { return v.strOf(KindEnum) }
func (v Value) Time() time.Time         { return v.timeOf(KindTime) }
func (v Value) Float() float64          { return v.floatOf(KindFloat) }

// String renders the value for flags, logs, and describe output.
func (v Value) String() string {
	switch v.kind {
	case KindDuration:
		return v.Duration().String()
	case KindBool:
		return strconv.FormatBool(v.Bool())
	case KindTime:
		return v.Time().Format(time.RFC3339)
	case KindString, KindEnum:
		return v.str
	case KindFloat:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case KindUnknown:
		return "<unset>"
	default:
		return strconv.FormatInt(v.num, 10)
	}
}

func boolNum(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// numOf panics on kind mismatch, which is always a wiring bug in this package:
// a Param's Kind and its setter are declared side by side.
func (v Value) numOf(k Kind) int64 {
	if v.kind != k {
		panic(fmt.Sprintf("spec: value of kind %s read as %s", v.kind, k))
	}
	return v.num
}

func (v Value) strOf(k Kind) string {
	if v.kind != k {
		panic(fmt.Sprintf("spec: value of kind %s read as %s", v.kind, k))
	}
	return v.str
}

// floatOf panics on kind mismatch, like the other typed accessors.
func (v Value) floatOf(k Kind) float64 {
	if v.kind != k {
		panic(fmt.Sprintf("spec: value of kind %s read as %s", v.kind, k))
	}
	return v.f
}

func (v Value) timeOf(k Kind) time.Time {
	if v.kind != k {
		panic(fmt.Sprintf("spec: value of kind %s read as %s", v.kind, k))
	}
	return v.t
}

// ParseValue converts flag or env text into a Value of kind.
func ParseValue(kind Kind, text string) (Value, error) {
	switch kind {
	case KindInt:
		n, err := strconv.Atoi(text)
		if err != nil {
			return Value{}, err
		}
		return NewInt(n), nil
	case KindUint32:
		n, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return Value{}, err
		}
		return NewUint32(uint32(n)), nil
	case KindDuration:
		d, err := time.ParseDuration(text)
		if err != nil {
			n, numErr := strconv.ParseInt(text, 10, 64)
			if numErr != nil {
				return Value{}, err
			}
			d = time.Duration(n)
		}
		return NewDuration(d), nil
	case KindBytes:
		n, err := parseBytes(text)
		if err != nil {
			return Value{}, err
		}
		return NewBytes(n), nil
	case KindBool:
		b, err := strconv.ParseBool(text)
		if err != nil {
			return Value{}, err
		}
		return NewBool(b), nil
	case KindString:
		return NewString(text), nil
	case KindEnum:
		return NewEnum(text), nil
	case KindFloat:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return Value{}, err
		}
		return NewFloat(f), nil
	case KindTime:
		t, err := time.Parse(time.RFC3339, text)
		if err != nil {
			return Value{}, err
		}
		return NewTime(t), nil
	default:
		return Value{}, fmt.Errorf("spec: unsupported kind %s", kind)
	}
}
