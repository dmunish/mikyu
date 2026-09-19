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
	for i := KindInt; i <= KindTime; i++ {
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
	Key        string     `json:"key"`  // canonical flat key: identity of env, flag, option, and patch
	Path       string     `json:"path"` // location in the nested spec, e.g. "engine.batch_size"
	Kind       Kind       `json:"kind"`
	Layer      Layer      `json:"layer"`
	Scope      Scope      `json:"scope"`
	Mutability Mutability `json:"mutability"`
	EnvName    string     `json:"env_name"`
	FlagName   string     `json:"flag_name"`
	OptionName string     `json:"option_name"`

	Apply        func(*Spec, Value)    `json:"-"`
	ApplySession func(*Session, Value) `json:"-"`
}

func (p Param) String() string { return p.Key }

// Parse converts flag or env text into a Value of this parameter's kind.
func (p Param) Parse(text string) (Value, error) {
	v, err := ParseValue(p.Kind, text)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %s: %w", ErrInvalidConfig, p.Key, err)
	}
	return v, nil
}

// specParam declares a queue-scoped parameter grouped under group. The nested
// leaf key is the canonical key with a redundant group prefix removed, so
// retention.age stays readable inside its block while retention_age stays
// globally unique for env vars, flags, and patches.
func specParam(group, key string, kind Kind, mut Mutability) Param {
	return Param{
		Key:        key,
		Path:       group + "." + strings.TrimPrefix(key, group+"_"),
		Kind:       kind,
		Layer:      LayerSpec,
		Mutability: mut,
		EnvName:    envName(key),
		FlagName:   flagName(key),
		OptionName: optionName(key),
	}
}

// sessionParam declares a process-only parameter living in the server block.
func sessionParam(key string, kind Kind, scope Scope) Param {
	return Param{
		Key:        key,
		Path:       "server." + key,
		Kind:       kind,
		Layer:      LayerSession,
		Scope:      scope,
		Mutability: MutNotApplicable,
		EnvName:    envName(key),
		FlagName:   flagName(key),
		OptionName: optionName(key),
	}
}

// callParam declares an ephemeral per-call parameter. Per-call values are
// arguments, not config, so they have no env var and no persisted path.
func callParam(key string, kind Kind) Param {
	return Param{
		Key:        key,
		Path:       key,
		Kind:       kind,
		Layer:      LayerCall,
		Mutability: MutNotApplicable,
		FlagName:   flagName(key),
		OptionName: optionName(key),
	}
}

// specSetters and sessionSetters bind each canonical key to its field. The
// compiler checks the kind of every assignment.
var specSetters = map[string]func(*Spec, Value){
	"batch_size":           func(s *Spec, v Value) { s.Engine.BatchSize = new(v.Int()) },
	"max_interval":         func(s *Spec, v Value) { s.Engine.MaxInterval = new(Duration(v.Duration())) },
	"min_interval":         func(s *Spec, v Value) { s.Engine.MinInterval = new(Duration(v.Duration())) },
	"segment_size":         func(s *Spec, v Value) { s.Engine.SegmentSize = new(v.Bytes()) },
	"segment_age":          func(s *Spec, v Value) { s.Engine.SegmentAge = new(Duration(v.Duration())) },
	"max_message_bytes":    func(s *Spec, v Value) { s.Engine.MaxMessageBytes = new(v.Bytes()) },
	"encrypted":            func(s *Spec, v Value) { s.Engine.Encrypted = new(v.Bool()) },
	"fsync":                func(s *Spec, v Value) { s.Engine.Fsync = new(v.Bool()) },
	"retention_age":        func(s *Spec, v Value) { s.Retention.Age = new(Duration(v.Duration())) },
	"retention_bytes":      func(s *Spec, v Value) { s.Retention.Bytes = new(v.Bytes()) },
	"default_visibility":   func(s *Spec, v Value) { s.Task.DefaultVisibility = new(Duration(v.Duration())) },
	"default_max_attempts": func(s *Spec, v Value) { s.Task.DefaultMaxAttempts = new(v.Uint32()) },
	"max_in_flight":        func(s *Spec, v Value) { s.Task.MaxInFlight = new(v.Uint32()) },
	"max_dlq":              func(s *Spec, v Value) { s.Task.MaxDLQ = new(v.Uint32()) },
}

var sessionSetters = map[string]func(*Session, Value){
	"target":                       func(s *Session, v Value) { s.Target = v.Text() },
	"data_dir":                     func(s *Session, v Value) { s.DataDir = v.Text() },
	"tls_enabled":                  func(s *Session, v Value) { s.TLSEnabled = v.Bool() },
	"tls_cert_file":                func(s *Session, v Value) { s.TLSCertFile = v.Text() },
	"tls_key_file":                 func(s *Session, v Value) { s.TLSKeyFile = v.Text() },
	"max_conns":                    func(s *Session, v Value) { s.MaxConns = v.Int() },
	"metrics_enabled":              func(s *Session, v Value) { s.MetricsEnabled = v.Bool() },
	"metrics_interval":             func(s *Session, v Value) { s.MetricsInterval = Duration(v.Duration()) },
	"metrics_history":              func(s *Session, v Value) { s.MetricsHistory = v.Int() },
	"metrics_listen":               func(s *Session, v Value) { s.MetricsListen = v.Text() },
	"metrics_per_queue_prometheus": func(s *Session, v Value) { s.MetricsPerQueuePrometheus = v.Bool() },
	"metrics_histograms":           func(s *Session, v Value) { s.MetricsHistograms = v.Bool() },
	"auth_method":                  func(s *Session, v Value) { s.AuthMethod = v.Text() },
	"auth_token_file":              func(s *Session, v Value) { s.AuthTokenFile = v.Text() },
	"auth_token":                   func(s *Session, v Value) { s.AuthToken = v.Text() },
	"tls_client_ca_file":           func(s *Session, v Value) { s.TLSClientCaFile = v.Text() },
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

var specGetters = map[string]func(*Spec) (Value, bool){
	"batch_size":           getter(func(s *Spec) *int { return s.Engine.BatchSize }, NewInt),
	"max_interval":         getter(func(s *Spec) *Duration { return s.Engine.MaxInterval }, NewSpecDuration),
	"min_interval":         getter(func(s *Spec) *Duration { return s.Engine.MinInterval }, NewSpecDuration),
	"segment_size":         getter(func(s *Spec) *int64 { return s.Engine.SegmentSize }, NewBytes),
	"segment_age":          getter(func(s *Spec) *Duration { return s.Engine.SegmentAge }, NewSpecDuration),
	"max_message_bytes":    getter(func(s *Spec) *int64 { return s.Engine.MaxMessageBytes }, NewBytes),
	"encrypted":            getter(func(s *Spec) *bool { return s.Engine.Encrypted }, NewBool),
	"fsync":                getter(func(s *Spec) *bool { return s.Engine.Fsync }, NewBool),
	"retention_age":        getter(func(s *Spec) *Duration { return s.Retention.Age }, NewSpecDuration),
	"retention_bytes":      getter(func(s *Spec) *int64 { return s.Retention.Bytes }, NewBytes),
	"default_visibility":   getter(func(s *Spec) *Duration { return s.Task.DefaultVisibility }, NewSpecDuration),
	"default_max_attempts": getter(func(s *Spec) *uint32 { return s.Task.DefaultMaxAttempts }, NewUint32),
	"max_in_flight":        getter(func(s *Spec) *uint32 { return s.Task.MaxInFlight }, NewUint32),
	"max_dlq":              getter(func(s *Spec) *uint32 { return s.Task.MaxDLQ }, NewUint32),
}

// Field returns the value p holds in s and whether s sets it.
func (s Spec) Field(p Param) (Value, bool) {
	get, ok := specGetters[p.Key]
	if !ok {
		panic("spec: " + p.Key + " is not a spec-layer parameter")
	}
	return get(&s)
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
		specParam("engine", "batch_size", KindInt, MutMutable),
		specParam("engine", "max_interval", KindDuration, MutMutable),
		specParam("engine", "min_interval", KindDuration, MutMutable),
		specParam("engine", "segment_size", KindBytes, MutCreateOnly),
		specParam("engine", "segment_age", KindDuration, MutMutable),
		specParam("engine", "max_message_bytes", KindBytes, MutCreateOnly),
		specParam("engine", "encrypted", KindBool, MutCreateOnly),
		specParam("engine", "fsync", KindBool, MutCreateOnly),
		specParam("retention", "retention_age", KindDuration, MutMutable),
		specParam("retention", "retention_bytes", KindBytes, MutMutable),
		specParam("task", "default_visibility", KindDuration, MutMutable),
		specParam("task", "default_max_attempts", KindUint32, MutMutable),
		specParam("task", "max_in_flight", KindUint32, MutMutable),
		specParam("task", "max_dlq", KindUint32, MutMutable),

		callParam("visibility", KindDuration),
		callParam("max_attempts", KindUint32),
		callParam("scheduled_at", KindTime),
		callParam("retry_after", KindDuration),

		sessionParam("target", KindString, ScopeBoth),
		sessionParam("data_dir", KindString, ScopeBoth),
		sessionParam("tls_enabled", KindBool, ScopeBoth),
		sessionParam("tls_cert_file", KindString, ScopeBoth),
		sessionParam("tls_key_file", KindString, ScopeBoth),
		sessionParam("max_conns", KindInt, ScopeBoth),

		sessionParam("metrics_enabled", KindBool, ScopeBoth),
		sessionParam("metrics_interval", KindDuration, ScopeBoth),
		sessionParam("metrics_history", KindInt, ScopeBoth),
		sessionParam("metrics_listen", KindString, ScopeServer),
		sessionParam("metrics_per_queue_prometheus", KindBool, ScopeServer),
		sessionParam("metrics_histograms", KindBool, ScopeBoth),

		sessionParam("auth_method", KindString, ScopeServer),
		sessionParam("auth_token_file", KindString, ScopeBoth),
		sessionParam("auth_token", KindString, ScopeBoth),
		sessionParam("tls_client_ca_file", KindString, ScopeServer),
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
var initialisms = map[string]string{"tls": "TLS", "dlq": "DLQ"}

// Value is a kind-tagged parameter value, as parsed from a flag, env var, or option.
type Value struct {
	kind Kind
	num  int64
	str  string
	t    time.Time
}

func NewInt(n int) Value                { return Value{kind: KindInt, num: int64(n)} }
func NewUint32(n uint32) Value          { return Value{kind: KindUint32, num: int64(n)} }
func NewDuration(d time.Duration) Value { return Value{kind: KindDuration, num: int64(d)} }
func NewBytes(b int64) Value            { return Value{kind: KindBytes, num: b} }
func NewBool(b bool) Value              { return Value{kind: KindBool, num: boolNum(b)} }
func NewString(s string) Value          { return Value{kind: KindString, str: s} }
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
func (v Value) Time() time.Time         { return v.timeOf(KindTime) }

// String renders the value for flags, logs, and describe output.
func (v Value) String() string {
	switch v.kind {
	case KindDuration:
		return v.Duration().String()
	case KindBool:
		return strconv.FormatBool(v.Bool())
	case KindTime:
		return v.Time().Format(time.RFC3339)
	case KindString:
		return v.str
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
		n, err := strconv.ParseInt(text, 10, 64)
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
