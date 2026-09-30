package mikyu

import "github.com/dmunish/mikyu/internal/spec"

// Param is one row of the parameter table: the canonical key, path, kind,
// layer, mutability, and every surface name a configuration value reaches.
type Param = spec.Param

// Kind is a parameter's value type.
type Kind = spec.Kind

// Layer is where a parameter may be set.
type Layer = spec.Layer

// Scope is which side of a session boundary a parameter configures.
type Scope = spec.Scope

// Session carries the process-level parameters a host or daemon is launched with.
type Session = spec.Session

const (
	// KindInt values are arbitrary-precision integers.
	KindInt = spec.KindInt
	// KindUint32 values are 32-bit unsigned integers.
	KindUint32 = spec.KindUint32
	// KindDuration values are time spans.
	KindDuration = spec.KindDuration
	// KindBytes values are integer byte counts.
	KindBytes = spec.KindBytes
	// KindBool values are booleans.
	KindBool = spec.KindBool
	// KindString values are free-form text.
	KindString = spec.KindString
	// KindTime values are instants.
	KindTime = spec.KindTime
	// KindEnum values are one of a row's declared constants.
	KindEnum = spec.KindEnum
	// KindFloat values are 64-bit floating point ratios.
	KindFloat = spec.KindFloat

	// LayerCall parameters are per-call arguments, never persisted.
	LayerCall = spec.LayerCall
	// LayerSpec parameters are per-queue settings persisted in queues.json.
	LayerSpec = spec.LayerSpec
	// LayerSession parameters are per-process settings that never reach the wire.
	LayerSession = spec.LayerSession

	// ScopeClient parameters configure only the client side of a connection.
	ScopeClient = spec.ScopeClient
	// ScopeServer parameters configure only the server side of a connection.
	ScopeServer = spec.ScopeServer
	// ScopeBoth parameters configure both sides of a connection.
	ScopeBoth = spec.ScopeBoth
)

// Schema returns a fresh copy of the compiled-in parameter table.
func Schema() []Param {
	out := make([]Param, len(spec.Table))
	copy(out, spec.Table)
	return out
}

// Lookup finds a parameter by its canonical key.
func Lookup(key string) (Param, bool) { return spec.Lookup(key) }

// EncodeSpec renders a sparse spec as JSON.
func EncodeSpec(s Spec) ([]byte, error) { return spec.EncodeSpec(s) }

// EncodeSpecYAML renders a sparse spec as YAML.
func EncodeSpecYAML(s Spec) ([]byte, error) { return spec.EncodeSpecYAML(s) }

// DecodeSpec parses a sparse spec from JSON, rejecting unknown keys and oversized payloads.
func DecodeSpec(data []byte) (Spec, error) { return spec.DecodeSpec(data) }

// DecodeSpecYAML parses a sparse spec from YAML, rejecting unknown keys.
func DecodeSpecYAML(data []byte) (Spec, error) { return spec.DecodeSpecYAML(data) }

// ValidateCreate checks a fully resolved spec for use at create time.
func ValidateCreate(s Spec) error { return spec.ValidateCreate(s) }

// ValidateUpdate rejects a patch that sets a create-only field.
func ValidateUpdate(patch Spec) error { return spec.ValidateUpdate(patch) }

// ValidateSession checks the process-level parameters.
func ValidateSession(s Session) error { return spec.ValidateSession(s) }

// Builtin returns the compiled-in defaults as a total spec.
func Builtin() Spec { return spec.Builtin() }

// BuiltinSession returns the compiled-in session defaults.
func BuiltinSession() Session { return spec.BuiltinSession() }
