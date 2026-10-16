package mikyu

import "github.com/dmunish/mikyu/internal/spec"

// SpecVersion is the spec schema version this build speaks. A server
// that speaks a newer version rejects an older client with
// ErrSpecTooNew.
const SpecVersion = spec.CurrentSpecVersion

// Spec is a set of queue settings. Every field is a pointer, and nil
// means "not set", which is what makes specs sparse: two specs merge by
// letting the more specific one win field by field.
//
// The layering, from most to least specific, is:
//
//	call options, the queue's stored spec, session defaults, built-ins
//
// So a queue runs with whatever its settings say, and inherits
// everything else from the node that opened the directory and the
// built-in defaults. Use the With constructors in this package to build
// one field at a time, and ValidateCreate or ValidateUpdate to check a
// spec before sending it.
type Spec = spec.Spec

// StorageSpec shapes the segment files a queue writes and how they are
// protected.
type StorageSpec = spec.StorageSpec

// FlushSpec bounds the write batch: how much pending data triggers a
// flush and how long a batch may linger before flushing anyway.
type FlushSpec = spec.FlushSpec

// RetentionSpec caps how much history may be removed: records older
// than MaxAge, or queues larger than MaxBytes. Zero means unlimited.
type RetentionSpec = spec.RetentionSpec

// TaskSpec holds the delivery settings a task queue starts from: the
// visibility lease, the retry budget, and the in-flight and
// dead-letter bounds.
type TaskSpec = spec.TaskSpec

// Duration is a time span that serializes as a Go duration string such
// as "10ms" or "5m"; a plain integer is read as nanoseconds. It exists
// so specs stay readable on the wire, in YAML, and in queues.json.
type Duration = spec.Duration

// Durability selects when an append is acknowledged.
type Durability = spec.Durability

// Encryption selects how queue data is protected at rest.
type Encryption = spec.Encryption

// Compaction selects how closed segments shed superseded records.
type Compaction = spec.Compaction

const (
	// DurabilitySync acknowledges an append only after its bytes and
	// index are fsynced. The default, and the safe choice.
	DurabilitySync = spec.DurabilitySync

	// DurabilityAsync acknowledges an append as soon as it is visible
	// in memory, flushing in the background. Much faster, with a small
	// loss window on a crash.
	DurabilityAsync = spec.DurabilityAsync

	// EncryptionNone stores queue data as plain files. The default.
	EncryptionNone = spec.EncryptionNone

	// EncryptionOS asks the operating system to encrypt the queue's
	// directory (fscrypt on Linux, EFS on Windows).
	EncryptionOS = spec.EncryptionOS

	// EncryptionAES seals every record payload with AES-256-GCM under
	// an envelope scheme: master keys come from a key file outside the
	// data directory, and each queue wraps its own data key.
	EncryptionAES = spec.EncryptionAES

	// CompactionNone keeps every record forever, subject only to
	// retention. The default.
	CompactionNone = spec.CompactionNone

	// CompactionKeys drops superseded records when a segment closes:
	// for each Group, only the newest record survives. Log queues only.
	CompactionKeys = spec.CompactionKeys
)

// Param is one row of the configuration table: a canonical key, where
// it applies, what kind of value it takes, and the name it carries on
// every surface (option constructor, CLI flag, environment variable).
// Schema returns the whole table.
type Param = spec.Param

// Kind is the value type of a configuration parameter.
type Kind = spec.Kind

// Layer says where a parameter may be set: per call, per queue (stored
// in the spec), or per process (in the session).
type Layer = spec.Layer

// Scope says which side of a connection a parameter configures: the
// client, the server, or both.
type Scope = spec.Scope

// Session carries the process-level settings a daemon or embedded node
// is launched with: bind target, TLS, auth, encryption keys, metrics.
// Session settings are ambient; they belong to the node, never to the
// data inside it.
type Session = spec.Session

// The Kind, Layer, and Scope constants mirror the values of the
// parameter table, so callers can switch over Param fields without
// importing internals.
const (
	KindInt      = spec.KindInt
	KindUint32   = spec.KindUint32
	KindDuration = spec.KindDuration
	KindBytes    = spec.KindBytes
	KindBool     = spec.KindBool
	KindString   = spec.KindString
	KindTime     = spec.KindTime
	KindEnum     = spec.KindEnum
	KindFloat    = spec.KindFloat

	LayerCall    = spec.LayerCall
	LayerSpec    = spec.LayerSpec
	LayerSession = spec.LayerSession

	ScopeClient = spec.ScopeClient
	ScopeServer = spec.ScopeServer
	ScopeBoth   = spec.ScopeBoth
)

// Schema returns a fresh copy of the compiled-in parameter table. The
// table is the single source of truth behind the SDK options, CLI
// flags, environment variables, YAML config, and wire format.
func Schema() []Param {
	out := make([]Param, len(spec.Table))
	copy(out, spec.Table)
	return out
}

// Lookup finds a parameter by its canonical key, such as
// "max_segment".
func Lookup(key string) (Param, bool) { return spec.Lookup(key) }

// EncodeSpec renders a sparse spec as JSON, omitting every unset
// field.
func EncodeSpec(s Spec) ([]byte, error) { return spec.EncodeSpec(s) }

// EncodeSpecYAML renders a sparse spec as YAML.
func EncodeSpecYAML(s Spec) ([]byte, error) { return spec.EncodeSpecYAML(s) }

// DecodeSpec parses a sparse spec from JSON. Unknown keys are rejected,
// as is trailing data after the JSON document.
func DecodeSpec(data []byte) (Spec, error) { return spec.DecodeSpec(data) }

// DecodeSpecYAML parses a sparse spec from YAML, rejecting unknown
// keys.
func DecodeSpecYAML(data []byte) (Spec, error) { return spec.DecodeSpecYAML(data) }

// ValidateCreate checks a fully resolved spec for use at create time,
// including the cross-field rules (a payload must fit a segment, the
// cache must cover what a segment can hold).
func ValidateCreate(s Spec) error { return spec.ValidateCreate(s) }

// ValidateUpdate rejects a patch that sets a create-only field, naming
// the field in the error. Call it client-side to fail fast before a
// round trip.
func ValidateUpdate(patch Spec) error { return spec.ValidateUpdate(patch) }

// ValidateSession checks a set of process-level settings: connection
// limits, metrics intervals, auth and TLS pairings.
func ValidateSession(s Session) error { return spec.ValidateSession(s) }

// Builtin returns the compiled-in defaults as a total spec: every
// queue-level field set.
func Builtin() Spec { return spec.Builtin() }

// BuiltinSession returns the compiled-in session defaults.
func BuiltinSession() Session { return spec.BuiltinSession() }
