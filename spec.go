package mikyu

import "github.com/dmunish/mikyu/internal/spec"

const SpecVersion = spec.CurrentSpecVersion

type Spec = spec.Spec

type StorageSpec = spec.StorageSpec

type FlushSpec = spec.FlushSpec

type RetentionSpec = spec.RetentionSpec

type TaskSpec = spec.TaskSpec

type Duration = spec.Duration

type Durability = spec.Durability

type Encryption = spec.Encryption

type Compaction = spec.Compaction

const (
	DurabilitySync  = spec.DurabilitySync
	DurabilityAsync = spec.DurabilityAsync

	EncryptionNone = spec.EncryptionNone
	EncryptionOS   = spec.EncryptionOS
	EncryptionAES  = spec.EncryptionAES

	CompactionNone = spec.CompactionNone
	CompactionKeys = spec.CompactionKeys
)

type Param = spec.Param

type Kind = spec.Kind

type Layer = spec.Layer

type Scope = spec.Scope

type Session = spec.Session

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

func Schema() []Param {
	out := make([]Param, len(spec.Table))
	copy(out, spec.Table)
	return out
}

func Lookup(key string) (Param, bool) { return spec.Lookup(key) }

func EncodeSpec(s Spec) ([]byte, error) { return spec.EncodeSpec(s) }

func EncodeSpecYAML(s Spec) ([]byte, error) { return spec.EncodeSpecYAML(s) }

func DecodeSpec(data []byte) (Spec, error) { return spec.DecodeSpec(data) }

func DecodeSpecYAML(data []byte) (Spec, error) { return spec.DecodeSpecYAML(data) }

func ValidateCreate(s Spec) error { return spec.ValidateCreate(s) }

func ValidateUpdate(patch Spec) error { return spec.ValidateUpdate(patch) }

func ValidateSession(s Session) error { return spec.ValidateSession(s) }

func Builtin() Spec { return spec.Builtin() }

func BuiltinSession() Session { return spec.BuiltinSession() }
