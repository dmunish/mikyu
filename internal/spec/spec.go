package spec

import "time"

// Spec holds every tunable parameter of a queue or session, sparsely.
type Spec struct {
	Engine    EngineSpec    `json:"engine,omitzero" yaml:"engine,omitempty"`
	Retention RetentionSpec `json:"retention,omitzero" yaml:"retention,omitempty"`
	Task      TaskSpec      `json:"task,omitzero" yaml:"task,omitempty"`
}

// EngineSpec covers the log-segment writer.
type EngineSpec struct {
	BatchSize       *int      `json:"batch_size,omitempty" yaml:"batch_size,omitempty"`
	MaxInterval     *Duration `json:"max_interval,omitempty" yaml:"max_interval,omitempty"`
	MinInterval     *Duration `json:"min_interval,omitempty" yaml:"min_interval,omitempty"`
	SegmentSize     *int64    `json:"segment_size,omitempty" yaml:"segment_size,omitempty"`
	SegmentAge      *Duration `json:"segment_age,omitempty" yaml:"segment_age,omitempty"`
	MaxMessageBytes *int64    `json:"max_message_bytes,omitempty" yaml:"max_message_bytes,omitempty"`
	Encrypted       *bool     `json:"encrypted,omitempty" yaml:"encrypted,omitempty"`
	Fsync           *bool     `json:"fsync,omitempty" yaml:"fsync,omitempty"`
}

// RetentionSpec caps how much history may be deleted. Zero means unlimited.
type RetentionSpec struct {
	Age   *Duration `json:"age,omitempty" yaml:"age,omitempty"`
	Bytes *int64    `json:"bytes,omitempty" yaml:"bytes,omitempty"`
}

// TaskSpec holds the task-queue defaults resolved under per-call options.
type TaskSpec struct {
	DefaultVisibility  *Duration `json:"default_visibility,omitempty" yaml:"default_visibility,omitempty"`
	DefaultMaxAttempts *uint32   `json:"default_max_attempts,omitempty" yaml:"default_max_attempts,omitempty"`
}

// Session carries the process-level parameters that never reach a queue spec or the wire.
type Session struct {
	Target      string `yaml:"target"`
	DataDir     string `yaml:"data_dir"`
	TLSEnabled  bool   `yaml:"tls_enabled"`
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`
	MaxConns    int    `yaml:"max_conns"`
}

// Merge returns s with each unset field filled from other.
func (s Spec) Merge(other Spec) Spec {
	return Spec{
		Engine:    s.Engine.merge(other.Engine),
		Retention: s.Retention.merge(other.Retention),
		Task:      s.Task.merge(other.Task),
	}
}

// Effective resolves s against the session layer and the built-in layer, in that precedence order.
func (s Spec) Effective(session, builtin Spec) Spec {
	return s.Merge(session).Merge(builtin)
}

func (e EngineSpec) merge(o EngineSpec) EngineSpec {
	if e.BatchSize == nil {
		e.BatchSize = o.BatchSize
	}
	if e.MaxInterval == nil {
		e.MaxInterval = o.MaxInterval
	}
	if e.MinInterval == nil {
		e.MinInterval = o.MinInterval
	}
	if e.SegmentSize == nil {
		e.SegmentSize = o.SegmentSize
	}
	if e.SegmentAge == nil {
		e.SegmentAge = o.SegmentAge
	}
	if e.MaxMessageBytes == nil {
		e.MaxMessageBytes = o.MaxMessageBytes
	}
	if e.Encrypted == nil {
		e.Encrypted = o.Encrypted
	}
	if e.Fsync == nil {
		e.Fsync = o.Fsync
	}
	return e
}

func (r RetentionSpec) merge(o RetentionSpec) RetentionSpec {
	if r.Age == nil {
		r.Age = o.Age
	}
	if r.Bytes == nil {
		r.Bytes = o.Bytes
	}
	return r
}

func (t TaskSpec) merge(o TaskSpec) TaskSpec {
	if t.DefaultVisibility == nil {
		t.DefaultVisibility = o.DefaultVisibility
	}
	if t.DefaultMaxAttempts == nil {
		t.DefaultMaxAttempts = o.DefaultMaxAttempts
	}
	return t
}

// Built-in defaults shared by every layer that resolves them.
const (
	// DefaultVisibility is the lease length when neither call, spec, nor session sets one.
	DefaultVisibility = 30 * time.Second
	// DefaultMaxAttempts is the retry budget when neither call, spec, nor session sets one.
	DefaultMaxAttempts = uint32(5)
)

// Every Spec and Session field appears here exactly once; Builtin and BuiltinSession are total.
const (
	builtinBatchSize          = 256
	builtinMaxInterval        = Duration(10 * time.Millisecond)
	builtinMinInterval        = Duration(30 * time.Microsecond)
	builtinSegmentSize        = int64(64 << 20)
	builtinSegmentAge         = Duration(time.Hour)
	builtinMaxMessageBytes    = int64(1 << 20)
	builtinEncrypted          = false
	builtinFsync              = true
	builtinRetentionAge       = Duration(0)
	builtinRetentionBytes     = int64(0)
	builtinDefaultVisibility  = Duration(DefaultVisibility)
	builtinDefaultMaxAttempts = DefaultMaxAttempts
	builtinTarget             = "tcp://127.0.0.1:7420"
	builtinDataDir            = "./data"
	builtinMaxConns           = 10
)

// Builtin returns the fully-populated built-in layer.
func Builtin() Spec {
	return Spec{
		Engine: EngineSpec{
			BatchSize:       new(builtinBatchSize),
			MaxInterval:     new(builtinMaxInterval),
			MinInterval:     new(builtinMinInterval),
			SegmentSize:     new(builtinSegmentSize),
			SegmentAge:      new(builtinSegmentAge),
			MaxMessageBytes: new(builtinMaxMessageBytes),
			Encrypted:       new(builtinEncrypted),
			Fsync:           new(builtinFsync),
		},
		Retention: RetentionSpec{
			Age:   new(builtinRetentionAge),
			Bytes: new(builtinRetentionBytes),
		},
		Task: TaskSpec{
			DefaultVisibility:  new(builtinDefaultVisibility),
			DefaultMaxAttempts: new(builtinDefaultMaxAttempts),
		},
	}
}

// BuiltinSession returns the built-in session layer. TLS is off at this layer.
func BuiltinSession() Session {
	return Session{
		Target:   builtinTarget,
		DataDir:  builtinDataDir,
		MaxConns: builtinMaxConns,
	}
}
