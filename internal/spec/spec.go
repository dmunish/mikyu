package spec

import "time"

// Spec holds every tunable parameter of a queue, sparsely.
type Spec struct {
	Storage   StorageSpec   `json:"storage,omitzero"   yaml:"storage,omitempty"`
	Flush     FlushSpec     `json:"flush,omitzero"     yaml:"flush,omitempty"`
	Retention RetentionSpec `json:"retention,omitzero" yaml:"retention,omitempty"`
	Task      TaskSpec      `json:"task,omitzero"      yaml:"task,omitempty"`
}

// StorageSpec shapes the segment files and their protection.
type StorageSpec struct {
	Durability    *Durability `json:"durability,omitempty"      yaml:"durability,omitempty"`
	Encryption    *Encryption `json:"encryption,omitempty"      yaml:"encryption,omitempty"`
	Compaction    *Compaction `json:"compaction,omitempty"      yaml:"compaction,omitempty"`
	MaxSegment    *int64      `json:"max_segment,omitempty"     yaml:"max_segment,omitempty"`
	MaxSegmentAge *Duration   `json:"max_segment_age,omitempty" yaml:"max_segment_age,omitempty"`
	MaxPayload    *int64      `json:"max_payload,omitempty"     yaml:"max_payload,omitempty"`
	MaxCache      *int64      `json:"max_cache,omitempty"       yaml:"max_cache,omitempty"`
}

// FlushSpec bounds the WAL write batch.
type FlushSpec struct {
	MaxBytes *int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
	MaxWait  *Duration `json:"max_wait,omitempty"  yaml:"max_wait,omitempty"`
}

// RetentionSpec caps how much history may be deleted. Zero means unlimited.
type RetentionSpec struct {
	MaxAge   *Duration `json:"max_age,omitempty"   yaml:"max_age,omitempty"`
	MaxBytes *int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
}

// TaskSpec holds the task-queue defaults and bounds resolved under per-call options.
type TaskSpec struct {
	Lease          *Duration `json:"lease,omitempty"            yaml:"lease,omitempty"`
	MaxAttempts    *uint32   `json:"max_attempts,omitempty"     yaml:"max_attempts,omitempty"`
	MaxInFlight    *uint32   `json:"max_in_flight,omitempty"    yaml:"max_in_flight,omitempty"`
	MaxDeadLetters *uint32   `json:"max_dead_letters,omitempty" yaml:"max_dead_letters,omitempty"`
}

// Merge returns s with each unset field filled from other.
func (s Spec) Merge(other Spec) Spec {
	return Spec{
		Storage:   s.Storage.merge(other.Storage),
		Flush:     s.Flush.merge(other.Flush),
		Retention: s.Retention.merge(other.Retention),
		Task:      s.Task.merge(other.Task),
	}
}

// Effective resolves s against the session layer and the built-in layer, in that precedence order.
func (s Spec) Effective(session, builtin Spec) Spec {
	return Resolve(s.Merge(session).Merge(builtin))
}

func (st StorageSpec) merge(o StorageSpec) StorageSpec {
	if st.Durability == nil {
		st.Durability = o.Durability
	}
	if st.Encryption == nil {
		st.Encryption = o.Encryption
	}
	if st.Compaction == nil {
		st.Compaction = o.Compaction
	}
	if st.MaxSegment == nil {
		st.MaxSegment = o.MaxSegment
	}
	if st.MaxSegmentAge == nil {
		st.MaxSegmentAge = o.MaxSegmentAge
	}
	if st.MaxPayload == nil {
		st.MaxPayload = o.MaxPayload
	}
	if st.MaxCache == nil {
		st.MaxCache = o.MaxCache
	}
	return st
}

func (f FlushSpec) merge(o FlushSpec) FlushSpec {
	if f.MaxBytes == nil {
		f.MaxBytes = o.MaxBytes
	}
	if f.MaxWait == nil {
		f.MaxWait = o.MaxWait
	}
	return f
}

func (r RetentionSpec) merge(o RetentionSpec) RetentionSpec {
	if r.MaxAge == nil {
		r.MaxAge = o.MaxAge
	}
	if r.MaxBytes == nil {
		r.MaxBytes = o.MaxBytes
	}
	return r
}

func (t TaskSpec) merge(o TaskSpec) TaskSpec {
	if t.Lease == nil {
		t.Lease = o.Lease
	}
	if t.MaxAttempts == nil {
		t.MaxAttempts = o.MaxAttempts
	}
	if t.MaxInFlight == nil {
		t.MaxInFlight = o.MaxInFlight
	}
	if t.MaxDeadLetters == nil {
		t.MaxDeadLetters = o.MaxDeadLetters
	}
	return t
}

// Session carries the process-level parameters that never reach a queue spec or the wire.
type Session struct {
	Target         string           `yaml:"target"`
	DataDir        string           `yaml:"data_dir"`
	MaxConnections int              `yaml:"max_connections"`
	TLS            TLSConfig        `yaml:"tls"`
	Auth           AuthConfig       `yaml:"auth"`
	Encryption     EncryptionConfig `yaml:"encryption"`
	Metrics        MetricsConfig    `yaml:"metrics"`
}

// TLSConfig holds the wire encryption material.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Cert     string `yaml:"cert"`
	Key      string `yaml:"key"`
	ClientCA string `yaml:"client_ca"`
}

// AuthConfig holds the handshake credentials.
type AuthConfig struct {
	Method    string `yaml:"method"`
	Token     string `yaml:"token"`
	TokenFile string `yaml:"token_file"`
}

// EncryptionConfig holds the at-rest key material source.
type EncryptionConfig struct {
	KeyFile string `yaml:"key_file"`
}

// MetricsConfig holds the collector settings.
type MetricsConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Interval           Duration `yaml:"interval"`
	History            int      `yaml:"history"`
	Listen             string   `yaml:"listen"`
	PerQueuePrometheus bool     `yaml:"per_queue_prometheus"`
	Histograms         bool     `yaml:"histograms"`
}

// Builtin returns the fully-resolved built-in layer.
func Builtin() Spec { return Resolve(Spec{}) }

// BuiltinSession returns the built-in session layer. TLS is off at this layer; metrics run enabled at a one-second tick.
func BuiltinSession() Session {
	return Session{
		Target:         "tcp://127.0.0.1:7420",
		MaxConnections: 10,
		Auth:           AuthConfig{Method: "none"},
		Metrics: MetricsConfig{
			Enabled:    true,
			Interval:   Duration(time.Second),
			History:    300,
			Histograms: true,
		},
	}
}
