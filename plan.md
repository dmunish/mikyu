# Configuration Redesign — Implementation Plan

**Goal:** Replace the current config surface with a cap-based, enum-driven, derivation-aware model. Pre-launch hard cut: no aliases, no deprecation shims.

**Non-goals:** Wire opcodes, engine record encoding, task checkpoint format, metrics snapshot shape, IPC, package boundaries.

---

## Phase 1 — Schema, derivation, validation (`internal/spec`)

This phase defines the new vocabulary. Nothing else compiles against it yet, but the package is complete and unit-tested in isolation.

### 1.1 New enums — `internal/spec/enums.go` (new file)

```go
type Durability string
const (
    DurabilitySync  Durability = "sync"
    DurabilityAsync Durability = "async"
)

type Encryption string
const (
    EncryptionNone Encryption = "none"
    EncryptionOS   Encryption = "os"
)

type Compaction string
const (
    CompactionNone Compaction = "none"
    CompactionKeys Compaction = "keys"
)
```

Each enum gets `String()` (returns the constant), and validation accepts only the declared constants.

### 1.2 New struct shape — `internal/spec/spec.go`

Replace `Spec` and `Session`:

```go
type Spec struct {
    Storage   StorageSpec   `json:"storage,omitzero"   yaml:"storage,omitempty"`
    Flush     FlushSpec     `json:"flush,omitzero"     yaml:"flush,omitempty"`
    Retention RetentionSpec `json:"retention,omitzero" yaml:"retention,omitempty"`
    Task      TaskSpec      `json:"task,omitzero"      yaml:"task,omitempty"`
}

type StorageSpec struct {
    Durability   *Durability `json:"durability,omitempty"   yaml:"durability,omitempty"`
    Encryption   *Encryption `json:"encryption,omitempty"   yaml:"encryption,omitempty"`
    Compaction   *Compaction `json:"compaction,omitempty"   yaml:"compaction,omitempty"`
    MaxSegment   *int64      `json:"max_segment,omitempty"  yaml:"max_segment,omitempty"`
    MaxSegmentAge *Duration  `json:"max_segment_age,omitempty" yaml:"max_segment_age,omitempty"`
    MaxPayload   *int64      `json:"max_payload,omitempty"  yaml:"max_payload,omitempty"`  // optional; derived if nil
    MaxCache     *int64      `json:"max_cache,omitempty"    yaml:"max_cache,omitempty"`    // optional; derived if nil
}

type FlushSpec struct {
    MaxBytes *int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
    MaxWait  *Duration `json:"max_wait,omitempty"  yaml:"max_wait,omitempty"`
}

type RetentionSpec struct {
    MaxAge   *Duration `json:"max_age,omitempty"   yaml:"max_age,omitempty"`
    MaxBytes *int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
}

type TaskSpec struct {
    Lease          *Duration `json:"lease,omitempty"            yaml:"lease,omitempty"`
    MaxAttempts    *uint32   `json:"max_attempts,omitempty"     yaml:"max_attempts,omitempty"`
    MaxInFlight    *uint32   `json:"max_in_flight,omitempty"    yaml:"max_in_flight,omitempty"`
    MaxDeadLetters *uint32   `json:"max_dead_letters,omitempty" yaml:"max_dead_letters,omitempty"` // optional; derived if nil
}

type Session struct {
    Target         string        `yaml:"target"`
    DataDir        string        `yaml:"data_dir"`
    MaxConnections int           `yaml:"max_connections"`
    TLS            TLSConfig     `yaml:"tls"`
    Auth           AuthConfig    `yaml:"auth"`
    Metrics        MetricsConfig `yaml:"metrics"`
}

type TLSConfig struct {
    Enabled  bool   `yaml:"enabled"`
    Cert     string `yaml:"cert"`
    Key      string `yaml:"key"`
    ClientCA string `yaml:"client_ca"`
}

type AuthConfig struct {
    Method    string `yaml:"method"`     // none | none-insecure | token | mtls
    Token     string `yaml:"token"`
    TokenFile string `yaml:"token_file"`
}

type MetricsConfig struct {
    Enabled            bool     `yaml:"enabled"`
    Interval           Duration `yaml:"interval"`
    History            int      `yaml:"history"`
    Listen             string   `yaml:"listen"`
    PerQueuePrometheus bool     `yaml:"per_queue_prometheus"`
    Histograms         bool     `yaml:"histograms"`
}
```

Delete the old `EngineSpec` entirely. `Merge`, `Effective`, `Field`, `Equal` are updated to the new sub-structs.

### 1.3 New table rows — `internal/spec/table.go`

Replace the spec-layer block. Final canonical keys, in table order:

| Key | Path | Kind | Mutability |
|---|---|---|---|
| `durability` | `storage.durability` | enum | create_only |
| `encryption` | `storage.encryption` | enum | create_only |
| `compaction` | `storage.compaction` | enum | create_only |
| `max_segment` | `storage.max_segment` | bytes | create_only |
| `max_segment_age` | `storage.max_segment_age` | duration | mutable |
| `max_payload` | `storage.max_payload` | bytes | create_only |
| `max_cache` | `storage.max_cache` | bytes | mutable |
| `max_flush_bytes` | `flush.max_bytes` | bytes | mutable |
| `max_flush_wait` | `flush.max_wait` | duration | mutable |
| `max_retention_age` | `retention.max_age` | duration | mutable |
| `max_retention_bytes` | `retention.max_bytes` | bytes | mutable |
| `lease` | `task.lease` | duration | mutable |
| `max_attempts` | `task.max_attempts` | uint32 | mutable |
| `max_in_flight` | `task.max_in_flight` | uint32 | mutable |
| `max_dead_letters` | `task.max_dead_letters` | uint32 | mutable |

Add a new `KindEnum` kind. `Value` gains `Enum()` returning the string; `ParseValue(KindEnum, text)` validates against a per-key allowed set (the `Param` carries an `Allowed []string` field).

Session rows keep the same keys except `max_conns` → `max_connections`, and each session `Param` gains a `Path` reflecting nesting (`server.tls.enabled`, `server.auth.method`, `server.metrics.interval`, etc.). Flag names stay flat/dashed: `--tls-enabled`, `--auth-method`, `--metrics-interval`, `--max-connections`.

Per-call rows: `lease` (renamed from `visibility`), `max_attempts`, `scheduled_at`, `retry_after`.

`EnvName`, `FlagName`, `OptionName` regenerate from the new keys via the existing helpers. Add `initialisms["tls"]="TLS"`, `["tls"]` already covered, `["ca"]="CA"`, `["dlq"]` is retired.

### 1.4 Derivation — `internal/spec/derive.go` (new file)

```go
const RecordOverhead = 36

// Resolve fills every derived field on s that was left nil.
// It does not validate; call ValidateCreate/ValidateUpdate after.
func Resolve(s Spec) Spec
```

Rules, applied in order:

1. `storage.max_payload` nil → `min(1 MiB, storage.max_segment - RecordOverhead)`.
2. `storage.max_cache` nil → `min(32 MiB, storage.max_segment * 4)`.
3. `task.max_dead_letters` nil → `task.max_in_flight`.
4. `flush.max_wait` nil → `10ms`.
5. `flush.max_bytes` nil → `1 MiB`.
6. `storage.max_segment` nil → `64 MiB`.
7. `storage.max_segment_age` nil → `1h`.
8. `storage.durability` nil → `"sync"`.
9. `storage.encryption` nil → `"none"`.
10. `storage.compaction` nil → `"none"`.
11. `task.lease` nil → `30s`.
12. `task.max_attempts` nil → `5`.
13. `task.max_in_flight` nil → `100000`.
14. `retention.max_age` nil → `0`.
15. `retention.max_bytes` nil → `0`.

Add an unexported helper `FlushLinger(s Spec) time.Duration` returning `max(0, max_flush_wait / 8)`. Not a table row.

`Builtin()` returns the resolved full spec. `Effective(queue, session, builtin)` = `queue.Merge(session).Merge(builtin).Resolve()`.

### 1.5 Validation — `internal/spec/validate.go`

Replace pair rules with:

- All `max_*` bytes fields must be `> 0`.
- All `max_*` durations fields must be `> 0` except retention which allows `0` for unlimited.
- Enum fields must be one of their declared constants.
- **Explicit-pair conflict:** if `storage.max_payload != nil && storage.max_segment != nil`, require `*max_payload <= *max_segment - RecordOverhead`. Error names both keys.
- **Explicit-pair conflict:** if `storage.max_cache != nil && storage.max_segment != nil`, require `*max_cache >= *max_segment`. Error names both keys.
- `flush.max_wait` must be `>= 100µs`.
- `task.max_dead_letters > 0` if explicitly set.

`ValidateUpdate(patch)` rejects create-only keys (`durability`, `encryption`, `compaction`, `max_segment`, `max_payload`) by iterating the table.

`ValidateSession` reads the nested `TLS`, `Auth`, `Metrics` fields.

### 1.6 Codec — `internal/spec/codec.go`

Bump `CurrentSpecVersion` to `4`. Everything else unchanged.

### Acceptance for Phase 1

- `go test ./internal/spec/...` passes.
- `TestTableSelfCheck`, `TestTableMatchesSpecFields`, `TestTableMatchesSessionFields` pass with the new shape.
- `TestSettersAndGettersAgree`, `TestSessionSettersLandOnTheirOwnField` pass.
- New test `TestResolveDerivesEveryOptional` pins all 15 derivation rules.
- New test `TestValidateRejectsExplicitPairConflict` covers both pair rules with both `nil`/non-`nil` combinations.

**Do not touch any package outside `internal/spec` in this phase.**

---

## Phase 2 — Engine and node consume the new spec

This phase makes the new shape actually drive behavior. The public surfaces still use the old names during this phase; only `engine`, `node`, and `config` are wired to the new struct.

### 2.1 Engine `Config` — `internal/engine/config.go`

Replace `Config` fields:

```go
type Config struct {
    Durability   spec.Durability  // import spec
    Encryption   spec.Encryption
    Compaction   spec.Compaction
    MaxSegment       int64
    MaxSegmentAge    time.Duration
    MaxPayload       int64
    MaxCache         int64
    MaxFlushBytes    int64
    MaxFlushWait     time.Duration
    FlushLinger      time.Duration   // derived, passed in
    CompactionPolicy CompactionPolicy
}
```

`withDefaults()` is deleted; `spec.Resolve` owns defaults now. `engine` gains a dependency on `spec`. Update `.go-arch-lint.yml`: `engine: { mayDependOn: [spec, metrics] }`.

### 2.2 WAL byte batching — `internal/engine/wal.go`

This is the real change. Replace the count-based sequencing.

- `pending []pendingEntry` stays.
- Track `pendingBytes int64` alongside `pushed int64` (bytes) and `published int64` (bytes). Rename `pushed`/`published` to `pushedBytes`/`publishedBytes` to make the unit explicit.
- `push(rec, pos)` returns `seq int64` = byte offset *after* this record. Increments `pushedBytes` by `len(rec)`.
- `pushBatch(recs, positions)` returns `seq` = `pushedBytes` after all records added.
- `wait(seq)` waits until `publishedBytes >= seq`.
- `wake` fires when `pendingBytes >= cfg.MaxFlushBytes`.
- `asyncChan` capacity becomes `max(1, cfg.MaxFlushBytes / 4096)`. Add a comment naming this as the byte-to-slot approximation.
- `adaptiveFlush` sleeps `cfg.FlushLinger` when `pendingBytes < cfg.MaxFlushBytes`.
- `UpdateConfig` accepts `MaxFlushBytes`, `MaxFlushWait`, `FlushLinger`.

Update `wal_test.go`: `TestBackpressureBlocksProducer` fills the channel by pushing `MaxFlushBytes / 4096` records, not `BatchSize` records. `TestWALBatch` uses byte sizes.

### 2.3 Byte-bounded ring cache — `internal/engine/cache.go`

Replace fixed-capacity slots:

```go
type ringCache struct {
    mu       sync.Mutex
    slots    []Record
    mask     uint64
    maxBytes int64
    curBytes int64
}
```

- `newRingCache(maxBytes int64)` sizes `slots` at `nextPow2(maxBytes / 256)`, min 64, max 65536. The cap is enforced by `maxBytes`, not by slot count.
- `Put` adds `len(msg.Payload) + recordOverhead` to `curBytes`; if `curBytes > maxBytes`, evict from the head in FIFO order until under cap.
- `EvictBefore` and `Evict` subtract evicted record sizes.

Update `openQueue` to pass `cfg.MaxCache`. Update cache tests.

### 2.4 Batch staging — `internal/engine/append.go`

In `AppendBatch`, the staged batch accumulates records until either:
- `staged.totalLen + recLen > cfg.MaxFlushBytes`, or
- the segment would roll.

Replace the `len(staged.recs) >= q.cfg.BatchSize` check. Delete all references to `q.cfg.BatchSize`.

### 2.5 Queue config application — `internal/engine/queue.go`

`ApplyConfig(Config)` accepts the new fields. Create-only fields (`MaxSegment`, `MaxPayload`, `Durability`, `Encryption`, `Compaction`) are read from the new config but only used at open; a live `ApplyConfig` ignoring them is correct.

`MaxAppendPayloadBytes()` returns `min(cfg.MaxPayload, cfg.MaxSegment - RecordOverhead)`.

### 2.6 Node translation — `internal/node/node.go`

Rewrite `toEngineConfig`, `taskDefaultsFor`, `policyFor` to read the new spec fields. Add:

```go
func toEngineConfig(s spec.Spec) engine.Config {
    return engine.Config{
        Durability:    deref(s.Storage.Durability, spec.DurabilitySync),
        Encryption:    deref(s.Storage.Encryption, spec.EncryptionNone),
        Compaction:    deref(s.Storage.Compaction, spec.CompactionNone),
        MaxSegment:    deref(s.Storage.MaxSegment, 64<<20),
        MaxSegmentAge: s.Storage.MaxSegmentAge.Duration(),
        MaxPayload:    deref(s.Storage.MaxPayload, 1<<20),
        MaxCache:      deref(s.Storage.MaxCache, 32<<20),
        MaxFlushBytes: deref(s.Flush.MaxBytes, 1<<20),
        MaxFlushWait:  s.Flush.MaxWait.Duration(),
        FlushLinger:   spec.FlushLinger(s),
    }
}
```

The compaction boolean in `createQueue` becomes `eff.Storage.Compaction == spec.CompactionKeys`.

### 2.7 Config loader — `internal/config/config.go`

Rewrite `defaultConfigYAML` with the new shape. Update `legacyYAMLKeys` to map every old key (`batch_size`, `max_interval`, `min_interval`, `segment_size`, `max_message_bytes`, `segment_age`, `fsync`, `encrypted`, `compaction`, `max_conns`, `max_dlq`, `default_visibility`, `default_max_attempts`, `retention_age`, `retention_bytes`) to its new key with a hard error message.

Update `legacyEnvVars` similarly: `MIKYU_BATCH_SIZE` → `MIKYU_MAX_FLUSH_BYTES` (noting the unit change in the message), `MIKYU_FSYNC` → `MIKYU_DURABILITY`, etc. Add new env var names for the new keys via `envName`.

`applyEnvOverrides` walks the new table; session rows now write into nested structs via `ApplySession`, which the table already supports.

### Acceptance for Phase 2

- `go test ./internal/engine/... ./internal/node/... ./internal/config/...` passes.
- New test `TestWALByteBackpressure` pushes records until the byte cap is hit and asserts the next push blocks until cancel.
- New test `TestRingCacheByteCap` inserts records past `maxBytes` and asserts `curBytes <= maxBytes`.
- New test `TestEngineConfigFromSpec` covers each derivation pass-through.
- `TestDirectoryFsyncSites` still passes (durability semantics unchanged).

**Do not touch `internal/client`, `internal/ui`, `internal/daemon`, or the root `mikyu` package in this phase.**

---

## Phase 3 — Public surfaces

Now propagate the new vocabulary. Nothing outside the packages touched here.

### 3.1 Client options — `internal/client/options.go`

Rename every `With*` to match `spec.Table`'s `OptionName`. Enum fields get a value parameter:

```go
func WithDurability(d spec.Durability) QueueOption
func WithEncryption(e spec.Encryption) QueueOption
func WithCompaction(c spec.Compaction) QueueOption
func WithMaxSegment(n int64) QueueOption
func WithMaxSegmentAge(d time.Duration) QueueOption
func WithMaxPayload(n int64) QueueOption
func WithMaxCache(n int64) QueueOption
func WithMaxFlushBytes(n int64) QueueOption
func WithMaxFlushWait(d time.Duration) QueueOption
func WithMaxRetentionAge(d time.Duration) QueueOption
func WithMaxRetentionBytes(n int64) QueueOption
func WithLease(d time.Duration) QueueOption
func WithMaxAttempts(n uint32) QueueOption
func WithMaxInFlight(n uint32) QueueOption
func WithMaxDeadLetters(n uint32) QueueOption
```

Session options: `WithMaxConnections(n int)`, `WithTLS(cfg TLSConfig)` — or keep flat `WithTLSEnabled`, `WithTLSCertFile`, etc. **Choose flat** to match the daemon CLI flags and keep `SessionConfig` flat internally. `SessionConfig.TLSEnabled` etc. survive; only the public YAML is nested.

`SessionConfig` gains `Durability`, `Encryption`, `Compaction` fields for the spec defaults.

### 3.2 Remote client — `internal/client/remote.go`

Update `writeCreateFrame`, `writeSpecFrame`, and the `EncodeSpec`/`DecodeSpec` calls to the new version. No wire changes.

### 3.3 Server auth — `internal/server/auth.go`

`NewAuthMode(spec.Session)` reads `sess.Auth.Method`, `sess.Auth.Token`, `sess.Auth.TokenFile`. `sess.TLS.Enabled` and `sess.TLS.ClientCA` replace their flat counterparts.

### 3.4 Daemon — `internal/daemon/daemon.go`

- Flag registration: rename flags to match the new table. Session flag group gains `--max-connections`, `--tls-enabled`, `--tls-cert`, `--tls-key`, `--tls-client-ca`, `--auth-method`, `--auth-token-file`, `--auth-token` (rejected), `--metrics-enabled`, `--metrics-interval`, `--metrics-history`, `--metrics-listen`, `--metrics-per-queue-prometheus`, `--metrics-histograms`. Spec-default flags: `--durability`, `--encryption`, `--compaction`, `--max-segment`, `--max-segment-age`, `--max-payload`, `--max-cache`, `--max-flush-bytes`, `--max-flush-wait`, `--max-retention-age`, `--max-retention-bytes`, `--lease`, `--max-attempts`, `--max-in-flight`, `--max-dead-letters`.
- Enum flags use a `flag.Value` that rejects unknown strings. Add a small `enumValue` type in `flag.go`; it holds the `Param` and calls `ParseValue(KindEnum, text)`.
- The fail-closed checks read `cfg.Server.Auth.Method`, `cfg.Server.TLS.Enabled`, `cfg.Server.TLS.ClientCA`.
- `logEffectiveConfig` logs the new fields.

### 3.5 CLI / REPL flag sets — `internal/ui/command/flag.go`

`SessionFlags`, `QueueFlagSet`, `QueuePatchFlagSet`, `EnqueueFlagSet`, `NackFlagSet` all iterate `spec.Params(...)` and read `FlagName`. They pick up the new names automatically. Fix only the `sessionValue.String()` switch, which names every session key explicitly. Replace with a table-driven switch that reads the appropriate sub-field from `spec.Session` via the new `Param.Path`.

`EnqueueFlagSet` currently special-cases `visibility`, `max_attempts`, `scheduled_at`. Rename the first to `lease`, keep the other two.

### 3.6 Facade — `mikyu/spec.go`, `mikyu/options.go`, `mikyu/mikyu.go`

Re-export the enums:

```go
type Durability = spec.Durability
type Encryption = spec.Encryption
type Compaction = spec.Compaction

const (
    DurabilitySync  = spec.DurabilitySync
    DurabilityAsync = spec.DurabilityAsync
    EncryptionNone  = spec.EncryptionNone
    EncryptionOS    = spec.EncryptionOS
    CompactionNone  = spec.CompactionNone
    CompactionKeys  = spec.CompactionKeys
)
```

`mikyu/options.go` renames every wrapper. The re-export list in `mikyu/mikyu.go` is unchanged except `ErrImmutableField` etc. still hold.

### 3.7 Config file default doc

`internal/config/config.go`'s `defaultConfigYAML` becomes the new YAML shape with comments. Update `config_test.go`'s `TestConfigEnvOverridesFromTable` to walk the new session fields via `Param.Path`.

### Acceptance for Phase 3

- `go build ./...` succeeds.
- `go test ./internal/client/... ./internal/daemon/... ./internal/ui/... ./...` (root) passes.
- `TestParitySurfaces` passes: every new table row is reachable from SDK option, CLI flag, env, YAML, queues.json, and the wire.
- `TestSchemaOverWire` passes with `SpecVersion = 4`.
- All golden help pages regenerate (`-update` flag then review diff).

---

## Phase 4 — Retire old names and lock the surface

This phase makes regressions impossible.

### 4.1 Naming-law vocabulary — `mikyu/surface_test.go`

Extend `bannedParts` with the retired words: `Produce`, `Publish`, `Pop`, `Stats`, `Info`, `Visibility`, `BatchSize`, `SegmentSize`, `MaxMessageBytes`, `MaxDLQ`, `MaxConns`, `Fsync`, `Encrypted`, `MinInterval`, `MaxInterval`. Extend the `banned` verb list accordingly.

Extend `TestNamingLaw`'s `compositionHelpers` set to include `WithDurability`, `WithEncryption`, `WithCompaction` — these are the only new option names not directly derived from a single table row's `OptionName`.

### 4.2 Legacy audit — `internal/audit/legacy_test.go`

Add to `bannedIdentifiers`: `EngineSpec`, `RetentionAge`, `RetentionBytes`, `DefaultVisibility`, `DefaultMaxAttempts`, `MaxDLQ`, `MaxConns`, `BatchSize`, `MaxInterval`, `MinInterval`, `SegmentSize`, `SegmentAge`, `MaxMessageBytes`, `VisibilityTimeout`. Add `bannedOpPattern` cases for `OpStats`, `OpInfo`.

Add to `legacyYAMLKeys` and `legacyEnvVars` in `internal/config/config.go`: the full old key set with a message naming the replacement. `TestConfigOldKeyHardError` and `TestLegacyEnvVarsHardError` become table-driven over both maps.

### 4.3 Describe output — `internal/node/node.go`

`QueueDescription` gains a `Derived spec.Spec` field filled by calling `spec.Resolve` on the stored spec. `DescribeQueue` populates it. `internal/client/types.go`'s `QueueDescription` mirrors the field. Update `internal/ui/command/queue.go`'s `Describe` and the CLI `handleDescribe` to print three sections: `spec` (what the user set), `derived` (what was derived), `effective` (fully resolved). Update `internal/ui/repl/repl.go`'s `handleDescribe` similarly.

### 4.4 Golden files — `internal/ui/command/testdata/*.txt`

Regenerate with `-update`, then manually verify:
- Index pages list no `--flag` lines (existing law).
- `create` detail lists every spec-layer flag.
- `update` detail omits create-only flags: `--durability`, `--encryption`, `--compaction`, `--max-segment`, `--max-payload`.

### 4.5 Delete dead code

- `internal/engine/config.go`: remove `withDefaults`.
- `internal/spec/spec.go`: remove `EngineSpec`, `Merge`'s engine branch.
- `internal/spec/table.go`: remove `buildTable`'s `specSetters`/`specGetters` entries for the retired keys; add entries for the new ones.
- Search for `BatchSize`, `SegmentSize`, `MaxMessageBytes`, `MaxDLQ`, `MaxConns`, `Visibility`, `DefaultVisibility` across the repo and delete or rename each occurrence.

### Acceptance for Phase 4

- `go test ./...` green.
- `TestNoLegacyNames`, `TestInternalCommentStyle`, `TestNamingLaw`, `TestParitySurfaces`, `TestSchemaIsTotalAndStable` all pass.
- Manual `describe --json` on a queue shows `spec`, `derived`, and `effective` as distinct objects.
- `mikyu daemon --help` lists every new flag with its enum values or byte unit in the doc string.

---

## Cross-phase rules

- **Hard cut, no aliases.** Every old name produces a validation error naming the replacement. The legacy maps in `internal/config` are the only place an old key is recognized, and only to reject it.
- **One phase, one `go test ./...`.** Each phase ends with the full suite green. Do not start a phase with a red tree.
- **Table is the source of truth.** If a name appears outside `spec.Table`, `spec.Spec`, or `spec.Session`, it is a bug.
- **Derived values never appear as table rows.** They live in `Resolve` and are exposed only through `QueueDescription.Derived`.
- **Every `max_*` is directly settable.** Derivation provides a default; it never forbids an override.