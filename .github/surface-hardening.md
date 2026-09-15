# Mikyu Surface Hardening — Four-Phase Plan

The goal is a public surface that delivers zero-config adoption, semantic parity across embedded, local daemon, and remote daemon, and a schema that translates cleanly to every language — without touching any hot path. The plan is sequenced so each phase is a self-contained commit that compiles, passes tests, and leaves the repo shippable.

The internal cleanliness constraint is respected throughout: no new files unless they are a coherent unit, no code moved across packages, no new abstractions added where an existing one suffices.

---

## Phase 1 — Correctness & Error Hygiene

**Goal.** Reject configurations that are invalid once resolved, stop leaking CLI advice into SDK error strings, and export every error sentinel the SDK can produce.

**Why first.** Pure bug fix, no surface change. Later phases depend on reliable validation and complete sentinel exports.

### Changes

**`internal/node/node.go`**

In `CreateQueue`, after computing `eff := s.Effective(n.defaults, spec.Builtin())` and before `n.mgr.CreateQueue`:

```go
if err := spec.ValidateCreate(eff); err != nil {
    return err
}
```

In `UpdateQueue`, after `merged := patch.Merge(current)` and before `n.registry.Queues[name] = merged`:

```go
eff := merged.Effective(n.defaults, spec.Builtin())
if err := spec.ValidateCreate(eff); err != nil {
    return err
}
```

`ValidateUpdate` on the patch is unchanged; `ValidateCreate` on the merged result catches pair conflicts that inherited the other side of the pair.

**`internal/config/config.go`**

In `Load`, after `applyEnvOverrides(&cfg)` and before the final return:

```go
if err := spec.ValidateSession(cfg.Server); err != nil {
    return nil, "", err
}
resolved := cfg.Defaults.Effective(spec.Spec{}, spec.Builtin())
if err := spec.ValidateCreate(resolved); err != nil {
    return nil, "", err
}
```

In `resolveOrBootstrapPath`, when writing the bootstrapped file fails, append the fix to the error:

```go
if err := os.WriteFile(path, []byte(defaultConfigYAML), 0o644); err != nil {
    return "", false, fmt.Errorf(
        "unable to bootstrap config file at %s: %w (pass --config <path> to specify an alternate location)",
        path, err,
    )
}
```

**`internal/node/lock.go`**

Change `ErrLocked`'s message to `"data directory is locked by another process"`. Drop the `— use --target to connect to it` suffix. That hint is CLI advice and does not belong in an SDK sentinel.

**`internal/ui/cli/cli.go`**

Wherever `Run` returns an error that is `mikyu.ErrLocked`, append the CLI-specific hint at the presentation layer:

```go
if errors.Is(err, mikyu.ErrLocked) {
    return fmt.Errorf("%w (use --target to connect to the running daemon)", err)
}
```

**`mikyu/mikyu.go`**

In the exported `var (...)` block, add:

```go
ErrNoData         = engine.ErrNoData
ErrRecordTooLarge = engine.ErrRecordTooLarge
ErrProtocol       = client.ErrProtocol
```

### Tests

**New** — `internal/node/node_test.go`:

- `TestCreateQueueRejectsInheritedConflict`: acquire a node with session default `segment_size = 1<<20`; create a queue with `max_message_bytes = 2<<20`; `ErrorIs(err, spec.ErrInvalidConfig)` and the message names both keys.
- `TestUpdateQueueRejectsInheritedConflict`: create a queue with `segment_size = 1<<20`; update with `max_message_bytes = 2<<20`; same assertion.
- `TestCreateQueueRejectsInvalidSessionDefault`: acquire with session default `batch_size = 0`; every `CreateQueue` fails with `ErrInvalidConfig`.

**New** — `internal/config/config_test.go`:

- `TestLoadRejectsInvalidSession`: YAML `server: { metrics_enabled: true, metrics_interval: "0s" }` → error.
- `TestLoadRejectsInvalidDefaults`: YAML `defaults: { engine: { max_message_bytes: 2000000, segment_size: 1000000 } }` → error naming both keys.
- `TestBootstrapWriteFailureHintsConfigFlag`: simulate an unwritable binary directory; assert the error mentions `--config`.

**New** — `mikyu/mikyu_test.go`:

- `TestOpenRejectsInvalidSessionSpec`: `Open(t.TempDir(), WithSessionSpec(Spec{Engine: EngineSpec{BatchSize: new(0)}}))` → `ErrorIs(err, mikyu.ErrInvalidConfig)`.

**Updated**:

- `mikyu/surface_test.go::TestSentinelUniqueness`: add `ErrNoData`, `ErrRecordTooLarge`, `ErrProtocol`.
- `internal/node/lock_test.go::TestLockedDirRejectsSecondProcess`: add `assert.NotContains(t, err.Error(), "--target")`.

### Commit

```
fix(spec): validate effective specs, neutralize locked error, export remaining sentinels
```

---

## Phase 2 — Public Schema Surface

**Goal.** Publish `spec.Table` and the spec codec on the `mikyu` facade, make the schema reachable over the wire for non-Go SDKs, and document the mental model that ties the two on-disk formats together.

**Why now.** Phase 3 filters CLI flags by scope and Phase 4 adds an interface method; both read from the schema. Publishing first lets every later phase consume the public surface rather than an internal one.

### Changes

**`internal/spec/table.go`**

Add JSON tags to `Param` and exclude the two function fields. Also give `Kind`, `Layer`, and `Mutability` string-valued JSON forms, so the wire shape is language-neutral:

```go
type Param struct {
    Key        string     `json:"key"`
    Path       string     `json:"path"`
    Kind       Kind       `json:"kind"`
    Layer      Layer      `json:"layer"`
    Mutability Mutability `json:"mutability"`
    EnvName    string     `json:"env_name"`
    FlagName   string     `json:"flag_name"`
    OptionName string     `json:"option_name"`

    Apply        func(*Spec, Value)     `json:"-"`
    ApplySession func(*Session, Value)  `json:"-"`
}

func (k Kind) MarshalJSON() ([]byte, error)       { return json.Marshal(k.String()) }
func (l Layer) MarshalJSON() ([]byte, error)      { return json.Marshal(l.String()) }
func (m Mutability) MarshalJSON() ([]byte, error) { return json.Marshal(m.String()) }
```

`Mutability` needs a `String()` method; add one mirroring `Kind.String()` and `Layer.String()`:

```go
func (m Mutability) String() string {
    switch m {
    case MutMutable:      return "mutable"
    case MutCreateOnly:   return "create_only"
    case MutNotApplicable:return "not_applicable"
    default:              return "unspecified"
    }
}
```

**`internal/spec/codec.go`**

Add the YAML encoder symmetric with `EncodeSpec`:

```go
// EncodeSpecYAML renders s as sparse YAML: unset fields are absent.
func EncodeSpecYAML(s Spec) ([]byte, error) {
    data, err := yaml.Marshal(s)
    if err != nil {
        return nil, fmt.Errorf("spec: encode yaml: %w", err)
    }
    return data, nil
}
```

**`internal/transport/protocol/opcodes.go`**

Add to the admin block:

```go
OpSchema byte = 0x08 // returns the spec parameter table as JSON
```

**`internal/server/handlers_admin.go`**

Add:

```go
// handleSchema serves OpSchema: a u16 specVersion followed by the JSON-encoded
// parameter table, the same envelope as OpDescribeQueue and OpMetrics.
func (s *Server) handleSchema() (byte, []byte) {
    data, err := json.Marshal(spec.Table)
    if err != nil {
        return opErr(protocol.ErrCodeUnknown, err.Error())
    }
    var buf protocol.Buffer
    buf.WriteU16(spec.CurrentSpecVersion)
    buf.WriteBytes(data)
    return protocol.OpSchema, buf.Bytes()
}
```

**`internal/server/dispatch.go`**

Add to the admin block:

```go
case protocol.OpSchema:
    respOp, respBody = s.handleSchema()
```

**`internal/client/embedded.go`**

Add:

```go
// Schema returns a fresh copy of the node's parameter table.
func (c *embeddedClient) Schema(ctx context.Context) ([]spec.Param, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    out := make([]spec.Param, len(spec.Table))
    copy(out, spec.Table)
    return out, nil
}
```

**`internal/client/remote.go`**

Add:

```go
// Schema fetches the server's parameter table over OpSchema.
func (c *remoteClient) Schema(ctx context.Context) ([]spec.Param, error) {
    resp, err := c.exchange(ctx, protocol.OpSchema, nil)
    if err != nil {
        return nil, err
    }
    r := protocol.NewReader(resp)
    version, err := r.ReadU16()
    if err != nil {
        return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
    }
    if err := spec.CheckSpecVersion(version); err != nil {
        return nil, err
    }
    data, err := r.ReadBytes()
    if err != nil {
        return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
    }
    if len(data) > spec.MaxSpecPayload {
        return nil, fmt.Errorf("%w: schema payload is %d bytes, limit is %d", ErrProtocol, len(data), spec.MaxSpecPayload)
    }
    var params []spec.Param
    if err := json.Unmarshal(data, &params); err != nil {
        return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
    }
    return params, nil
}
```

**`mikyu/spec.go`** (new file)

```go
package mikyu

import "github.com/dmunish/mikyu/internal/spec"

type (
    Param   = spec.Param
    Kind    = spec.Kind
    Layer   = spec.Layer
    Session = spec.Session
)

const (
    KindInt      = spec.KindInt
    KindUint32   = spec.KindUint32
    KindDuration = spec.KindDuration
    KindBytes    = spec.KindBytes
    KindBool     = spec.KindBool
    KindString   = spec.KindString
    KindTime     = spec.KindTime

    LayerCall    = spec.LayerCall
    LayerSpec    = spec.LayerSpec
    LayerSession = spec.LayerSession
)

// Schema returns a fresh copy of the parameter table.
func Schema() []Param {
    out := make([]Param, len(spec.Table))
    copy(out, spec.Table)
    return out
}

// Lookup finds a parameter by canonical key.
func Lookup(key string) (Param, bool) { return spec.Lookup(key) }

// EncodeSpec renders a sparse spec as JSON.
func EncodeSpec(s Spec) ([]byte, error) { return spec.EncodeSpec(s) }

// EncodeSpecYAML renders a sparse spec as YAML.
func EncodeSpecYAML(s Spec) ([]byte, error) { return spec.EncodeSpecYAML(s) }

// DecodeSpec parses JSON, rejecting unknown keys and oversized payloads.
func DecodeSpec(data []byte) (Spec, error) { return spec.DecodeSpec(data) }

// DecodeSpecYAML parses a sparse spec from YAML.
func DecodeSpecYAML(data []byte) (Spec, error) { return spec.DecodeSpecYAML(data) }

// ValidateCreate checks a proposal for create-time use.
func ValidateCreate(s Spec) error { return spec.ValidateCreate(s) }

// ValidateUpdate rejects a patch that sets a create-only field.
func ValidateUpdate(patch Spec) error { return spec.ValidateUpdate(patch) }

// ValidateSession checks the process-level parameters.
func ValidateSession(s Session) error { return spec.ValidateSession(s) }

// Builtin returns the compiled-in defaults as a total spec.
func Builtin() Spec { return spec.Builtin() }

// BuiltinSession returns the compiled-in session defaults.
func BuiltinSession() Session { return spec.BuiltinSession() }
```

**`mikyu/mikyu.go`**

Add to the `Client` interface:

```go
// Schema returns the parameter table the server accepts. Embedded clients
// return spec.Table directly; remote clients fetch it over OpSchema.
Schema(ctx context.Context) ([]Param, error)
```

Update the package doc with the mental model:

```go
// Package mikyu is the public SDK for the Mikyu message queue.
//
// Mikyu separates process configuration from data:
//
//   - mikyu.yaml lives with the binary and describes how a daemon runs:
//     bind target, TLS, metrics HTTP, and the ambient session defaults new
//     queues inherit on this host. The daemon reads it; embedded clients
//     do not.
//
//   - queues.json lives inside the data directory alongside the log
//     segments, index files, offset store, and lock. It records the name
//     and sparse spec of every queue and is written only by mikyu.
//
// Copying a data directory copies everything durable. Installing a binary
// copies the process's launch configuration. They travel independently
// because they are different things.
//
// Session defaults are ambient: they belong to the node that opened the
// directory, not to the data inside it. To make a queue's behavior
// portable across hosts, set the field on the queue itself.
```

**`internal/node/node.go`**

Extend `QueueDescription`'s godoc:

```go
// QueueDescription is the full truth about one queue: its stored spec, its
// effective spec (stored over defaults over built-in), and the session
// defaults it inherits from.
//
// Session defaults are the ambient environment of the node that opened this
// directory; they are not stored with the queue. To make a queue's behavior
// portable across hosts, set the field on the queue itself. Spec shows
// exactly which fields were stored; Effective shows the resolution.
```

### Tests

**New** — `mikyu/schema_test.go`:

- `TestSchemaIsTotalAndStable`: every `Param` has non-empty `Key`, `FlagName`, `OptionName`, `EnvName`; every `Kind != KindUnknown`; every `Layer != LayerUnspecified`; `len(Schema()) == len(spec.Table)`.
- `TestLookupFindsEverySchemaKey`: for each `Schema()` entry, `Lookup(p.Key)` returns it.
- `TestSchemaCopyIsIsolated`: mutating the returned slice does not affect a second call.
- `TestEncodeDecodeJSONRoundTrip`: build a full spec from `LayerSpec` sample values, `EncodeSpec` then `DecodeSpec`, assert equal.
- `TestEncodeDecodeYAMLRoundTrip`: same through `EncodeSpecYAML` / `DecodeSpecYAML`.
- `TestJSONIsValidYAML`: `DecodeSpecYAML(jsonBytes)` succeeds — YAML 1.2 is a JSON superset, and this pins the property.
- `TestDecodeSpecRejectsUnknownKey`: `{"engine":{"bogus":1}}` → error.
- `TestValidateCreateRejectsPairConflict`: `max_message_bytes > segment_size` → error.
- `TestValidateUpdateRejectsCreateOnly`: for every `Schema()` entry with `Layer == LayerSpec && Mutability == MutCreateOnly`, `ValidateUpdate` on that single field errors.
- `TestBuiltinIsTotal`: for every `LayerSpec` parameter, `Builtin().Field(p)` returns `set == true`.
- `TestBuiltinSessionIsTotal`: `BuiltinSession()` carries non-zero `Target`, `MaxConns`, `MetricsInterval`, `MetricsHistory`.
- `TestSchemaGateParity`: every `OptionName` in `Schema()` is declared as a `With*` function on the facade.

**New** — `internal/transport/protocol/opcodes_test.go::TestOpcodeNibbleGrouping`: add `OpSchema` to the admin list.

**New** — `internal/server/handlers_test.go::TestHandleSchema`: call `handleSchema`, decode `u16` version and `bytes` payload, assert `len(params) == len(spec.Table)` and every key present.

**New** — `internal/client/wire_test.go::TestRemoteSchemaMatchesLocal`: loopback server, `Schema(ctx)`, compare key sets against `spec.Table`.

**New** — `mikyu/mikyu_test.go::TestSchemaOverWire`: embedded and remote `Schema(ctx)` responses share the same key set.

**Updated**:

- `internal/spec/spec_test.go::TestSpecTagsIdentical`: extend to walk `Param` — assert JSON tags exist on every exported field, `Apply`/`ApplySession` are `json:"-"`.
- `mikyu/surface_test.go::TestNamingLaw`: the `exportedFuncs` list now includes `Schema`, `Lookup`, `EncodeSpec`, `EncodeSpecYAML`, `DecodeSpec`, `DecodeSpecYAML`, `ValidateCreate`, `ValidateUpdate`, `ValidateSession`, `Builtin`, `BuiltinSession`. Add them to the allowlist.

### Commit

```
feat(spec): publish the parameter table and codec across embedded, wire, and package surfaces
```

---

## Phase 3 — Session Scope Split & Option Discipline

**Goal.** Stop advertising options that don't apply, stop accepting options that silently do nothing, and wire embedded metrics so `mikyu.Open(dir, WithMetricsEnabled(true))` actually starts a collector.

**Why now.** Phase 2 made the schema public. Phase 3 makes it honest.

### Changes

**`internal/spec/table.go`**

Add:

```go
// Scope says which side of a session boundary a parameter configures.
type Scope int

const (
    ScopeUnspecified Scope = iota
    ScopeClient
    ScopeServer
    ScopeBoth
)

func (s Scope) String() string {
    switch s {
    case ScopeClient: return "client"
    case ScopeServer: return "server"
    case ScopeBoth:   return "both"
    default:          return "unspecified"
    }
}

func (s Scope) MarshalJSON() ([]byte, error) { return json.Marshal(s.String()) }
```

Add `Scope Scope \`json:"scope"\`` to `Param`. Update `sessionParam` to take a scope and store it. Spec and call params get `ScopeUnspecified`.

Assign scopes in `buildTable`:

| key | scope |
|---|---|
| `target`, `data_dir`, `tls_enabled`, `tls_cert_file`, `tls_key_file` | `ScopeBoth` |
| `metrics_enabled`, `metrics_interval`, `metrics_history`, `metrics_histograms` | `ScopeBoth` |
| `max_conns` | `ScopeClient` |
| `metrics_listen`, `metrics_per_queue_prometheus` | `ScopeServer` |

Add:

```go
// ParamsInScope returns the table entries on layer whose scope is one of scopes.
func ParamsInScope(layer Layer, scopes ...Scope) []Param {
    var out []Param
    for _, p := range Table {
        if p.Layer != layer {
            continue
        }
        if slices.Contains(scopes, p.Scope) {
            out = append(out, p)
        }
    }
    return out
}
```

**`mikyu/spec.go`**

Re-export:

```go
type Scope = spec.Scope

const (
    ScopeClient = spec.ScopeClient
    ScopeServer = spec.ScopeServer
    ScopeBoth   = spec.ScopeBoth
)
```

**`internal/ui/command/flags.go::SessionFlags`**

Iterate `spec.ParamsInScope(spec.LayerSession, spec.ScopeClient, spec.ScopeBoth)` instead of `spec.Params(spec.LayerSession)`. `--dir` alias unchanged.

**`internal/ui/cli/cli.go::printUsage`**

Replace `for _, p := range spec.Params(spec.LayerSession)` with `spec.ParamsInScope(spec.LayerSession, spec.ScopeClient, spec.ScopeBoth)`.

**`internal/client/errors.go`**

Add:

```go
// ErrModeMismatch reports an option that does not apply in this client mode.
var ErrModeMismatch = errors.New("option does not apply in this client mode")
```

**`internal/client/options.go`**

Add:

```go
// metricsConfig builds the node collector settings this session requests.
func (c SessionConfig) metricsConfig() node.MetricsConfig {
    return node.MetricsConfig{
        Enabled:    c.MetricsEnabled,
        Interval:   c.MetricsInterval,
        History:    c.MetricsHistory,
        Histograms: c.MetricsHistograms,
    }
}
```

Update the godoc on each `WithMetrics*` to say "embedded clients only; a remote client cannot configure the server's collector."

**`internal/client/embedded.go::NewEmbedded`**

At the top, before touching `dir`:

```go
if cfg.Target != "" {
    return nil, fmt.Errorf("%w: target is for remote clients, use Dial", ErrModeMismatch)
}
```

Change the node acquisition:

```go
n, release, err := node.Acquire(dir, cfg.Spec, node.WithMetrics(cfg.metricsConfig()))
```

**`internal/client/remote.go::NewRemote`**

At the top:

```go
if cfg.DataDir != "" {
    return nil, fmt.Errorf("%w: data_dir is for embedded clients, use Open", ErrModeMismatch)
}
```

**`internal/daemon/daemon.go::resolveConfig`**

Remove `maxConns := fs.Int("max-conns", 0, ...)`, its `fs.Visit` case, and the `--max-conns` line from the usage text.

**`mikyu/mikyu.go`**

Add `ErrModeMismatch = client.ErrModeMismatch` to the exported error block.

### Tests

**New** — `internal/spec/table_test.go`:

- `TestSessionScopesAreAssigned`: every `LayerSession` parameter has non-zero `Scope`.
- `TestSessionScopeAssignments`: table-driven, asserts the exact scope for each of the twelve session keys.
- `TestParamsInScope`: `ParamsInScope(LayerSession, ScopeClient)` returns exactly `max_conns`; `ScopeServer` returns the three metrics-HTTP keys; `ScopeBoth` returns the rest.

**New** — `mikyu/mikyu_test.go`:

- `TestOpenRejectsTarget`: `Open(dir, WithTarget("tcp://127.0.0.1:1"))` → `ErrModeMismatch`.
- `TestDialRejectsDataDir`: `Dial(target, WithDataDir("./x"))` → `ErrModeMismatch`.
- `TestEmbeddedMetricsHonorsSession`: `Open(dir, WithMetricsEnabled(false))` → `Metrics(ctx).Disabled == true`. `Open(dir2, WithMetricsEnabled(true), WithMetricsInterval(time.Hour), WithMetricsHistory(4))` → `Disabled == false`, `Runtime.Goroutines > 0`.
- `TestEmbeddedMetricsHistoryHonored`: `WithMetricsHistory(4)` and a 20ms interval; after 100ms, `len(Collector().History()) <= 4`.

**New** — `internal/client/wire_test.go`: `TestEmbeddedRejectsTarget`, `TestRemoteRejectsDataDir`.

**Updated**:

- `internal/spec/spec_test.go::TestTableSelfCheck`: add a clause asserting `Scope` is non-zero for `LayerSession`.
- `internal/spec/parity_test.go::TestParitySurfaces`: session loop now asserts client-scope params appear in `globalFlags` and server-scope params do not.
- `internal/daemon/daemon_config_test.go::TestDaemonHelpListsEveryFlag`: remove `"max-conns"`.
- `internal/daemon/daemon_config_test.go`: delete `"max-conns"` from `TestDaemonAllEngineFlagsReachConfig`'s args and the corresponding `assert`.
- `mikyu/surface_test.go::TestSentinelUniqueness`: add `ErrModeMismatch`.
- `mikyu/options.go`: godoc on all six `WithMetrics*` updated.

### Commit

```
feat(spec): scope session parameters, wire embedded metrics, drop daemon orphan
```

---

## Phase 4 — Mode Continuity & Semantic Parity

**Goal.** Make embedded and remote behave identically for the operations users actually run: task backoff, live metrics, and daemon lifecycle.

**Why last.** Everything before is surface. This phase changes runtime semantics and one wire format; it depends on Phase 3's scope classification and Phase 1's error exports.

### Changes

**`internal/transport/protocol/task.go`**

Document the new `OpNack` payload:

```
bytes(queue) taskRef u8 mode [u64 retryDelayNanos if mode == 1]
```

`mode == 0` means inherit: the server computes backoff from the task's actual `Attempts`. `mode == 1` carries an explicit retry delay.

**`internal/client/remote_taskqueue.go::Nack`**

Rewrite:

```go
func (q *remoteTaskQueue) Nack(ctx context.Context, ref taskqueue.TaskRef, opts ...taskqueue.NackOption) error {
    var b protocol.Buffer
    b.WriteBytes([]byte(q.name))
    protocol.EncodeTaskRef(&b, toWireRef(ref))
    if len(opts) == 0 {
        protocol.WriteMode(&b, 0)
    } else {
        var task taskqueue.Task
        if t, ok := ref.(taskqueue.Task); ok {
            task = t
        }
        protocol.WriteMode(&b, 1)
        b.WriteU64(uint64(int64(taskqueue.ResolveNackOptions(task, opts...))))
    }
    _, err := q.client.exchange(ctx, protocol.OpNack, b.Bytes())
    return err
}
```

**`internal/server/handlers_task.go::handleNack`**

Replace the current `retryRaw` read and `WithRetryAfter` construction with:

```go
mode, err := protocol.ReadMode(r)
if err != nil {
    return opErr(protocol.ErrCodeTaskInvalid, err.Error())
}
var opts []taskqueue.NackOption
if mode == 1 {
    retryRaw, err := r.ReadU64()
    if err != nil {
        return opErr(protocol.ErrCodeTaskInvalid, err.Error())
    }
    opts = append(opts, taskqueue.WithRetryAfter(time.Duration(int64(retryRaw))))
}
nackErr := q.Nack(ctx, refFromWire(taskID, wt), opts...)
```

`TaskQueue.Nack` already computes exponential backoff from `task.Attempts` when `opts` is empty. No driver change.

**`internal/client/remote.go`**

Add `SubscribeSnapshots` honoring the client's session interval. Default to one second when unset:

```go
// SubscribeSnapshots streams live snapshots until the returned cancel runs.
// The poll interval is the session's metrics interval, or one second.
func (c *remoteClient) SubscribeSnapshots() (<-chan metrics.Snapshot, func()) {
    interval := c.cfg.MetricsInterval
    if interval <= 0 {
        interval = time.Second
    }
    ch := make(chan metrics.Snapshot, 1)
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        defer close(ch)
        t := time.NewTicker(interval)
        defer t.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-t.C:
                snap, err := c.Metrics(ctx)
                if err != nil {
                    continue
                }
                select {
                case ch <- snap:
                default:
                }
            }
        }
    }()
    return ch, cancel
}
```

**`mikyu/mikyu.go::Client`**

Add:

```go
// SubscribeSnapshots streams live snapshots until the returned cancel runs.
// Embedded clients delegate to the node's collector; remote clients poll
// OpMetrics on the session's metrics interval.
SubscribeSnapshots() (<-chan Metrics, func())
```

Both `*client.embeddedClient` (already has it) and `*client.remoteClient` satisfy the interface.

**`internal/client/options.go`**

Add `AutoStart bool` to `SessionConfig`:

```go
// WithAutoStart makes Dial spawn a daemon with the session's settings when
// the target is not already serving.
func WithAutoStart(enabled bool) Option {
    return func(c *SessionConfig) { c.AutoStart = enabled }
}
```

**`internal/daemon/daemon.go::EnsureDaemonRunning`**

Change the signature to accept a struct. Add:

```go
// SpawnOptions carries the session settings a spawned daemon must inherit.
type SpawnOptions struct {
    Target      string
    DataDir     string
    TLSEnabled  bool
    TLSCertFile string
    TLSKeyFile  string
    ExtraArgs   []string
}

// EnsureDaemonRunning checks if a daemon is alive at opts.Target and, if not,
// spawns one with the same settings.
func EnsureDaemonRunning(opts SpawnOptions) error {
    if opts.Target == "" {
        return errors.New("EnsureDaemonRunning: target is required")
    }
    if IsDaemonAlive(opts.Target) {
        return nil
    }
    executable, err := os.Executable()
    if err != nil {
        return err
    }
    args := []string{"daemon"}
    args = append(args, "--target", opts.Target)
    if opts.DataDir != "" {
        args = append(args, "--data-dir", opts.DataDir)
    }
    if opts.TLSEnabled {
        args = append(args, "--tls-enabled")
    }
    if opts.TLSCertFile != "" {
        args = append(args, "--tls-cert-file", opts.TLSCertFile)
    }
    if opts.TLSKeyFile != "" {
        args = append(args, "--tls-key-file", opts.TLSKeyFile)
    }
    args = append(args, opts.ExtraArgs...)
    cmd := exec.Command(executable, args...)
    setDetachedProcessAttr(cmd)
    if err := cmd.Start(); err != nil {
        return err
    }
    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        if IsDaemonAlive(opts.Target) {
            return nil
        }
        time.Sleep(50 * time.Millisecond)
    }
    return fmt.Errorf("daemon did not start within 3 seconds")
}
```

**`internal/ui/command/flags.go::OpenClient`**

Replace `daemon.EnsureDaemonRunning(sess.Target)` with:

```go
if err := daemon.EnsureDaemonRunning(daemon.SpawnOptions{
    Target:      sess.Target,
    DataDir:     sess.DataDir,
    TLSEnabled:  sess.TLSEnabled,
    TLSCertFile: sess.TLSCertFile,
    TLSKeyFile:  sess.TLSKeyFile,
}); err != nil {
    return nil, err
}
```

**`internal/client/remote.go::NewRemote`**

After TLS config is built and before constructing the client:

```go
if cfg.AutoStart {
    if err := daemon.EnsureDaemonRunning(daemon.SpawnOptions{
        Target:      target,
        DataDir:     cfg.DataDir,
        TLSEnabled:  cfg.TLSEnabled,
        TLSCertFile: cfg.TLSCertFile,
        TLSKeyFile:  cfg.TLSKeyFile,
    }); err != nil {
        return nil, err
    }
}
```

**`.go-arch-lint.yml`**

Add `daemon` to `client.mayDependOn`. No cycle: `daemon` does not import `client`.

```yaml
client:
  mayDependOn: [spec, engine, node, group, logqueue, taskqueue, ipc, protocol, metrics, daemon]
```

**`mikyu/options.go`**

Add:

```go
// WithAutoStart makes Dial spawn a daemon with the session's settings when
// the target is not already serving.
func WithAutoStart(enabled bool) ClientOption { return client.WithAutoStart(enabled) }
```

### Tests

**Updated** — `internal/transport/protocol/wire_test.go::TestFrameRoundTripAllOps`:

- `OpNack` payload now writes `WriteMode(&b, 1)` before the delay; the decode branch reads the mode byte first.
- Add a sub-case for `OpNack` with mode 0 and no delay bytes.

**New** — `internal/transport/protocol/task_test.go`:

- `TestNackModeInheritNoDelay`: encode with mode 0; decode yields mode 0, no trailing bytes.
- `TestNackModeExplicitDelay`: encode with mode 1 and `2s`; decode yields mode 1 and `2s`.

**New** — `internal/server/task_handlers_test.go`:

- `TestNackInheritBackoff`: enqueue with `MaxAttempts=3`; dequeue and nack twice with mode 0; assert the next `Visibility` reflects exponential backoff (`>= 1s`, `< 5m`).
- `TestNackExplicitDelay`: nack with mode 1 and `0ns`; assert the task is immediately re-dequeueable.

**New** — `internal/daemon/daemon_test.go`:

- `TestEnsureDaemonRunningPassesTarget`: free port; `EnsureDaemonRunning(SpawnOptions{Target: target, DataDir: t.TempDir()})`; `IsDaemonAlive(target)` within 3s; data dir created. Shut down with `SendShutdownSignal(target)`.
- `TestEnsureDaemonRunningRejectsEmptyTarget`: `EnsureDaemonRunning(SpawnOptions{})` → error.

**New** — `mikyu/mikyu_test.go`:

- `TestSubscribeSnapshotsEmbedded`: `Open(dir, WithMetricsEnabled(true), WithMetricsInterval(20*time.Millisecond))`; snapshot arrives within 500ms.
- `TestSubscribeSnapshotsRemote`: loopback server; snapshot arrives within 2s.
- `TestSubscribeSnapshotsRemoteHonorsInterval`: `Dial(target, WithMetricsInterval(50*time.Millisecond))`; two snapshots within 1s.
- `TestSubscribeSnapshotsCancelCloses`: cancel; channel closes.
- `TestNackInheritBackoffEmbedded`: enqueue with `WithMaxAttempts(3)`; dequeue and nack with no opts; second delivery's `Visibility` is `> 1s` out.
- `TestNackInheritBackoffRemote`: same assertions through a remote client.

**Updated**:

- `internal/client/remote_dequeue_test.go`: add `TestRemoteNackSendsModeZero` using a fake server that inspects the mode byte.

### Commit

```
feat: unify embedded and remote task, metrics, and daemon semantics
```

---

## What This Plan Does Not Do

- **C ABI implementation.** Phase 2 makes the schema JSON-native and reachable over the wire, which is the prerequisite. The `//export` surface — opaque handles, string-keyed option setters, callbacks, panic recovery — is a separate project.
- **TUI.** Phase 4's `SubscribeSnapshots` is what a TUI would consume. Building the TUI is orthogonal.
- **Server-side connection limits.** Phase 3 removes `--max-conns` rather than implementing it. A real connection limit is a server feature, not a surface fix.
- **A YAML reader for embedded mode.** Embedded is programmatic by design. Operators use YAML; developers use code; the wire uses JSON. One schema, three readers, three formats — each idiomatic to its audience.