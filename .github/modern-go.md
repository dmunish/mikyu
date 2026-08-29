# Go Changes: 1.23 → 1.27

Covers the five most recent major releases — **1.23** (Aug 2024), **1.24** (Feb 2025), **1.25** (Aug 2025), **1.26** (Feb 2026), and **1.27** (Aug 2026, current) Each item is tagged `[1.2x]` so you can trace it back. Official notes: [go.dev/doc/go1.23](https://go.dev/doc/go1.23) · [go1.24](https://go.dev/doc/go1.24) · [go1.25](https://go.dev/doc/go1.25) · [go1.26](https://go.dev/doc/go1.26) · [go1.27](https://go.dev/doc/go1.27). Blog series: [go.dev/blog](https://go.dev/blog).

---

## 1. Language changes

The language itself moved slowly and carefully over this span — most of the action is generics-adjacent.

- **`[1.23]` Range-over-func** became official: a `for...range` loop can now range over an iterator function of shape `func(func() bool)`, `func(func(K) bool)`, or `func(func(K,V) bool)`. This is *the* headline feature of the 1.23–1.24 era and underpins the new `iter` package and dozens of new iterator-returning stdlib functions (see §5). [Blog: Range Over Function Types](https://go.dev/blog/range-functions) · [`iter` package](https://go.dev/pkg/iter)

  ```go
  for line := range bytes.Lines(data) {
      fmt.Println(line)
  }
  ```

- **`[1.23]` → `[1.24]` Generic type aliases**: previewed behind `GOEXPERIMENT=aliastypeparams` in 1.23, and **fully supported by default** in 1.24 — a type alias can now be parameterized like a defined type (`type Set[T comparable] = map[T]bool`). The experiment flag was removed for 1.25.

- **`[1.25]`** No new language-affecting changes. The spec's "core types" concept was **removed in favor of explicit prose** per-feature — a spec cleanup, not a behavior change. [Blog: Core Types](https://go.dev/blog/coretypes)

- **`[1.26]`**
  - **`new` now accepts an expression**, not just a type — lets you allocate + initialize a pointer inline. Great for optional-field patterns in JSON/protobuf-style structs:
    ```go
    Age: new(yearsSince(born))   // instead of a temp var + &tmp
    ```
  - **Self-referential generic type constraints** are now legal — a generic type can refer to itself in its own type-parameter list:
    ```go
    type Adder[A Adder[A]] interface { Add(A) A }
    ```

- **`[1.27]` Generic methods** — the headline language feature of 1.27. A method declaration may now have its own type parameters, independent of the receiver's. Previously a generic operation on a type had to be a package-level function; now it can live on the type itself. `math/rand/v2.Rand` gained a generic method `N[Int intType](Int) Int` replacing/complementing the old package-level function.
  - Struct literal keys can now be **any valid field selector**, not just a top-level field name.
  - **Function type inference generalized** to apply anywhere a generic function is assigned/converted to a matching function type.

---

## 2. Toolchain & `go` command

- **`[1.23]`**
  - **Go Telemetry** launched — opt-in usage/crash statistics (`go telemetry on|local|off`). [go.dev/doc/telemetry](https://go.dev/doc/telemetry)
  - `go env -changed`, `go mod tidy -diff` (prints a diff instead of writing), `go list -m -json` gained `Sum`/`GoModSum` fields, new `godebug` directive in `go.mod`/`go.work`.

- **`[1.24]`**
  - **Tool dependency tracking**: `tool` directives in `go.mod` replace the old "blank-import in tools.go" hack. `go get -tool pkg` adds one; `go tool <name>` runs it; the `tool` meta-pattern (`go get tool`, `go install tool`) targets all of them.
  - `go build`/`go install -json` and `go test -json` now emit structured build output.
  - New `GOAUTH` env var for flexible private-module auth (`go help goauth`).
  - `go build` now stamps the **VCS version** (+`-dirty` suffix) into binaries automatically; `-buildvcs=false` opts out.
  - `GOCACHEPROG` (pluggable build/test cache backend) graduated out of GOEXPERIMENT.

- **`[1.25]`**
  - `go build -asan` now does **leak detection by default** at exit.
  - Fewer prebuilt tool binaries ship in the distribution — non-build/test tools are built on demand by `go tool`.
  - New `go.mod ignore` directive to exclude directories from package-pattern matching (still included in zips).
  - `go doc -http` spins up a local doc server in your browser.
  - `go version -m -json` prints `BuildInfo` as JSON.
  - Module root can now be a **repo subdirectory** via the `go-import` meta tag syntax.
  - New `work` package pattern matches all packages in the current module/workspace.

- **`[1.26]`**
  - **`go fix` was completely rebuilt** into the home of Go's "modernizers" — automated, safe rewrites to modern idioms/APIs, built on the same analysis framework as `go vet`. Includes a source-level inliner via `//go:fix inline` directives for library authors to script their own migrations. All the old (obsolete) fixers were deleted.
  - `go mod init` now targets an **older `go` line by default** (N-1, or N-2 for RCs) to encourage broader compatibility.
  - `cmd/doc` / `go tool doc` **deleted** — use `go doc` instead (same flags).
  - `pprof -http` web UI now **defaults to the flame graph view**.

- **`[1.27]`**
  - `go doc` supports **`pkg@version`** syntax and a new `-ex` flag to list/print executable examples with source + comments.
  - `go fix` gained new modernizers: `atomictypes`, `embedlit`, `slicesbackward`, `unsafefuncs`. The `fmtappendf` analyzer was dropped; `waitgroup` renamed to `waitgroupgo`.
  - `go mod tidy` **auto-consolidates require blocks** into a clean direct/indirect two-block structure (for modules on `go 1.27+`), preserving attached comments.
  - `go` command **dropped Bazaar (`bzr`) VCS support**.
  - `GODEBUG` settings for *removed* features are now accepted in `go.mod`/`//go:debug` if pinned to their final value (Go 1 compat spirit) — but fail the build if pinned to an old value.
  - `go test` now runs the `stdversion` vet check by default; `go test -json` gained an `OutputType` field (`error`, `error-continue`, `frame`).
  - `@file` response-file parsing (GCC-compatible) now supported by `compile`, `link`, `asm`, `cgo`, `cover`, `pack`.
  - `go tool trace -http` now binds to **localhost only** when given a bare port, matching `pprof`'s behavior.

### `go vet`
- `[1.23]` new **`stdversion`** analyzer — flags references to stdlib symbols too new for the file's effective Go version.
- `[1.24]` new **`tests`** analyzer (bad test/fuzz/benchmark/example signatures); `printf` now flags `fmt.Printf(s)` with no args; `buildtag` flags invalid point-release build constraints (`//go:build go1.23.1`); `copylock` flags `sync.Locker` values captured in 3-clause `for` loops (relevant since 1.22's per-iteration variable semantics).
- `[1.25]` new **`waitgroup`** (misplaced `WaitGroup.Add`) and **`hostport`** (flags `fmt.Sprintf("%s:%d", host, port)` — breaks on IPv6; use `net.JoinHostPort`) analyzers.

---

## 3. Runtime & garbage collector

- **`[1.23]`** `Timer`/`Ticker` overhaul: unstopped timers are now GC-eligible immediately (previously only after firing); timer channels are now **unbuffered** (`cap` reports 0), so `Reset`/`Stop` no longer race with stale values. Gate: `go.mod` line ≥ 1.23; revert via `GODEBUG=asynctimerchan=1`.

- **`[1.24]`** New Swiss-Tables-based **map implementation**, more efficient small-object allocation, new internal mutex — combined **2–3% average CPU overhead reduction**. Opt-outs: `GOEXPERIMENT=noswissmap` / `nospinbitmutex`.

- **`[1.25]`**
  - **Container-aware `GOMAXPROCS`**: on Linux, respects cgroup CPU bandwidth limits (i.e., Kubernetes "CPU limit"), and on all OSes, periodically re-evaluates as available CPUs change. Auto-disabled if you set `GOMAXPROCS` manually. Opt-outs: `GODEBUG=containermaxprocs=0`/`updatemaxprocs=0`.
  - **Experimental "Green Tea" GC** (`GOEXPERIMENT=greenteagc`) — better locality/scalability for marking small objects; **10–40% GC overhead reduction** expected.
  - **`runtime/trace.FlightRecorder`** — continuously records execution trace into an in-memory ring buffer; snapshot only when something interesting happens, instead of always writing a huge trace file.
  - Re-panic output simplified (no longer repeats panic text twice).
  - VMA naming on Linux (`[anon: Go: heap]`) for easier `/proc` inspection.

- **`[1.26]`**
  - **Green Tea GC is now the default** (opt-out: `GOEXPERIMENT=nogreenteagc`, expected removed in 1.27). Extra ~10% win on newer amd64 CPUs (Ice Lake / Zen 4+) via vector instructions.
  - **cgo call overhead cut by ~30%**.
  - **Heap base address randomization** on 64-bit platforms (security hardening against cgo-adjacent exploits); opt-out `GOEXPERIMENT=norandomizedheapbase64`.
  - **Experimental goroutine leak profile** (`GOEXPERIMENT=goroutineleakprofile`) — uses GC reachability to detect goroutines permanently blocked on unreachable channels/mutexes/conds. Exposed at `/debug/pprof/goroutineleak`. (Contributed by Uber.)

- **`[1.27]`**
  - **Goroutine leak profile is now GA** (no experiment flag needed) — `runtime/pprof` type `goroutineleak`, HTTP endpoint `/debug/pprof/goroutineleak`.
  - `asynctimerchan` GODEBUG **permanently removed** — timer channels are always unbuffered now, no matter what.
  - **Size-specialized malloc**: compiler emits calls to size-specialized allocation routines, up to 30% cheaper for small (<80B) allocations (~1% overall win expected), at the cost of ~60KB binary size. Opt-out: `GOEXPERIMENT=nosizespecializedmalloc`.
  - Tracebacks now include `pprof` goroutine labels in the header (for `go 1.27+` modules); opt-out `GODEBUG=tracebacklabels=0`.

---

## 4. Compiler, linker & PGO

- **`[1.23]`** PGO build-time overhead slashed (100%+ → single digits). Compiler can overlap stack frame slots of disjoint-lifetime locals, reducing stack usage. PGO-driven hot-block alignment on 386/amd64 (+1–1.5% perf, +0.1% size). Linker now **restricts `//go:linkname`** to symbols explicitly marked for it in the stdlib (`-checklinkname=0` to disable for debugging).

- **`[1.24]`** Compiler now consistently rejects methods on cgo-generated receiver types, including via alias indirection.

- **`[1.25]`**
  - **Fixed a long-standing nil-pointer-check delay bug** (since 1.21!) — code that used a value before checking its accompanying error could previously *not* panic when it should have. Now it correctly panics:
    ```go
    f, err := os.Open("missing")
    name := f.Name()   // now correctly panics if err != nil
    ```
  - **DWARF 5** debug info by default — smaller binaries, faster linking (`GOEXPERIMENT=nodwarf5` to revert).
  - More slice backing-store **stack allocation** (faster, but can amplify bad `unsafe.Pointer` bugs — use the `bisect` tool with `-compile=variablemake` to hunt down issues).
  - Linker: `-funcalign=N` flag for function alignment control.

- **`[1.26]`** More slice stack-allocation improvements (same bisect/debug story). Linker: `windows/arm64` internal linking for cgo now supported. Multiple **binary-format cleanups** (moduledata now in its own `.go.module` section, `.gosymtab` removed, `pcHeader` relocations dropped, ELF sections sorted by address) — invisible to running programs, relevant only to binary analysis tools/custom linker scripts.

- **`[1.27]`**
  - `//line` directive relative filenames now resolve against the containing file's directory (matches `go/scanner`).
  - Closures get **simpler, more stable names** and can be deduplicated across inlined call sites — cosmetic for most, but can affect code that (incorrectly) compares function pointers for identity.
  - macOS linker: `-macos`/`-macsdk` flags to set target OS/SDK versions in `LC_BUILD_VERSION`.

---

## 5. Iterators, `slices`, `maps`, `strings`, `bytes`

The range-over-func feature (§1) triggered a wave of new iterator-returning helpers across the standard library.

- **`[1.23]`**
  - `iter` package: base definitions (`Seq`, `Seq2`).
  - `slices`: `All`, `Values`, `Backward`, `Collect`, `AppendSeq`, `Sorted`, `SortedFunc`, `SortedStableFunc`, `Chunk`, plus non-iterator addition `Repeat`.
  - `maps`: `All`, `Keys`, `Values`, `Insert`, `Collect`.
  - `unique` package (new): value canonicalization/interning via `unique.Make[T]` → `Handle[T]` (cheap pointer-equality comparisons).
  - `structs` package (new): `structs.HostLayout` marker type for host-ABI-compatible struct layout (cgo interop).

- **`[1.24]`**
  - `bytes`: `Lines`, `SplitSeq`, `SplitAfterSeq`, `FieldsSeq`, `FieldsFuncSeq`.
  - `strings`: same five iterator functions, mirrored.
  - `go/types`: iterator versions of most `Len()`/`At(i)` pair APIs (`Interface.Methods`, `Struct.Fields`, `Tuple.Variables`, etc.) — turns `for i := 0; i < x.Len(); i++ { use(x.At(i)) }` into `for v := range x.Variables() { use(v) }`.
  - `weak` package (new): weak pointers, for memory-efficient caches/canonicalization maps alongside `unique`.
  - `runtime.AddCleanup` (new): a better `SetFinalizer` — multiple cleanups per object, works on interior pointers, doesn't leak on cycles, doesn't delay freeing.

- **`[1.26]`**
  - `reflect`: `Type.Fields/Methods/Ins/Outs` and `Value.Fields/Methods` — iterators over a type's/value's fields, methods, and function parameters.
  - `bytes.Buffer.Peek(n)` — read ahead without consuming.

- **`[1.27]`**
  - `bytes.CutLast` / `strings.CutLast` — slice around the *last* occurrence of a separator (simplifies common `LastIndex` patterns).

---

## 6. Cryptography

Far and away the most active area across all five releases — TLS, post-quantum crypto, and FIPS 140-3 dominate.

### Post-quantum & key exchange
- `[1.23]` Experimental `X25519Kyber768Draft00` PQ key exchange, enabled by default (revert: `GODEBUG=tlskyber=0`).
- `[1.24]` New **`crypto/mlkem`** package (ML-KEM-768/1024, [FIPS 203](https://doi.org/10.6028/NIST.FIPS.203)). `X25519MLKEM768` becomes the default hybrid KEM in `crypto/tls`; the draft Kyber support is removed.
- `[1.26]` New **`crypto/hpke`** package ([RFC 9180](https://rfc-editor.org/rfc/rfc9180.html)). `SecP256r1MLKEM768` / `SecP384r1MLKEM1024` hybrid KEMs on by default in TLS.
- `[1.27]` New **`crypto/mldsa`** package — post-quantum ML-DSA signatures ([FIPS 204](https://csrc.nist.gov/pubs/fips/204/final)), plus `crypto/x509` and `crypto/tls` (TLS 1.3, `MLDSA44/65/87` signature schemes) support. `MLKEM1024` key exchange added (opt-in via `CurvePreferences`).

### FIPS 140-3
- `[1.24]` New Go Cryptographic Module machinery: `GOFIPS140` env var selects the module version; `GODEBUG=fips140` toggles runtime enforcement. First shipped module: v1.0.0. [go.dev/doc/security/fips140](https://go.dev/doc/security/fips140)
- `[1.25]` Signing under FIPS mode is **4× faster** for ECDSA/Ed25519 (parity with non-FIPS). `fips140` GODEBUG changes after start are now a no-op (previously could panic).
- `[1.26]` `crypto/fips140.WithoutEnforcement`/`Enforced` for selectively disabling strict checks even in `GODEBUG=fips140=only` mode.

### crypto/tls
- `[1.23]` Client-side Encrypted Client Hello (ECH, draft spec). 3DES removed from default cipher suites (`GODEBUG=tls3des=1` to revert). `X509KeyPair`/`LoadX509KeyPair` populate `Certificate.Leaf` by default.
- `[1.24]` Server-side ECH support. `Server`/`Transport` gain unencrypted HTTP/2 support ("h2c with prior knowledge") via new `Protocols` fields.
- `[1.25]` New `ConnectionState.CurveID` field. SHA-1 sigs disallowed in TLS 1.2 (`GODEBUG=tlssha1=1` reverts). Servers now prefer the highest mutually-supported protocol version. Both peers stricter about spec compliance.
- `[1.26]` `Certificate.PrivateKey` implementing `crypto.MessageSigner` is now preferred over `Sign`. **Five old GODEBUG toggles slated for removal in 1.27**: `tlsunsafeekm`, `tlsrsakex`, `tls10server`, `tls3des`, `x509keypairleaf`.
- `[1.27]` Those five GODEBUG settings **permanently removed**. New `ConnectionState.LocalCertificate` (chain presented to the peer). `Config.Rand` deprecated in favor of `testing/cryptotest.SetGlobalRandom`.

### Hashing & misc crypto
- `[1.24]` New **`crypto/hkdf`**, **`crypto/pbkdf2`**, **`crypto/sha3`** packages (promoted from `golang.org/x/crypto`). `NewGCMWithRandomNonce`. RSA key-gen rejects <1024-bit keys. `crypto/rand.Read` now **guaranteed never to fail**; uses `getrandom` vDSO on Linux 6.11+, `arc4random_buf` on OpenBSD. New `crypto/rand.Text()` for random text tokens.
- `[1.25]` `hash.XOF`/`hash.Cloner` interfaces; all stdlib hashes implement `Cloner`. AVX2 now effectively required for fast SHA-1/256/512 on amd64.
- `[1.26]` **Deterministic random parameters removed** from most key-gen/signing functions (`ecdsa`, `ecdh`, `ed25519`, `dsa`, `rsa`, `rand.Prime`, etc.) — they now always use a real CSPRNG; use the new **`testing/cryptotest.SetGlobalRandom`** for deterministic tests instead (`GODEBUG=cryptocustomrand=1` to temporarily revert). `crypto.Encapsulator`/`Decapsulator` interfaces for abstract KEM keys.

---

## 7. Networking (`net`, `net/http`)

- **`[1.23]`** `net.KeepAliveConfig` for fine-grained TCP keepalive tuning. `net/http`: `Cookie.Quoted`/`Cookie.Partitioned` fields, `Request.CookiesNamed`, `ParseCookie`/`ParseSetCookie`, `Request.Pattern` (matched `ServeMux` pattern).

- **`[1.24]`** `ListenConfig` uses **MPTCP by default** on Linux. `net/http.Transport`'s 1xx-response handling changed from a hard count limit to a byte-size limit (tunable via `MaxResponseHeaderBytes`). New `Server.Protocols`/`Transport.Protocols` fields for configuring HTTP/1 vs (unencrypted) HTTP/2.

- **`[1.25]`** `net/http.CrossOriginProtection` — built-in CSRF defense using Fetch metadata headers, no tokens/cookies required. `net.LookupMX` now also returns IP-address-shaped DNS names (previously silently discarded).

- **`[1.26]`** `Dialer.DialIP/DialTCP/DialUDP/DialUnix` (context-aware typed dialing). `net/http`: `HTTP2Config.StrictMaxConcurrentRequests`, `Transport.NewClientConn` for manual connection management, `Client` now scopes cookies to `Request.Host`. `ServeMux` trailing-slash redirects now use **307** instead of 301. `httputil.ReverseProxy.Director` **deprecated** in favor of `Rewrite` (Director is fundamentally unsafe against header-stripping attacks). `net/url.Parse` now **rejects malformed host:port strings** with stray colons (`GODEBUG=urlstrictcolons=0` to revert).

- **`[1.27]`**
  - HTTP/2 server now honors **client priority signals** (RFC 9218) — `Server.DisableClientPriority` to opt out.
  - HTTP/1 `Response.Body` **auto-drains** remaining content on `Close()` (up to a limit) for better connection reuse.
  - New `Server.MaxHeaderValueCount` cap.
  - `Transport`/`Server` support TLS ALPN negotiation on user-supplied `net.Conn`s.
  - `httptest.NewTestServer` — in-memory fake-network server for use with `testing/synctest`.
  - `net.UnixConn` read methods now return **unwrapped `io.EOF`**.
  - `net/url.URL.Clone` / `Values.Clone`.
  - Unexported groundwork for **HTTP/3 hooks** in `net/http` — nothing public yet, but the plumbing is there for a future `Transport` that speaks HTTP/3 (noted by third-party analysis, not the official notes).

---

## 8. Testing

- **`[1.24]` `testing.B.Loop`** — the new idiomatic benchmark loop (`for b.Loop() { ... }`), replacing `for range b.N`. Runs setup/teardown exactly once per `-count`, and keeps call params/results alive so the compiler can't dead-code the body away.
  ```go
  func BenchmarkFoo(b *testing.B) {
      for b.Loop() {
          Foo()
      }
  }
  ```
  Also new: `T.Context`/`B.Context` (auto-canceled context per test), `T.Chdir`/`B.Chdir`.
  **`testing/synctest`** debuts experimentally (`GOEXPERIMENT=synctest`) — isolated "bubbles" with a fake clock for deterministic concurrency tests.

- **`[1.25]`** `testing/synctest` **graduates to GA** (API: `Test`, `Wait`) — old experimental API still there under the flag but removed in 1.26. New `T.Attr`/`B.Attr`/`F.Attr` (structured test metadata), `TB.Output()` (writer sharing `Log`'s stream), `AllocsPerRun` now panics under `-parallel`.

- **`[1.26]`** `T.ArtifactDir`/`B.ArtifactDir`/`F.ArtifactDir` — directory for test-produced files, wired to `go test -artifacts`. `B.Loop` no longer blocks inlining (fixes a performance footgun). New `testing/cryptotest.SetGlobalRandom` (see §6).

- **`[1.27]`** `testing/synctest.Sleep` helper (= `time.Sleep` + `synctest.Wait` in one call).

---

## 9. JSON

The biggest slow-burn standard-library rewrite of this whole span.

- **`[1.24]`** `encoding/json`: new **`omitzero`** struct tag option — omits a field when it's the zero value (uses `IsZero() bool` if present), clearer than `omitempty` and correctly handles `time.Time{}` (a long-standing `omitempty` pain point).

- **`[1.25]`** **`encoding/json/v2`** and **`encoding/json/jsontext`** land experimentally behind `GOEXPERIMENT=jsonv2` — a from-scratch rewrite with much faster decoding and parity encoding. `encoding/json` itself gains new `Options` under the flag; error text may change but behavior doesn't.

- **`[1.27]`** **`encoding/json/v2` and `jsontext` are stable, non-experimental packages**, and **`encoding/json` is now backed by the v2 implementation by default** (opt-out: `GOEXPERIMENT=nojsonv2`). v2 has stricter defaults (rejects invalid UTF-8, rejects duplicate object keys) but `encoding/json` gains compatibility `Options` to keep v1 semantics without a full migration — you are **not required to migrate**. Marshal perf ~parity, unmarshal **significantly faster**. Some `GOEXPERIMENT`-era APIs were renamed/removed before stabilizing (`inline` tag → `embed`, `format`/`unknown` tag options and `DiscardUnknownMembers` dropped, `SkipFunc` sentinel removed) — worth checking if you experimented with jsonv2 early.
  [`encoding/json/v2` docs](https://go.dev/pkg/encoding/json/v2) · [migration guide](https://go.dev/pkg/encoding/json#hdr-Migrating_to_v2) · [proposal #71497](https://go.dev/issue/71497)

---

## 10. Filesystem & OS

- **`[1.24]` Directory-scoped filesystem access**: new `os.Root` type + `os.OpenRoot` — perform file ops confined to a directory, immune to symlink escapes. Mirrors most of `os`'s API (`Root.Open`, `Root.Create`, `Root.Mkdir`, `Root.Stat`, ...).
  ```go
  root, _ := os.OpenRoot("/safe/dir")
  f, _ := root.Open("../../etc/passwd") // fails — can't escape root
  ```

- **`[1.25]`** `os.Root` gains a big batch of additional methods: `Chmod`, `Chown`, `Chtimes`, `Lchown`, `Link`, `MkdirAll`, `ReadFile`, `Readlink`, `RemoveAll`, `Rename`, `Symlink`, `WriteFile`. `os.DirFS`/`Root.FS` implement new `io/fs.ReadLinkFS`; `os.CopyFS` now supports symlinks. Windows: async-I/O file handles now integrate with the Go runtime's I/O completion port (non-blocking `Read`/`Write`/`ReadAt`/`WriteAt` + working deadlines) — notably helps named-pipe-heavy apps.

- **`[1.26]`** `os.Process.WithHandle` — access the underlying OS process handle (pidfd on Linux, `Handle` on Windows). Windows `OpenFile` accepts arbitrary Windows-specific flags (`FILE_FLAG_OVERLAPPED`, `FILE_FLAG_SEQUENTIAL_SCAN`, etc). `os/signal.NotifyContext` now cancels with a `CancelCauseFunc` carrying the signal.

---

## 11. Ports & platform support

Steady march forward — old OS versions keep getting dropped a release after being announced.

| Release | Platform news |
|---|---|
| `[1.23]` | Requires **macOS 11 Big Sur+**. Last release supporting **Linux kernel 2.6.32+** (1.24 needs 3.2+). Experimental OpenBSD/riscv64. New `GOARM64`, `GORISCV64` env vars. |
| `[1.24]` | Requires **Linux kernel 3.2+**. Last release on **macOS 11**. `go:wasmexport` directive; wasip1 reactor/library builds via `-buildmode=c-shared`. 32-bit `windows/arm` marked **broken**. |
| `[1.25]` | Requires **macOS 12 Monterey+**. Last release with the broken 32-bit `windows/arm` (removed in 1.26). `GOAMD64=v3+` now uses fused multiply-add (changes exact FP results — use explicit casts to avoid). linux/loong64 gains race detector + cgo traceback + internal linking. linux/riscv64 gains `plugin` build mode; new `GORISCV64=rva23u64`. |
| `[1.26]` | Last release on **macOS 12** (1.27 needs 13 Ventura+). 32-bit `windows/arm` **removed**. freebsd/riscv64 marked **broken**. linux/riscv64 gains race detector. Last release with **ELFv1 ABI** on linux/ppc64 (switches to ELFv2 in 1.27). s390x supports register-based calling convention. Wasm: unconditional sign-extension/non-trapping-conversion instructions; much smaller heap growth increments. |
| `[1.27]` | Requires **macOS 13 Ventura+**. linux/ppc64 now uses **ELFv2 ABI** (needs kernel 3.13+; RHEL7's 3.10 backports it) — cgo, PIE, and external linking now supported there; use `CGO_ENABLED=0` for old-style static pure-Go binaries. |

---

## 12. New packages, at a glance

| Package | Since | Purpose |
|---|---|---|
| [`iter`](https://go.dev/pkg/iter) | 1.23 | Core iterator (`Seq`/`Seq2`) definitions for range-over-func |
| [`unique`](https://go.dev/pkg/unique) | 1.23 | Value canonicalization / interning |
| [`structs`](https://go.dev/pkg/structs) | 1.23 | `HostLayout` marker for cgo-compatible struct layout |
| [`weak`](https://go.dev/pkg/weak) | 1.24 | Weak pointers |
| [`crypto/mlkem`](https://go.dev/pkg/crypto/mlkem) | 1.24 | Post-quantum ML-KEM-768/1024 |
| [`crypto/hkdf`](https://go.dev/pkg/crypto/hkdf) | 1.24 | HKDF key derivation (RFC 5869) |
| [`crypto/pbkdf2`](https://go.dev/pkg/crypto/pbkdf2) | 1.24 | PBKDF2 key derivation (RFC 8018) |
| [`crypto/sha3`](https://go.dev/pkg/crypto/sha3) | 1.24 | SHA-3 / SHAKE / cSHAKE |
| `testing/synctest` (experimental) | 1.24 | Deterministic concurrency testing (GA in 1.25) |
| [`encoding/json/v2`](https://go.dev/pkg/encoding/json/v2) (experimental) | 1.25 | JSON rewrite (GA/default in 1.27) |
| [`encoding/json/jsontext`](https://go.dev/pkg/encoding/json/jsontext) (experimental) | 1.25 | Low-level JSON tokenizer (GA in 1.27) |
| [`crypto/hpke`](https://go.dev/pkg/crypto/hpke) | 1.26 | Hybrid Public Key Encryption (RFC 9180) |
| `simd/archsimd` (experimental) | 1.26 | Arch-specific SIMD (amd64 in 1.26; +arm64/wasm in 1.27) |
| `runtime/secret` (experimental) | 1.26 | Secure erasure of secret temporaries (forward secrecy) |
| [`crypto/mldsa`](https://go.dev/pkg/crypto/mldsa) | 1.27 | Post-quantum ML-DSA signatures (FIPS 204) |
| [`uuid`](https://go.dev/pkg/uuid) | 1.27 | UUID generation/parsing (finally, in the stdlib!) |
| `simd` (experimental) | 1.27 | Portable, vector-size-agnostic SIMD |

---

## 13. Recommended further reading

- [Go 1.23 release blog](https://go.dev/blog/go1.23) · [1.24](https://go.dev/blog/go1.24) · [1.25](https://go.dev/blog/go1.25) · [1.26](https://go.dev/blog/go1.26) · [1.27](https://go.dev/blog/go1.27)
- [Range Over Function Types](https://go.dev/blog/range-functions) — deep dive on the 1.23 iterator feature
- [Container-aware GOMAXPROCS](https://go.dev/blog/go1.25) blog follow-up — 1.25's cgroup-aware scheduler default
- [The FIPS 140-3 Go Cryptographic Module](https://go.dev/doc/security/fips140)
- [Using `go fix` to modernize Go code](https://go.dev/blog/go1.26) — 1.26's modernizer tooling
- Third-party "interactive tour" style writeups (not official, but good hands-on examples): [antonz.org Go 1.23 tour](https://antonz.org/go-1-23/), [VictoriaMetrics Go 1.27 tour](https://victoriametrics.com/blog/go-1-27/)