# Mikyu Metrics Renderer — Implementation Plan

**Goal:** ship `mikyu metrics`, `--detail`, `--queue`, `--watch`, `--output json` with full REPL/CLI parity, TTY/machine dual behavior, and a visual result worth screenshotting.
**Style target:** modern Go, lipgloss v2 (`charm.land/lipgloss/v2`) used to its full depth.

Read this whole document before writing code. Section 0 (invariants) is contract — violating it in code review is a bug, not a preference.

---

## 0. Invariants — the non-negotiables

Memorize these; every phase depends on them.

| # | Invariant | Where it lives |
|---|---|---|
| I1 | **Numbers right-aligned, labels left-aligned. Always.** | `DataTable`, `kvSection` |
| I2 | **Color means state, never decoration.** Only the header card border is decorative (gradient). Everything else colored must be a threshold or a state glyph. | style tokens |
| I3 | **TTY ⇒ humanized + colored; pipe ⇒ raw + colorless.** Detection is on the *output writer*, never `os.Stdout` globally. | humanizer |
| I4 | **`--output json` is plumbing**: the snapshot marshaled verbatim, zero added keys. Text view is porcelain: free to redesign. | command layer |
| I5 | **Section headers and column headers are stable literal strings.** Scripts grep them. Pinned by golden test. | views |
| I6 | **State glyph appears at most twice per queue**: queue title edge (detail), STATE column (summary). Colored but legible in grayscale (glyph shape differs). | views |
| I7 | **One gradient surface per screen**: the header card. | style |
| I8 | **Empty sections don't render.** No zero-row tables, no empty-state headers — instead a teaching empty state. | views |
| I9 | **All printing goes through `lipgloss.Fprint/Fprintln/Sprint`** — never `fmt.Println` — so downsampling and non-TTY stripping are automatic. | everywhere |
| I10 | **A cancelled watch exits 0.** | watch |
| I11 | **Layer diff is the signal; equality is silence.** Spec markers only where layers disagree. | detail view |
| I12 | **Histogram buckets render nowhere in text.** Percentiles only. json keeps buckets reachable. | views |

---

## Phase 0 — Foundations: health derivation + humanizer

### 0.1 State derivation — `internal/metrics`

State semantics belong where the data lives, not in the UI (decided; confirmed by the systemctl precedent). Add a new file `internal/metrics/state.go`:

```go
// Health is the derived operational state of a node or queue, computed from
// a single snapshot. It is a judgment of the metrics layer, not the UI.
type Health int

const (
    HealthOK Health = iota
    HealthWarn
    HealthCritical
)

// QueueHealth derives a queue's state from one snapshot. Deterministic,
// single-snapshot: no history, no wall clock.
func QueueHealth(q QueueView) Health
// NodeHealth derives the node state: worst of node gauges, or worst queue.
func NodeHealth(s Snapshot) Health
```

Exact rules — implement precisely this table (export it as documentation in `state.go`):

| Signal | Warn | Critical |
|---|---|---|
| `backpressure.wal_channel_fill` | ≥ 0.5 | ≥ 0.9 |
| `backpressure.wal_byte_fill` | ≥ 0.5 | ≥ 0.9 |
| `backpressure.queue_full_events` | > 0 | — |
| `backpressure.retry_budget_drops` | > 0 | — |
| `producers.blocked_on_wal` | > 0 | — |
| log queue: `lag / high_water` (groups) | > 0.1 | > 0.5 |
| task queue: dead-lettered > 0 | ✓ | — |
| task queue: `in_flight / max_in_flight` | ≥ 0.8 | ≥ 0.95 |
| node: `storage.cache_bytes / cache_limit` | ≥ 0.95 | — |
| node: `gc.pause.p99_ms` | > 10 | > 50 |
| node: `process.cpu_percent` | > 80 | > 95 |

Node/queue state = worst signal present. Write table-driven tests: each row, both sides of each threshold, and a "quiet snapshot ⇒ OK" case.

`QueueView` should be the existing per-queue type in `metrics.Snapshot` (whatever `snap.Queue(name)` returns — check `snapshot.go`). If gauge access needs small accessors (e.g., `LagRatio()`), add them here, not in the UI.

### 0.2 Humanizer — `internal/ui/human` (new package)

One formatter, two modes (I3):

```go
package human

// Formatter renders quantities for humans (TTY) or machines (pipes).
type Formatter struct{ Raw bool }

func (f Formatter) Bytes(n int64) string      // TTY: "1.2 MiB" · Raw: "209715200"
func (f Formatter) Count(n int64) string      // TTY: "1.2M", "999,800" · Raw: "1200000"
func (f Formatter) Duration(d time.Duration) string // TTY: "1h 23m" · Raw: "5025.5"
func (f Formatter) Percent(frac float64) string     // "12.4 %" / "12.4"
func (f Formatter) Rate(bytesPerSec float64) string // "12.0 MB/s" / "12582912"
```

Rules (encode in the implementation, pin in tests):

- **Bytes**: binary units (KiB/MiB/GiB/TiB), one decimal, **no space** before unit: `200 MiB` → actually `200.0 MiB`? No — trim trailing `.0`: `1.0 GiB` is correct (keeps column width stable), `1.2 MiB` not `1.20 MiB`. Under 1 KiB: `512 B` (raw: `512`).
- **Count**: ≥ 1M → `1.2M`; ≥ 1,000 → thousands separators `999,800`; < 1,000 → plain. One decimal max after abbreviation.
- **Duration (adaptive, two most significant units only)**: `1h 23m`, `4m 12s`, `42s`, `350ms`, `12µs`. Never `5025.5s`. Raw mode: seconds as float.
- **Wire durations** (`"10ms"`, `"30s"` in the spec): parse, then format through `Duration` — but with **one unit minimum precision**: `10ms` stays `10ms`, not `10ms 0µs`.

Tests: golden table covering every unit boundary (`1023 B / 1024 B / 1.5 KiB`, `999999 / 1000000 / 1234567`, sub-ms through hours).

### 0.3 Architecture wiring

`.go-arch-lint.yml` — add components:

```yaml
  human:    { in: internal/ui/human }
  metricsview: { in: internal/ui/metricsview }
```

```yaml
  metricsview:
    mayDependOn: [mikyu, metrics, human, spec]
  human:
    anyVendorDeps: true
```

`internal/ui/style` remains the token/primitive home (existing component, no new deps needed — it's a leaf).

**Acceptance:** `go test ./internal/metrics ./internal/ui/human` green; `go-arch-lint` passes.

---

## Phase 1 — Theme + the seven primitives — `internal/ui/style`

### 1.1 Theme tokens (`style.go`, extend existing)

```go
package style

import (
    "os"
    "charm.land/lipgloss/v2"
)

var ld = lipgloss.LightDark(lipgloss.HasDarkBackground(os.Stdin, os.Stdout))

var (
    Accent  = ld(lipgloss.Color("#6D28D9"), lipgloss.Color("#7D56F4")) // brand, titles
    Accent2 = ld(lipgloss.Color("#0E7490"), lipgloss.Color("#38BDF8")) // gradient end
    Label   = ld(lipgloss.Color("#52525B"), lipgloss.Color("#8B8B96")) // labels, units, rules
    Ok      = ld(lipgloss.Color("#15803D"), lipgloss.Color("#4ADE80"))
    Warn    = ld(lipgloss.Color("#A16207"), lipgloss.Color("#FACC15"))
    Danger  = ld(lipgloss.Color("#B91C1C"), lipgloss.Color("#F87171"))
    Faint   = lipgloss.NewStyle().Faint(true)
)
```

Notes for the newbie:
- Styles are **value types** — define at package level, never rebuild per row/tick (performance: watch mode re-renders every 2s; style construction is setup, not render work).
- `LightDark` is evaluated once at init. Do not use v1 `compat.AdaptiveColor`.
- `Health → Color` mapping lives in exactly one function: `func HealthColor(h metrics.Health) lipgloss.Color` — wait, style can't import metrics (check arch: style is currently a leaf). Instead put the mapping in `metricsview` and pass colors in. Keep `style` ignorant of the metrics domain.

### 1.2 The seven primitives (`internal/ui/style/components.go`)

Every primitive takes pre-humanized strings; primitives never call the humanizer (separation: *what to say* vs *how to align it*).

**1. `TitledBox(title, body string, width int, titleColor lipgloss.Color) string`**

The btop edge-title. Lipgloss can't place a title inside a border, so build the top edge manually and draw the remaining three sides with lipgloss (snippet from our design discussion — use it verbatim):

```go
func TitledBox(title, body string, width int, tc lipgloss.Color) string {
    edge := lipgloss.NewStyle().Foreground(tc)
    t := edge.Render("╭─ ") + title + edge.Render(" ")
    gap := width - lipgloss.Width(t) - 1
    if gap < 1 { gap = 1 }
    top := t + edge.Render(strings.Repeat("─", gap)+"╮")
    rest := lipgloss.NewStyle().
        Border(lipgloss.RoundedBorder(), false, true, true, true).
        BorderForeground(tc).
        Width(width - 2).
        Padding(0, 1).
        Render(body)
    return top + "\n" + rest
}
```

Critical details: measure with `lipgloss.Width` (ANSI-safe), never `len`. `Padding(0,1)` gives content breathing room inside every card. Body must already be `width-4` wide or narrower; test with overlong titles.

**2. `GradientHeader(title, subtitle1, subtitle2 string, width int) string`** — the only gradient surface (I7):

```go
lipgloss.NewStyle().
    Border(lipgloss.RoundedBorder()).
    BorderForegroundBlend(Accent, Accent2).
    Padding(0, 2).Width(width - 2)
```

Title line carries the state glyph + bold name; two dim subtitle lines follow.

**3. `StatBand(stats [][2]string) string`** — big value, dim label beneath, joined horizontally:

```go
value := lipgloss.NewStyle().Bold(true)
label := lipgloss.NewStyle().Foreground(Label).Transform(strings.ToUpper)
// per stat: JoinVertical(lipgloss.Center, value.Render(v), label.Render(l))
// then JoinHorizontal(lipgloss.Bottom, blocks...) with two spaces between
```

**4. `KVSection(title string, rows [][2]string, labelWidth, valueWidth int, twoCol bool) string`**

- Title: `Accent`, bold, uppercase via `Transform(strings.ToUpper)`.
- Each row: `kvLabel.Width(labelWidth).Render(k)` joined horizontally with `kvValue.Width(valueWidth).Align(lipgloss.Right).Render(v)` — **this is invariant I1's enforcement point**.
- `twoCol=true`: split rows into left/right halves, `JoinHorizontal(Top, leftBlock, rightBlock)` with a 4-space gutter. Single-column fallback below 80 cols is the caller's decision, not the primitive's.

**5. `DataTable(headers []string, rows [][]string, numeric map[int]bool) string`**

```go
table.New().
    Border(lipgloss.NormalBorder()).
    BorderTop(false).BorderLeft(false).BorderRight(false).
    BorderStyle(lipgloss.NewStyle().Foreground(Label)).
    StyleFunc(func(row, col int) lipgloss.Style {
        switch {
        case row == table.HeaderRow:
            return lipgloss.NewStyle().Bold(true).Foreground(Accent).
                Padding(0, 1).Transform(strings.ToUpper)
        case numeric[col]:
            return lipgloss.NewStyle().Padding(0, 1).Align(lipgloss.Right)
        default:
            return lipgloss.NewStyle().Padding(0, 1)
        }
    }).
    Headers(headers...).Rows(rows...).String()
```

`numeric` map is passed by the caller — the declaration site of column semantics (I1). Set `TabWidth(0)` on cell styles.

**6. `Gauge(frac float64, width int) string`**

```go
// filled portion: "▓" run; remainder: "·" run; leading "▏" tick.
// color by threshold: <0.60 → Label, 0.60–0.85 → Warn... 
```

Thresholds: `< 0.60` neutral (Label color), `0.60–0.85` Warn, `> 0.85` Danger — **except** when the caller passes `ok=true` (a legitimately-full hot cache) → Ok color. Clamp `frac` to `[0,1]`; width includes the value-free lane (bar is fixed-width — the caller prints the number separately, always in its own right-aligned lane, per the backpressure fix).

**7. `Footer(hints ...[2]string) string`** — dim prose, `Accent` command spans. Returns "" when list is empty (machine mode suppresses it).

**Component tests:** each primitive gets table-driven tests asserting exact rendered strings (plain profile) — including alignment with varying value widths, overlong titles, `frac` clamping, empty inputs.

**Acceptance:** all primitive tests green; a `TestComponentsPlainProfile` file pins that rendering with color stripped produces clean ASCII-aligned output.

---

## Phase 2 — Summary view — `internal/ui/metricsview/summary.go`

```go
// Render writes the summary view for the snapshot to w.
// Width is the terminal width (default 80 if unknown).
func Summary(w io.Writer, snap metrics.Snapshot, f human.Formatter, width int) error
```

Content, top to bottom (exact structure — this is the contract):

1. **Header** — `GradientHeader`: state glyph (node health) + `mikyu` bold; line 2: `go1.27 · linux/amd64 · spec v5 · up 1h 23m`; line 3: data dir + segments + snapshot timestamp (`2006-01-02 15:04:05Z` UTC).
2. **StatBand**: Queues / Messages / On Disk / Throughput In (`▲ ` prefix, Ok-colored).
3. **LOG QUEUES card** — `TitledBox` + `DataTable`, columns exactly:
   `NAME · MESSAGES · SIZE · APPENDS/s · READS/s · LAG · STATE`
   LAG = max group lag ratio rendered as percentage (`2.0 %`) — raw: absolute count. STATE = `●`/`◐`/`○` colored by `metrics.QueueHealth`. **numeric map: cols 1–5.**
   Last/Min Offset columns are *not here* (forensic — moved to `--queue`).
4. **TASK QUEUES card** — columns exactly:
   `NAME · ACTIVE · IN-FLIGHT · SCHEDULED · DEAD · ENQ/s · DEQ/s · STATE`
5. **Empty states (I8):** no log queues → no card; instead a dim line: `no log queues yet — create one with: mikyu queue create <name>`. Same pattern for task queues.
6. **Footer** (TTY only): `Run mikyu metrics --queue <name> for detail, or --detail for everything.`

Task-queue fields: derive ACTIVE (dead-lettered count), IN-FLIGHT, SCHEDULED from `metrics.Snapshot`'s per-queue view — inspect `snapshot.go` for what's exposed; if task-queue gauges are missing from the snapshot type, add read-only accessors in `internal/metrics` (Phase 0 territory) rather than computing in the view.

Golden test: fixture snapshot (the one from `internal/metrics/testdata/snapshot.json` plus a synthetic task queue) → pin full rendered output, plain profile, width 80. This test is the column-contract test (I5) — changing a header must break it visibly.

---

## Phase 3 — Detail view — `internal/ui/metricsview/detail.go`

```go
func Detail(w io.Writer, snap metrics.Snapshot, selected []string, f human.Formatter, width int) error
```

`selected` = `--queue` names (empty ⇒ all queues). Section order — fixed narrative (contract):

1. Header card (identical to summary).
2. **Queue cards**, one per selected (or each) queue — `QueueCard(q, f, width)`:

   Structure (log queue):
   - Title edge: state glyph + name, right side `log · healthy|degraded|critical` (health word colored, dim kind).
   - `THROUGHPUT` KVSection two-col: appends/s · reads/s · bytes in · bytes out (+ counters `total appends / total reads` as one dim line beneath — raw counters present but demoted).
   - `LATENCY`: single line `p50 0.5 ms · p90 5.0 ms · p99 9.0 ms · max 10 ms` — values dim-labeled, bright numbers. No buckets (I12).
   - `PRODUCERS` / `CONSUMERS`: one KVSection, two-col, six pairs.
   - `BACKPRESSURE`: **two lanes** (the fix): gauge rows (`WAL channel fill [bar] 0.3 %`) then a two-col KV row pair (queue full events · producer blocks · consumer parks · retry drops). Gauge lane and kv lane never share a row.
   - `CONSUMER GROUPS` DataTable: `NAME · COMMITTED · HIGH WATER · LAG` — **no in-cell gauge** (the fix); LAG right-aligned, cell foreground colored by threshold (Ok→Warn→Danger via `StyleFunc` col check).

   Task queue card: same skeleton, kind-driven fields — `THROUGHPUT` (enq/deq/s), `LEASES` (active · in flight · scheduled) / `DEAD LETTERS` (count · capacity gauge with Ok coloring when by design), `POLICY` KV (lease · max attempts · max in flight · max dead letters). No producers/consumers/backpressure sections.

3. **Node grid**: `RUNTIME` + `PROCESS` as a horizontal pair, then `GC` + `STORAGE` pair (via `JoinHorizontal(Top)` when width ≥ 100, else stacked). Content exactly as designed — including the spelled-out states line `2 running · 3 runnable · 16 waiting` and cpu-time `120.5 u · 30.3 s` dim line. STORAGE carries the two gauges: cache (`cache 32.0/32.0 MiB ▓▓▓▓▓▓▓▓  100 % · 99.8 % hit rate` — hit-rate-first, Ok-colored when healthy-hot), WAL pending.
4. **LOCKS**: only if any observer has `waits > 0` or `global_wait_seconds > 0`; otherwise skip entirely (I8). DataTable: `NAME · WAITS · TOTAL WAIT · MAX WAIT · P50 · P99`.
5. **EFFECTIVE SPEC**: node-level = session defaults (all four groups, KV layout grouped by `storage` / `flush` / `retention` / `task` sub-titles). Per selected queue, an extra `EFFECTIVE SPEC — <name>` card.

   **Diff marker logic (I11)** — implement exactly:
   ```go
   // for each field of the queue's effective spec:
   //   queue spec unset            → render value, no marker
   //   queue spec set, == session  → render value, faint "· session" tag
   //   queue spec set, != session  → render value, Accent "← override (session: X)"
   ```
   Needs the sparse stored spec vs session vs effective — `mikyu.Client.DescribeQueue` provides this when available; when rendering purely from a snapshot, compare effective vs session block and use the `← override` form. Check what `Snapshot` exposes and keep one code path.

6. Footer (TTY only): `mikyu metrics --queue <name> focuses one queue · --output json for tools`.

Empty selection: `--queue jobs` with no such queue → print teaching empty state `no queue named "jobs" — mikyu metrics lists what exists` and **exit 1**.

Golden tests: full detail (fixture), single log queue, single task queue, spec-diff fixture (override present and absent).

---

## Phase 4 — Command wiring, parity, piping — `internal/ui/command/metrics.go`

New file (sibling of `log.go`, `queue.go`). Follow the existing verb/flag conventions in `verb.go`/`flag.go` exactly.

### 4.1 Flags — final surface, no additions

```
mikyu metrics [--detail] [--queue NAME]... [--watch] [--output text|json]
```

- `--detail` boolean.
- `--queue` repeatable string slice; implies focused detail for named queues (pass through to `Detail`).
- `--watch` boolean.
- `--output` enum `text|json`, default `text`. Invalid value → usage error, exit 2.

Register in the metrics verb; help text for `--output` must say: *"`json` emits the snapshot verbatim (for scripts); the text view may change between releases."* — the git porcelain/plumbing rule made user-visible.

### 4.2 The one render path — parity by construction

The command produces a **render function**, and both surfaces consume it:

```go
// RunMetrics renders metrics output to w. Used by both the CLI and the REPL.
// w is the destination; TTY detection and width are derived from it (or from
// the REPL's known width), never from os.Stdout directly.
func RunMetrics(ctx context.Context, c mikyu.Client, w io.Writer, opts MetricsOptions) error
```

- **REPL**: `internal/ui/repl` dispatches `metrics ...` → builds `MetricsOptions` from REPL args → calls `RunMetrics` with the REPL's writer and its known width (use the existing vt detection in `repl`; default 80 when unknown). No REPL-specific formatting code may exist — if you find yourself writing a second renderer, you've broken parity.
- **CLI**: `internal/ui/cli` same call, writer = stdout.
- **Width/TTY resolution order**: explicit REPL width → TTY check on `w` (via existing vt/term helpers) → 80.

### 4.3 Machine mode details

- `--output json`: `json.Encoder` (with `SetIndent("", "  ")`) writing the `metrics.Snapshot` **directly** — no wrapper object, no injected fields (I4). Works with `--watch` (one JSON object per tick, newline-delimited). No footer, no empty-state prose.
- **Piped text mode** (TTY == false, `--output text`): `human.Formatter{Raw: true}`, gradient/cards/borders disabled (`Plain` render mode — see below), no footer, tables borderless. This is the greppable contract (I3, I5).
- Implement modes as a small enum: `type renderMode int; const (modeRich, modePlain)`. Rich = colors+boxes+gauges; Plain = no color, no borders, ASCII alignment only. Every primitive gets a `plain` branch — simplest correct approach: primitives take a `plain bool`, or `style` exposes a package-level `Plain` flag set once at startup. Prefer explicit parameter threading over global state — pass a `style.Theme{Plain bool}` struct.

### 4.4 Exit codes

| Situation | Code |
|---|---|
| snapshot fetched, rendered | 0 |
| `--queue` matched nothing | 1 |
| fetch error (dial, unauthorized…) | 1 |
| usage error | 2 |
| watch cancelled (Ctrl-C) | 0 (I10) |
| watch ended unrecoverably | 1 |

---

## Phase 5 — Fix `--watch` — `internal/ui/command/metrics_watch.go`

### 5.1 First, read the log tail

Open `internal/ui/command/log.go` and study its tail/streaming implementation before writing this file. Steal its patterns: context-driven cancellation, interval ticking, error handling on the client's stream/subscribe channel, and how it detects interruption. The watch here must use the **same cancellation idiom** so the two commands behave identically under Ctrl-C. (If tail uses `signal.NotifyContext`, do the same; if it uses a channel-based stopper, mirror it.)

### 5.2 Behavior — implement exactly

```
mikyu metrics [--detail] [--queue ...] --watch
```

- **Interval**: fixed default **2s**. Override only via `MIKYU_WATCH_INTERVAL` env (`time.ParseDuration`; invalid value → usage error before first frame, exit 2). No flag (decided).
- **Fetch each tick** via `c.Metrics(ctx)` with a per-tick timeout (use the client's existing operation timeout; if none, wrap with `context.WithTimeout(ctx, interval/2)` minimum).
- **Frame assembly**: render the full frame (rich or plain per TTY; json per `--output`) into a buffer each tick. Re-measure width every tick — resize between frames renders correctly on the next one. Reuse package-level styles; only data is recomputed.
- **TTY repaint (the fix for "prints a new frame each tick")**:

  ```go
  // after the first frame, move cursor up N lines and clear each:
  //   fmt.Fprintf(w, "\x1b[%dA", lines)      // cursor up N
  //   per line: "\x1b[2K" then content, or once: "\x1b[0J" after moving up
  ```

  Concretely: keep `lastLines int`. Before printing frame k>0: emit `\x1b[{lastLines}A\x1b[0J` (up + clear-from-cursor-to-end), then write the frame, then set `lastLines = lipgloss.Height(frame)`. **Never** `clear` the screen — scrollback above the output is preserved (decided). ANSI emission only in rich mode (I3: plain mode never emits escapes).
- **Non-TTY append mode**: every frame preceded by one banner line: `── 2023-11-14T22:13:20Z · detail=false · queues=1 ──` (vmstat timestamp discipline). In json+watch: no banner, NDJSON.
- **Stale frames**: fetch error mid-watch → keep the last good frame on screen, print a dim inline line beneath it: `last snapshot 22:13:20Z · dial failed: connection refused`, retry next tick. Recover silently. Three consecutive failures → give up, exit 1 with the error (unrecoverable stream).
- **Cancellation**: `signal.NotifyContext(ctx, os.Interrupt)` (or the log-tail idiom). On cancel: stop ticking, **restore cursor** (emit `\x1b[0J` + newline if rich mode), print one dim summary line `watched for 4m 12s · 126 snapshots`, exit 0 (I10).

Tests: fake clock (inject interval + ticker), fake snapshot source; assert repaint escape sequence counts (rich), banner presence (plain), banner format, cancel-exit-0, stale-frame error line, three-failure abort.

---

## Phase 6 — Polish, guarantees, and the lasting impression

This phase is small but it's where "correct" becomes "memorable":

1. **Golden test infrastructure** — `internal/ui/metricsview/golden_test.go`: helper that renders each view with a **pinned colorprofile** (force plain + force truecolor via the colorprofile writer wrapped on a bytes.Buffer — lipgloss v2 downsamples per writer) against fixture snapshots in `testdata/`. Committed `.golden` files; `go test -update` flag to regenerate; **reviewers diff goldens**.
2. **Coverage audit test** — a table in `metricsview/coverage_test.go` mapping every leaf of `metrics.Snapshot` to the view+section that renders it (the audit table from our design). If a field maps to `"json-only"` it must be justified in a comment. New snapshot fields break this test — forcing a conscious home for every future metric.
3. **Column contract test** — assert exact header strings for both summary tables and the locks/groups tables (I5).
4. **Narrow-terminal flow test** — width 60 render: grid stacks, no card exceeds width, no panic. (Below 60 is best-effort: stacked single-column, gauges at fixed small width.)
5. **Performance sanity** — one benchmark: `BenchmarkDetailRender` on the fixture; assert it stays under ~5ms (it will, if styles are package-level and you're not rebuilding tables per row). Watch mode renders 0.5 frames/s — correctness over cleverness, but don't allocate styles per tick.
6. **Manual QA script** (document in the PR): run against a live node — `mikyu metrics`, `| cat`, `--detail`, `--queue events`, `--queue nope` (exit 1), `--output json | jq .totals`, `--watch` + Ctrl-C (exit 0, single summary line), `--watch > log` (bannered frames), resize terminal mid-watch, `NO_COLOR=1`, `TERM=dumb`, REPL: `metrics`, `metrics --detail`, `metrics --watch` then Ctrl-C returns to the prompt cleanly.

---

## File manifest (new/changed)

| File | Action | Phase |
|---|---|---|
| `internal/metrics/state.go` + `state_test.go` | new — Health derivation | 0 |
| `internal/ui/human/human.go` + tests | new — Formatter | 0 |
| `.go-arch-lint.yml` | add `human`, `metricsview` components | 0 |
| `internal/ui/style/theme.go` | extend — LightDark tokens, Theme{Plain} | 1 |
| `internal/ui/style/components.go` + tests | new — seven primitives | 1 |
| `internal/ui/metricsview/summary.go` + golden | new | 2 |
| `internal/ui/metricsview/detail.go`, `queuecard.go`, `spec.go` + goldens | new | 3 |
| `internal/ui/metricsview/coverage_test.go` | new | 6 |
| `internal/ui/command/metrics.go` | new — flags, RunMetrics | 4 |
| `internal/ui/command/metrics_watch.go` + tests | new — watch (the fix) | 5 |
| `internal/ui/cli/cli.go`, `internal/ui/repl/repl.go` | wire `metrics` verb → RunMetrics | 4 |
| `internal/ui/command/help.go` | metrics help text incl. plumbing note | 4 |

## Build order & definition of done

Ship phases in order; each lands green before the next starts. Done = all golden tests pass in CI across plain and truecolor profiles, REPL and CLI produce byte-identical output for identical flags, the QA script passes end-to-end, and the header card's gradient renders on your terminal exactly once per screen — above six silent, perfectly aligned numbers.

That last sentence is the product. Build to it.