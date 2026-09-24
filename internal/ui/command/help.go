package command

import (
	"fmt"
	"io"
	"strings"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// RenderOptions controls one help render.
type RenderOptions struct {
	Mode  Mode
	Width int
	Color bool
}

// WriteIndex writes the top-level command list: one line per verb, no flags.
func WriteIndex(w io.Writer, opts RenderOptions) error {
	s := style.NewHelpStyles(opts.Color)

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	for _, kind := range []VerbKind{KindSession, KindLog, KindTask, KindAdmin} {
		verbs := verbsInMode(kind, opts.Mode)
		if len(verbs) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "  %s\n", s.Section.Render(strings.ToUpper(kind.String()))); err != nil {
			return err
		}

		nameWidth, argWidth := columnWidths(verbs)
		for _, v := range verbs {
			if _, err := fmt.Fprintln(w, renderIndexRow(v, s, nameWidth, argWidth)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}

	hint := "Run `mikyu <verb> --help` for flags and examples."
	if opts.Mode&ModeCLI == 0 {
		hint = "Run `<verb> --help` for flags and examples."
	}
	_, err := fmt.Fprintf(w, "  %s\n", s.Dim.Render(hint))
	return err
}

// columnWidths sizes the two columns the index uses, with two trailing
// spaces between columns.
func columnWidths(verbs []Verb) (name, args int) {
	name, args = 10, 12
	for _, v := range verbs {
		if n := len(v.Name) + 2; n > name {
			name = n
		}
		if v.Args != "" && len(v.Args)+2 > args {
			args = len(v.Args) + 2
		}
	}
	return name, args
}

// renderIndexRow formats one verb row for the index.
func renderIndexRow(v Verb, s style.HelpStyles, nameWidth, argWidth int) string {
	var b strings.Builder
	b.WriteString("    ")
	b.WriteString(s.Name.Render(v.Name))
	b.WriteString(strings.Repeat(" ", nameWidth-len(v.Name)))
	if v.Args != "" {
		b.WriteString(formatArgsStyled(v.Args, s))
		b.WriteString(strings.Repeat(" ", argWidth-len(v.Args)))
	} else {
		b.WriteString(strings.Repeat(" ", argWidth))
	}
	b.WriteString(s.Summary.Render(v.Summary))
	return b.String()
}

// WriteVerb writes one verb's detail page.
func WriteVerb(w io.Writer, v Verb, opts RenderOptions) error {
	width := resolveWidth(opts.Width)
	s := style.NewHelpStyles(opts.Color)

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeVerbHeader(w, v, s); err != nil {
		return err
	}
	if err := writeVerbLong(w, v, s, width); err != nil {
		return err
	}
	if err := writeVerbFlags(w, v, s); err != nil {
		return err
	}
	if err := writeVerbExamples(w, v, s, opts.Mode); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

func writeVerbHeader(w io.Writer, v Verb, s style.HelpStyles) error {
	var head strings.Builder
	head.WriteString("  ")
	head.WriteString(s.Name.Render(v.Name))
	if v.Args != "" {
		head.WriteString(" ")
		head.WriteString(formatArgsStyled(v.Args, s))
	}
	if _, err := fmt.Fprintln(w, head.String()); err != nil {
		return err
	}
	if v.Summary == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "    %s\n", s.Summary.Render(v.Summary))
	return err
}

func writeVerbLong(w io.Writer, v Verb, s style.HelpStyles, width int) error {
	if v.Long == "" {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	for _, line := range wrapParagraphs(v.Long, wrapWidth(width)) {
		if line == "" {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(w, "    %s\n", line); err != nil {
			return err
		}
	}
	return nil
}

func writeVerbFlags(w io.Writer, v Verb, s style.HelpStyles) error {
	groups := buildFlagGroups(v)
	if len(groups) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\n  %s\n", s.Section.Render("FLAGS")); err != nil {
		return err
	}
	return writeFlagGroups(w, groups, s)
}

func writeVerbExamples(w io.Writer, v Verb, s style.HelpStyles, mode Mode) error {
	if len(v.Examples) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\n  %s\n", s.Section.Render("EXAMPLES")); err != nil {
		return err
	}
	prefix := ""
	if mode&ModeCLI != 0 {
		prefix = "mikyu "
	}
	for _, ex := range v.Examples {
		if _, err := fmt.Fprintf(w, "    %s %s\n",
			s.FlagSection.Render(">"),
			s.Name.Render(prefix+ex.Invocation),
		); err != nil {
			return err
		}
	}
	return nil
}

// paramsFor returns the spec parameters a verb renders in its FLAGS section.
func paramsFor(v Verb) []spec.Param {
	switch v.Flags {
	case FlagsQueueCreate:
		return spec.Params(spec.LayerSpec)
	case FlagsQueuePatch:
		return spec.Updatable()
	case FlagsEnqueue:
		return pickCallParams("visibility", "max_attempts", "scheduled_at")
	case FlagsNack:
		return pickCallParams("retry_after")
	default:
		return nil
	}
}

// pickCallParams returns the call-layer params whose keys are in keys,
// in table order.
func pickCallParams(keys ...string) []spec.Param {
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	var out []spec.Param
	for _, p := range spec.Params(spec.LayerCall) {
		if want[p.Key] {
			out = append(out, p)
		}
	}
	return out
}

// flagRow is one rendered flag line before grouping.
type flagRow struct {
	group string
	name  string
	kind  string
	doc   string
	def   string
}

// flagGroup is a titled block of flag rows inside the FLAGS section.
type flagGroup struct {
	name string
	rows []flagRow
}

// collectSpecRows turns spec params into flag rows keyed by their path
// prefix: `engine.batch_size` lands in the "engine" group.
func collectSpecRows(params []spec.Param) []flagRow {
	rows := make([]flagRow, 0, len(params))
	for _, p := range params {
		g, _, ok := strings.Cut(p.Path, ".")
		if !ok {
			g = "options"
		}
		rows = append(rows, flagRow{
			group: g,
			name:  "--" + p.FlagName,
			kind:  "<" + p.Kind.String() + ">",
			doc:   p.Doc,
			def:   defaultString(p),
		})
	}
	return rows
}

// collectExtraRows turns presentation flags into flag rows under "options".
func collectExtraRows(extras []FlagDoc) []flagRow {
	rows := make([]flagRow, 0, len(extras))
	for _, f := range extras {
		var kind string
		if f.Kind != "" {
			kind = "<" + f.Kind + ">"
		}
		rows = append(rows, flagRow{
			group: "options",
			name:  "--" + f.Name,
			kind:  kind,
			doc:   f.Doc,
			def:   f.Default,
		})
	}
	return rows
}

// buildFlagGroups assembles a verb's flag groups: spec params first
// (kept in table order, grouped by path prefix), then presentation flags.
func buildFlagGroups(v Verb) []flagGroup {
	var groups []flagGroup
	index := map[string]int{}

	add := func(row flagRow) {
		if i, ok := index[row.group]; ok {
			groups[i].rows = append(groups[i].rows, row)
			return
		}
		index[row.group] = len(groups)
		groups = append(groups, flagGroup{name: row.group, rows: []flagRow{row}})
	}

	for _, r := range collectSpecRows(paramsFor(v)) {
		add(r)
	}
	for _, r := range collectExtraRows(v.ExtraFlags) {
		add(r)
	}
	return groups
}

// humanGroup renders a flag group's key as its display label.
func humanGroup(name string) string {
	if name == "" {
		return "OPTIONS"
	}
	return strings.ToUpper(name)
}

func writeFlagGroups(w io.Writer, groups []flagGroup, s style.HelpStyles) error {
	if len(groups) == 0 {
		return nil
	}

	maxName, maxKind := 0, 0
	for _, g := range groups {
		for _, r := range g.rows {
			if n := len(r.name); n > maxName {
				maxName = n
			}
			if n := len(r.kind); n > maxKind {
				maxKind = n
			}
		}
	}
	nameCol, kindCol := maxName+2, maxKind+2

	multi := len(groups) > 1
	indent := "    "
	if multi {
		indent = "      "
	}

	for gi, g := range groups {
		if gi > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if multi {
			if _, err := fmt.Fprintf(w, "    %s\n", s.FlagSection.Render(humanGroup(g.name))); err != nil {
				return err
			}
		}
		for _, r := range g.rows {
			if _, err := fmt.Fprintln(w, renderFlagRow(r, s, indent, nameCol, kindCol)); err != nil {
				return err
			}
		}
	}
	return nil
}

func renderFlagRow(r flagRow, s style.HelpStyles, indent string, nameCol, kindCol int) string {
	var b strings.Builder
	b.WriteString(indent)
	b.WriteString(s.FlagName.Render(r.name))
	b.WriteString(strings.Repeat(" ", nameCol-len(r.name)))
	if r.kind != "" {
		b.WriteString(s.RequiredArg.Render(r.kind))
	}
	b.WriteString(strings.Repeat(" ", kindCol-len(r.kind)))
	b.WriteString(s.FlagDoc.Render(r.doc))
	if r.def != "" {
		b.WriteString(" ")
		b.WriteString(s.FlagDefault.Render("(default " + r.def + ")"))
	}
	return b.String()
}

// defaultString renders p's built-in default as a short human string.
func defaultString(p spec.Param) string {
	v, ok := p.Default()
	if !ok {
		return ""
	}
	return formatDefault(v)
}

// formatDefault renders one built-in value, mapping zero durations and
// bytes to "unlimited".
func formatDefault(v spec.Value) string {
	switch v.Kind() {
	case spec.KindBytes:
		b := v.Bytes()
		if b == 0 {
			return "unlimited"
		}
		return humanBytes(b)
	case spec.KindDuration:
		d := v.Duration()
		if d == 0 {
			return "unlimited"
		}
		return d.String()
	default:
		return v.String()
	}
}

// humanBytes renders b in binary units (KiB, MiB, ...).
func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.0f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatArgsStyled renders a positional argument shape, styling <required>
// args in the danger color and [optional] args in muted grey.
func formatArgsStyled(raw string, s style.HelpStyles) string {
	var sb strings.Builder
	sb.Grow(len(raw))

	i := 0
	for i < len(raw) {
		switch raw[i] {
		case '<':
			end := strings.IndexByte(raw[i:], '>')
			if end < 0 {
				sb.WriteString(s.Arg.Render(raw[i:]))
				return sb.String()
			}
			end += i + 1
			sb.WriteString(s.RequiredArg.Render(raw[i:end]))
			i = end
		case '[':
			end := strings.IndexByte(raw[i:], ']')
			if end < 0 {
				sb.WriteString(s.Arg.Render(raw[i:]))
				return sb.String()
			}
			end += i + 1
			sb.WriteString(s.OptionalArg.Render(raw[i:end]))
			i = end
		default:
			next := strings.IndexAny(raw[i:], "<[")
			if next < 0 {
				sb.WriteString(s.Arg.Render(raw[i:]))
				return sb.String()
			}
			sb.WriteString(s.Arg.Render(raw[i : i+next]))
			i += next
		}
	}
	return sb.String()
}

// FormatArgsStyled is the color-bool form of formatArgsStyled for callers outside the help renderer.
func FormatArgsStyled(raw string, color bool) string {
	return formatArgsStyled(raw, style.NewHelpStyles(color))
}

// resolveWidth returns opts.Width, or the terminal width, or 80.
func resolveWidth(w int) int {
	if w > 0 {
		return w
	}
	if t := GetTerminalWidth(); t > 0 {
		return t
	}
	return 80
}

// wrapWidth caps the body wrap at 78 columns.
func wrapWidth(total int) int {
	w := total - 4
	if w < 40 {
		w = 40
	}
	if w > 78 {
		w = 78
	}
	return w
}

// wrapParagraphs wraps each blank-line-separated paragraph at width columns.
func wrapParagraphs(text string, width int) []string {
	var out []string
	for _, para := range strings.Split(text, "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		var line strings.Builder
		for _, word := range strings.Fields(para) {
			if line.Len() == 0 {
				line.WriteString(word)
				continue
			}
			if line.Len()+1+len(word) > width {
				out = append(out, line.String())
				line.Reset()
				line.WriteString(word)
				continue
			}
			line.WriteString(" ")
			line.WriteString(word)
		}
		if line.Len() > 0 {
			out = append(out, line.String())
		}
		out = append(out, "")
	}
	if len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}
