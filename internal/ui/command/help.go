package command

import (
	"fmt"
	"io"
	"strings"

	"github.com/dmunish/mikyu/internal/spec"
)

// RenderOptions controls one help render.
type RenderOptions struct {
	Mode  Mode
	Width int
	Color bool
}

// WriteIndex writes the top-level command list: one line per verb, no flags.
func WriteIndex(w io.Writer, opts RenderOptions) error {
	width := resolveWidth(opts.Width)
	s := newHelpStyles(opts.Color)

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  %s\n\n", s.header.Render("MIKYU — commands")); err != nil {
		return err
	}

	for _, kind := range []VerbKind{KindSession, KindLog, KindTask, KindAdmin} {
		verbs := verbsInMode(kind, opts.Mode)
		if len(verbs) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "  %s\n", s.section.Render(strings.ToUpper(kind.String()))); err != nil {
			return err
		}

		nameWidth := 0
		argWidth := 0
		for _, v := range verbs {
			line := v.Name
			if v.Args != "" {
				line += " " + v.Args
			}
			if len(v.Name) > nameWidth {
				nameWidth = len(v.Name)
			}
			if v.Args != "" && len(v.Args) > argWidth {
				argWidth = len(v.Args)
			}
		}
		nameWidth += 2
		argWidth += 2
		if nameWidth < 10 {
			nameWidth = 10
		}
		if argWidth < 12 {
			argWidth = 12
		}

		for _, v := range verbs {
			visibleName := v.Name
			visibleArgs := v.Args
			namePad := nameWidth - len(visibleName)
			if namePad < 1 {
				namePad = 1
			}
			argPad := argWidth - len(visibleArgs)
			if argPad < 1 {
				argPad = 1
			}
			var b strings.Builder
			b.WriteString(s.name.Render(v.Name))
			b.WriteString(strings.Repeat(" ", namePad))
			if v.Args != "" {
				b.WriteString(formatArgsStyled(v.Args, s))
				b.WriteString(strings.Repeat(" ", argPad))
			} else {
				b.WriteString(strings.Repeat(" ", argWidth))
			}
			b.WriteString(s.summary.Render(v.Summary))
			if _, err := fmt.Fprintf(w, "    %s\n", b.String()); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}

	hint := "Run `mikyu <verb> help` for flags and examples."
	if opts.Mode&ModeCLI == 0 {
		hint = "Run `<verb> help` for flags and examples."
	}
	_ = width
	_, err := fmt.Fprintf(w, "  %s\n", s.dim.Render(hint))
	return err
}

// WriteVerb writes one verb's detail page: long form, usage, examples, and flags.
func WriteVerb(w io.Writer, v Verb, opts RenderOptions) error {
	width := resolveWidth(opts.Width)
	s := newHelpStyles(opts.Color)

	header := v.Name
	if v.Args != "" {
		header += " " + v.Args
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  %s %s\n\n", s.header.Render("●"), s.name.Render(header)); err != nil {
		return err
	}

	if v.Long != "" {
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
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}

	if len(v.Usage) > 0 {
		if _, err := fmt.Fprintf(w, "  %s\n", s.section.Render("USAGE")); err != nil {
			return err
		}
		prefix := ""
		if opts.Mode&ModeCLI != 0 {
			prefix = "mikyu "
		}
		for _, u := range v.Usage {
			if _, err := fmt.Fprintf(w, "    %s\n", s.usage.Render(prefix+u)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}

	if len(v.Examples) > 0 {
		if _, err := fmt.Fprintf(w, "  %s\n", s.section.Render("EXAMPLES")); err != nil {
			return err
		}
		prefix := ""
		if opts.Mode&ModeCLI != 0 {
			prefix = "mikyu "
		}
		for _, ex := range v.Examples {
			if _, err := fmt.Fprintf(w, "    %s\n", s.exampleC.Render(ex.Comment)); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "      %s\n", s.example.Render(prefix+ex.Invocation)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}

	params := paramsFor(v)
	if len(params) > 0 || len(v.ExtraFlags) > 0 {
		if _, err := fmt.Fprintf(w, "  %s\n", s.section.Render("FLAGS")); err != nil {
			return err
		}
		var rows []flagRow
		rows = append(rows, collectExtraRows(v.ExtraFlags)...)
		rows = append(rows, collectSpecRows(params)...)
		if err := writeFlagRows(w, rows, s); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
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

// pickCallParams returns the call-layer params whose keys are in keys, in table order.
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
	left  string
	doc   string
	def   string
}

// collectSpecRows turns spec params into flag rows keyed by their path prefix.
func collectSpecRows(params []spec.Param) []flagRow {
	rows := make([]flagRow, 0, len(params))
	for _, p := range params {
		g, _, ok := strings.Cut(p.Path, ".")
		if !ok {
			g = "options"
		}
		rows = append(rows, flagRow{
			group: g,
			left:  fmt.Sprintf("--%s <%s>", p.FlagName, p.Kind.String()),
			doc:   p.Doc,
			def:   defaultString(p),
		})
	}
	return rows
}

// collectExtraRows turns presentation flags into flag rows under one group.
func collectExtraRows(extras []FlagDoc) []flagRow {
	rows := make([]flagRow, 0, len(extras))
	for _, f := range extras {
		left := "--" + f.Name
		if f.Kind != "" {
			left += " <" + f.Kind + ">"
		}
		rows = append(rows, flagRow{
			group: "options",
			left:  left,
			doc:   f.Doc,
			def:   f.Default,
		})
	}
	return rows
}

// writeFlagRows renders rows grouped by first appearance, aligned on the widest left column.
func writeFlagRows(w io.Writer, rows []flagRow, s helpStyles) error {
	if len(rows) == 0 {
		return nil
	}
	maxLeft := 0
	for _, r := range rows {
		if len(r.left) > maxLeft {
			maxLeft = len(r.left)
		}
	}
	maxLeft += 2
	if maxLeft > 42 {
		maxLeft = 42
	}

	var groups []string
	seen := map[string]bool{}
	for _, r := range rows {
		if !seen[r.group] {
			seen[r.group] = true
			groups = append(groups, r.group)
		}
	}

	for _, g := range groups {
		if _, err := fmt.Fprintf(w, "\n    %s\n", s.section.Render(g)); err != nil {
			return err
		}
		for _, r := range rows {
			if r.group != g {
				continue
			}
			pad := maxLeft - len(r.left)
			if pad < 1 {
				pad = 1
			}
			line := s.flagName.Render(r.left) + strings.Repeat(" ", pad) + s.flagDoc.Render(r.doc)
			if r.def != "" {
				line += " " + s.flagDef.Render("(default "+r.def+")")
			}
			if _, err := fmt.Fprintf(w, "      %s\n", line); err != nil {
				return err
			}
		}
	}
	return nil
}

// defaultString renders p's built-in default as a short human string.
func defaultString(p spec.Param) string {
	v, ok := p.Default()
	if !ok {
		return ""
	}
	return formatDefault(v)
}

// formatDefault renders one built-in value, mapping zero durations and bytes to "unlimited".
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

// formatArgsStyled renders a verb's positional shape with required args highlighted.
func formatArgsStyled(raw string, s helpStyles) string {
	var out []string
	for _, tok := range strings.Fields(raw) {
		if strings.HasPrefix(tok, "<") && strings.HasSuffix(tok, ">") {
			out = append(out, s.reqArg.Render(tok))
			continue
		}
		out = append(out, s.arg.Render(tok))
	}
	return strings.Join(out, " ")
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
