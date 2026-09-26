package style

import (
	"fmt"
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTitledBoxPlain(t *testing.T) {
	theme := PlainTheme()
	assert.Equal(t, "TITLE\nbody line", theme.TitledBox("TITLE", "body line", 40, theme.Accent))
	assert.Equal(t, "TITLE", theme.TitledBox("TITLE", "", 40, theme.Accent))
}

// TestTitledBoxOverlongTitle: a title wider than the box must not corrupt the edge.
func TestTitledBoxOverlongTitle(t *testing.T) {
	theme := NewTheme(true)
	long := strings.Repeat("W", 60)
	out := theme.TitledBox(long, "body", 20, theme.Accent)
	first := strings.Split(out, "\n")[0]
	assert.True(t, strings.HasSuffix(stripANSI(first), "╮"), "top edge must still close: %q", stripANSI(first))
	for _, line := range strings.Split(out, "\n") {
		assert.LessOrEqual(t, lipgloss.Width(line), lipgloss.Width(first), "line wider than the titled edge")
	}
}

// TestTitledBoxRichBorders: rich mode draws an edge title and a full box.
func TestTitledBoxRichBorders(t *testing.T) {
	theme := NewTheme(true)
	plain := stripANSI(theme.TitledBox("LOG QUEUES", "a  1", 30, theme.Accent))
	lines := strings.Split(plain, "\n")
	require.Len(t, lines, 3)
	assert.True(t, strings.HasPrefix(lines[0], "╭─ LOG QUEUES "), "got %q", lines[0])
	assert.True(t, strings.HasSuffix(lines[0], "╮"), "got %q", lines[0])
	assert.True(t, strings.HasPrefix(lines[1], "│ ") && strings.HasSuffix(lines[1], "│"), "got %q", lines[1])
	assert.Equal(t, "a  1", strings.TrimSpace(strings.Trim(lines[1], "│")))
	assert.True(t, strings.HasSuffix(lines[2], "╯"), "got %q", lines[2])
	for i, line := range lines {
		assert.Equal(t, 30, lipgloss.Width(line), "line %d must be exactly the declared width", i)
	}
}

func TestGradientHeader(t *testing.T) {
	theme := PlainTheme()
	assert.Equal(t, "TITLE\nsub one\nsub two",
		theme.GradientHeader("TITLE", "sub one", "sub two", 60))
	assert.Equal(t, "TITLE\nsub one", theme.GradientHeader("TITLE", "sub one", "", 60))

	rich := stripANSI(NewTheme(true).GradientHeader("TITLE", "one", "two", 40))
	lines := strings.Split(rich, "\n")
	require.Len(t, lines, 5, "top border, three content lines, bottom border")
	assert.Equal(t, "TITLE", strings.TrimSpace(unbox(lines[1])))
	assert.Equal(t, "one", strings.TrimSpace(unbox(lines[2])))
	assert.Equal(t, "two", strings.TrimSpace(unbox(lines[3])))
	for i, line := range lines {
		assert.Equal(t, 40, lipgloss.Width(line), "line %d must be exactly the declared width", i)
	}
}

// unbox strips a box's vertical edges and padding so a test can read the content.
func unbox(line string) string {
	return strings.Trim(strings.TrimSpace(line), "│")
}

func TestStatBand(t *testing.T) {
	theme := PlainTheme()
	assert.Equal(t, "", theme.StatBand(nil))

	one := strings.Split(stripANSI(theme.StatBand([][2]string{{"1.2M", "messages"}})), "\n")
	require.Len(t, one, 2)
	assert.Equal(t, "1.2M", strings.TrimSpace(one[0]))
	assert.Equal(t, "MESSAGES", strings.TrimSpace(one[1]))

	three := stripANSI(theme.StatBand([][2]string{{"3", "queues"}, {"1.2M", "messages"}, {"9.0 GiB", "on disk"}}))
	lines := strings.Split(three, "\n")
	require.Len(t, lines, 2, "values share one line, labels share the next")
	for _, want := range []string{"3", "1.2M", "9.0 GiB"} {
		assert.Contains(t, lines[0], want)
	}
	for _, want := range []string{"QUEUES", "MESSAGES", "ON DISK"} {
		assert.Contains(t, lines[1], want)
	}
}

// TestStatBandSingleLine: one stat renders value over label, not beside.
func TestStatBandSingleLine(t *testing.T) {
	lines := strings.Split(stripANSI(PlainTheme().StatBand([][2]string{{"42", "queues"}})), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, "42", strings.TrimSpace(lines[0]))
	assert.Equal(t, "QUEUES", strings.TrimSpace(lines[1]))
}

// TestKVSectionRightAlignsValues is the invariant I1 check: labels left, values right.
func TestKVSectionRightAlignsValues(t *testing.T) {
	theme := PlainTheme()
	rows := [][2]string{{"appends", "1"}, {"reads", "1000"}, {"bytes in", "123456789"}}
	out := strings.Split(stripANSI(theme.KVSection("throughput", rows, 10, 9, false)), "\n")
	require.Len(t, out, 4)
	assert.Equal(t, "THROUGHPUT", out[0])
	for i, row := range rows {
		assert.Equal(t, fmt.Sprintf("%-10s %9s", row[0], row[1]), out[i+1])
		assert.Equal(t, 20, lipgloss.Width(out[i+1]), "row %d must end in the same column", i)
	}
}

func TestKVSectionTwoColumn(t *testing.T) {
	theme := PlainTheme()
	rows := [][2]string{
		{"appends", "1"}, {"reads", "2"}, {"bytes in", "3"}, {"bytes out", "4"},
	}
	lines := strings.Split(stripANSI(theme.KVSection("throughput", rows, 10, 6, true)), "\n")
	require.Len(t, lines, 3, "title plus two rows")
	assert.Equal(t, "THROUGHPUT", lines[0])
	assert.Contains(t, lines[1], "appends")
	assert.Contains(t, lines[1], "bytes in")
	assert.NotContains(t, lines[1], "reads")
}

// TestKVSectionOddRowSplit: an odd row count puts the extra row on the left.
func TestKVSectionOddRowSplit(t *testing.T) {
	rows := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}
	lines := strings.Split(stripANSI(PlainTheme().KVSection("t", rows, 6, 4, true)), "\n")
	require.Len(t, lines, 3, "title plus two rows")
	assert.Contains(t, lines[1], "a", "first left row")
	assert.Contains(t, lines[1], "c", "the odd row is the only one on the right")
	assert.NotContains(t, lines[2], "a")
	assert.Contains(t, lines[2], "b", "second left row")
}

func TestKVSectionEmpty(t *testing.T) {
	assert.Equal(t, "", PlainTheme().KVSection("title", nil, 10, 10, false))
}

func TestDataTableAlignsNumericColumns(t *testing.T) {
	theme := PlainTheme()
	headers := []string{"NAME", "MESSAGES", "STATE"}
	rows := [][]string{
		{"a", "1", "●"},
		{"longer", "1000000", "○"},
	}
	out := strings.Split(theme.DataTable(headers, rows, map[int]bool{1: true}), "\n")
	require.Len(t, out, 3, "header plus two rows, no rules in plain mode")
	// Column widths come from the widest cell: NAME 6, MESSAGES 8, STATE 5.
	assert.Equal(t, "NAME  ", out[0][:6])
	assert.Equal(t, "a     ", out[1][:6])
	assert.Equal(t, "longer", out[2][:6])
	assert.Equal(t, "       1", out[1][7:15], "numeric column must be right-aligned")
	assert.Equal(t, " 1000000", out[2][7:15])
	for _, line := range out {
		assert.Equal(t, lipgloss.Width(out[0]), lipgloss.Width(line))
	}
}

// TestDataTableHeadersAreLiteral pins invariant I5: no transform on headers.
func TestDataTableHeadersAreLiteral(t *testing.T) {
	out := strings.Split(PlainTheme().DataTable([]string{"appends/s"}, [][]string{{"1"}}, nil), "\n")
	assert.Equal(t, "appends/s", out[0], "headers must survive verbatim for grepping")
}

func TestDataTableRichAddsRules(t *testing.T) {
	theme := NewTheme(true)
	lines := strings.Split(stripANSI(theme.DataTable([]string{"NAME", "N"}, [][]string{{"a", "1"}}, map[int]bool{1: true})), "\n")
	require.Len(t, lines, 4, "header, rule, row, rule")
	assert.Equal(t, "NAME N", lines[0])
	assert.Equal(t, "a    1", lines[2])
	assert.Equal(t, lipgloss.Width(lines[0]), lipgloss.Width(lines[1]), "rule must match the table width")
	assert.Equal(t, lipgloss.Width(lines[0]), lipgloss.Width(lines[2]))
	assert.Equal(t, lipgloss.Width(lines[0]), lipgloss.Width(lines[3]))
}

// TestDataTableRichWithoutRows: a header with no data still closes with one rule.
func TestDataTableRichWithoutRows(t *testing.T) {
	lines := strings.Split(stripANSI(NewTheme(true).DataTable([]string{"A"}, nil, nil)), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, "A", lines[0])
	assert.Equal(t, "─", lines[1])
}

func TestDataTableShortRowAndEmpty(t *testing.T) {
	out := strings.Split(PlainTheme().DataTable([]string{"A", "B", "C"}, [][]string{{"1"}}, nil), "\n")
	assert.Equal(t, "A B C", out[0])
	assert.Equal(t, "1    ", out[1], "missing cells pad rather than vanish")
	assert.Equal(t, "", PlainTheme().DataTable(nil, nil, nil))
}

func TestGauge(t *testing.T) {
	theme := PlainTheme()
	tests := []struct {
		name string
		frac float64
		want string
	}{
		{"empty", 0, gaugeTick + strings.Repeat(gaugeEmpty, 10)},
		{"full", 1, gaugeTick + strings.Repeat(gaugeFull, 10)},
		{"half rounds", 0.5, gaugeTick + strings.Repeat(gaugeFull, 5) + strings.Repeat(gaugeEmpty, 5)},
		{"clamped high", 2, gaugeTick + strings.Repeat(gaugeFull, 10)},
		{"clamped low", -1, gaugeTick + strings.Repeat(gaugeEmpty, 10)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, theme.Gauge(tt.frac, 10, false))
		})
	}
	assert.Equal(t, "", theme.Gauge(0.5, 0, false), "no lane, no bar")
}

// TestGaugeFixedWidth: every bar is the same display width so bars form a column.
func TestGaugeFixedWidth(t *testing.T) {
	theme := PlainTheme()
	for _, frac := range []float64{0, 0.01, 0.25, 0.5, 0.99, 1} {
		assert.Equal(t, 11, lipgloss.Width(theme.Gauge(frac, 10, false)), "frac %v", frac)
	}
}

// TestGaugeColorsByThreshold checks the grading behavior, not the color
// encoding, so a palette change does not fail this test.
func TestGaugeColorsByThreshold(t *testing.T) {
	theme := NewTheme(true)
	bar := func(frac float64, ok bool) string { return sgr(theme.Gauge(frac, 10, ok)) }
	calm, warm, hot := bar(0.1, false), bar(0.60, false), bar(0.85, false)
	assert.NotEqual(t, calm, warm, "crossing 0.60 must change the color")
	assert.NotEqual(t, warm, hot, "crossing 0.85 must change the color")
	// The three bands are [0,0.60) calm, [0.60,0.85) warn, [0.85,1] danger.
	assert.Equal(t, bar(0.1, false), bar(0.59, false), "both calm")
	assert.Equal(t, bar(0.60, false), bar(0.849, false), "both warn, 0.60 included")
	assert.Equal(t, bar(0.85, false), bar(1, false), "both danger, 0.85 included")
	assert.Equal(t, bar(0.3, true), bar(1, true), "ok holds across the whole range")
	assert.NotEqual(t, bar(1, false), bar(1, true), "ok must override the full-bar danger color")
	assert.NotEmpty(t, calm, "a rich gauge must actually be colored")
	assert.Empty(t, PlainTheme().Gauge(0.5, 10, false)[:0]+sgr(PlainTheme().Gauge(0.5, 10, false)),
		"a plain gauge must not be colored")
}

// sgr returns the escape sequence a styled fragment opens with, so a test can
// compare colors without depending on the fill glyphs.
func sgr(s string) string {
	if !strings.HasPrefix(s, "\x1b[") {
		return ""
	}
	if i := strings.IndexByte(s, 'm'); i >= 0 {
		return s[:i+1]
	}
	return ""
}

func TestFooter(t *testing.T) {
	assert.Equal(t, "", PlainTheme().Footer([2]string{"--detail", "everything"}), "plain mode is silent")
	assert.Equal(t, "", NewTheme(true).Footer(), "no hints, no footer")
	out := stripANSI(NewTheme(true).Footer([2]string{"--detail", "everything"}, [2]string{"json", "for tools"}))
	assert.Contains(t, out, "--detail")
	assert.Contains(t, out, "everything")
	assert.Contains(t, out, "json")
	assert.Contains(t, out, "for tools")
}

func TestHealthGlyphs(t *testing.T) {
	assert.Equal(t, GlyphOK, HealthGlyph(0))
	assert.Equal(t, GlyphWarn, HealthGlyph(1))
	assert.Equal(t, GlyphCritical, HealthGlyph(2))
	assert.Equal(t, GlyphOK, HealthGlyph(9), "unknown level reads healthy")
	assert.Equal(t, 3, len(map[string]bool{GlyphOK: true, GlyphWarn: true, GlyphCritical: true}),
		"glyph shapes must differ so grayscale still reads")
}

func TestThemePlainEmitsNoEscapes(t *testing.T) {
	theme := PlainTheme()
	rendered := theme.TitledBox("T", "body", 30, theme.Accent) +
		theme.GradientHeader("t", "a", "b", 30) +
		theme.StatBand([][2]string{{"1", "x"}}) +
		theme.KVSection("s", [][2]string{{"a", "1"}}, 6, 4, false) +
		theme.DataTable([]string{"A"}, [][]string{{"1"}}, nil) +
		theme.Gauge(0.5, 8, false) +
		theme.Footer([2]string{"a", "b"})
	assert.NotContains(t, rendered, "\x1b", "plain mode must never emit an escape")
}

// TestThemePlainKeepsAlignment: I1 holds in plain mode, since pipes are read too.
func TestThemePlainKeepsAlignment(t *testing.T) {
	rows := [][2]string{{"a", "1"}, {"bb", "1000"}}
	out := strings.Split(PlainTheme().KVSection("s", rows, 6, 6, false), "\n")
	require.Len(t, out, 3)
	assert.Equal(t, lipgloss.Width(out[1]), lipgloss.Width(out[2]))
}

// TestComponentsPlainProfile pins the colorless render of all seven primitives
// at one width, so a change to any primitive's plain form is a visible diff.
func TestComponentsPlainProfile(t *testing.T) {
	theme := PlainTheme()
	parts := []string{
		theme.TitledBox("LOG QUEUES", "events  ok", 46, theme.Accent),
		theme.GradientHeader("mikyu", "go1.27 linux/amd64", "up 1h 23m", 46),
		theme.StatBand([][2]string{{"3", "queues"}, {"1.2M", "messages"}}),
		theme.KVSection("throughput", [][2]string{{"appends", "12"}, {"reads", "340"}}, 8, 6, false),
		theme.DataTable([]string{"NAME", "MESSAGES", "STATE"},
			[][]string{{"events", "1000000", "●"}, {"audit", "0", "○"}},
			map[int]bool{1: true}),
		theme.Gauge(0.5, 8, false),
	}
	want := []string{
		"LOG QUEUES",
		"events  ok",
		"mikyu",
		"go1.27 linux/amd64",
		"up 1h 23m",
		"   3      1.2M  ",
		"QUEUES  MESSAGES",
		"THROUGHPUT",
		"appends      12",
		"reads       340",
		"NAME   MESSAGES STATE",
		"events  1000000 ●    ",
		"audit         0 ○    ",
		"▏▓▓▓▓····",
	}
	assert.Equal(t, strings.Join(want, "\n"), strings.Join(parts, "\n"))
	assert.NotContains(t, strings.Join(parts, "\n"), "\x1b")
	assert.Empty(t, theme.Footer([2]string{"--detail", "everything"}), "plain mode suppresses the footer")
}

// stripANSI removes escape sequences so a test can assert on layout alone.
func stripANSI(s string) string { return ansi.Strip(s) }
