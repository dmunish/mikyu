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

// TestTitledBoxOverlongTitle: a title wider than the box is truncated, not wrapped.
func TestTitledBoxOverlongTitle(t *testing.T) {
	theme := NewTheme(true)
	long := strings.Repeat("W", 200)
	for _, width := range []int{20, 46, 80} {
		out := theme.TitledBox(long, "body", width, theme.Accent)
		lines := strings.Split(stripANSI(out), "\n")
		for i, line := range lines {
			assert.Equal(t, width, lipgloss.Width(line),
				"width %d line %d must never exceed the requested width", width, i)
		}
		assert.Contains(t, stripANSI(out), "…", "an overlong title is marked as truncated")
		assert.True(t, strings.HasSuffix(lines[0], "╮"), "the drawn edge must still close")
	}
}

// TestCardsNeverExceedWidth is the narrow-terminal guarantee from phase 6.
func TestCardsNeverExceedWidth(t *testing.T) {
	theme := NewTheme(true)
	body := strings.Repeat("x", 400)
	header := strings.Repeat("h", 400)
	for _, width := range []int{30, 60, 80, 100} {
		for name, card := range map[string]string{
			"titled": theme.TitledBox(header, body, width, theme.Accent),
			"header": theme.GradientHeader(header, body, body, width),
		} {
			for i, line := range strings.Split(stripANSI(card), "\n") {
				assert.LessOrEqual(t, lipgloss.Width(line), width,
					"%s card at width %d line %d overflows", name, width, i)
			}
		}
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

	one := strings.Split(stripANSI(theme.StatBand([]Stat{{Value: "1.2M", Label: "messages"}})), "\n")
	require.Len(t, one, 2)
	assert.Equal(t, "1.2M", strings.TrimSpace(one[0]))
	assert.Equal(t, "MESSAGES", strings.TrimSpace(one[1]))

	three := strings.Split(stripANSI(theme.StatBand([]Stat{
		{Value: "3", Label: "queues"}, {Value: "1.2M", Label: "messages"}, {Value: "9.0 GiB", Label: "on disk"},
	})), "\n")
	require.Len(t, three, 2, "values share one line, labels share the next")
	for _, want := range []string{"3", "1.2M", "9.0 GiB"} {
		assert.Contains(t, three[0], want)
	}
	for _, want := range []string{"QUEUES", "MESSAGES", "ON DISK"} {
		assert.Contains(t, three[1], want)
	}
}

// TestStatBandSingleLine: one stat renders value over label, not beside.
func TestStatBandSingleLine(t *testing.T) {
	lines := strings.Split(stripANSI(PlainTheme().StatBand([]Stat{{Value: "42", Label: "queues"}})), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, "42", strings.TrimSpace(lines[0]))
	assert.Equal(t, "QUEUES", strings.TrimSpace(lines[1]))
}

// TestStatBandPerStatColor: a colored value differs from an uncolored one.
func TestStatBandPerStatColor(t *testing.T) {
	theme := NewTheme(true)
	valueColor := func(stats []Stat) string { return sgr(strings.Split(theme.StatBand(stats), "\n")[0]) }
	base := valueColor([]Stat{{Value: "1", Label: "a"}})
	ok := valueColor([]Stat{{Value: "1", Label: "a", Color: theme.Ok}})
	danger := valueColor([]Stat{{Value: "1", Label: "a", Color: theme.Danger}})
	assert.NotEqual(t, base, ok, "a nil color falls back to the default")
	assert.NotEqual(t, ok, danger, "each stat carries its own color")
	assert.NotEmpty(t, base)
	assert.Empty(t, sgr(strings.Split(PlainTheme().StatBand(
		[]Stat{{Value: "1", Label: "a", Color: theme.Ok}}), "\n")[0]), "plain mode ignores it")
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
	require.Len(t, out, 3, "plain mode renders no rules, so header plus two rows")
	for _, line := range out {
		assert.Equal(t, lipgloss.Width(out[0]), lipgloss.Width(line), "every line shares the table width")
	}
	assert.Contains(t, out[0], "NAME", "the caller's literal header survives")
	assert.Contains(t, out[0], "MESSAGES")
	assert.Contains(t, out[0], "STATE")
	// Right alignment means every value ends in the same column, whether it is
	// one digit or seven.
	endShort := strings.Index(out[1], "1") + len("1")
	endLong := strings.Index(out[2], "1000000") + len("1000000")
	assert.Equal(t, endLong, endShort, "both values must end at the same column: %q / %q", out[1], out[2])
	assert.NotEqual(t, strings.Index(out[1], "1"), strings.Index(out[2], "1")+1,
		"the short value must be pushed right, not left")
}

// TestDataTableHeadersAreLiteral pins invariant I5: no transform on headers.
func TestDataTableHeadersAreLiteral(t *testing.T) {
	out := strings.Split(PlainTheme().DataTable([]string{"appends/s"}, [][]string{{"1"}}, nil), "\n")
	require.NotEmpty(t, out)
	assert.Equal(t, "appends/s", strings.TrimSpace(out[0]), "headers must survive verbatim for grepping")
}

// TestDataTableRichAddsRules: a terminal gets a header rule and a bottom rule.
func TestDataTableRichAddsRules(t *testing.T) {
	theme := NewTheme(true)
	lines := strings.Split(stripANSI(theme.DataTable([]string{"NAME", "N"}, [][]string{{"a", "1"}}, map[int]bool{1: true})), "\n")
	require.Len(t, lines, 4, "header, rule, row, rule")
	assert.Contains(t, lines[0], "NAME")
	assert.Equal(t, strings.Repeat("─", lipgloss.Width(lines[0])), lines[1])
	assert.Contains(t, lines[2], "a")
	assert.Equal(t, lines[1], lines[3])
}

// TestDataTableRichWithoutRows: with no data the two rules would double up.
func TestDataTableRichWithoutRows(t *testing.T) {
	lines := strings.Split(stripANSI(NewTheme(true).DataTable([]string{"A", "B"}, nil, nil)), "\n")
	require.Len(t, lines, 2, "header plus a single rule")
	assert.Contains(t, lines[0], "A")
	assert.Equal(t, strings.Repeat("─", lipgloss.Width(lines[0])), lines[1])
}

func TestDataTableShortRowAndEmpty(t *testing.T) {
	out := strings.Split(PlainTheme().DataTable([]string{"A", "B", "C"}, [][]string{{"1"}}, nil), "\n")
	require.Len(t, out, 2)
	assert.Equal(t, lipgloss.Width(out[0]), lipgloss.Width(out[1]), "missing cells pad rather than vanish")
	assert.Contains(t, out[1], "1")
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
		theme.StatBand([]Stat{{Value: "1", Label: "x"}}) +
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
		theme.StatBand([]Stat{{Value: "3", Label: "queues"}, {Value: "1.2M", Label: "messages"}}),
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
		" NAME    MESSAGES  STATE ",
		" events   1000000  ●     ",
		" audit          0  ○     ",
		"▏▓▓▓▓····",
	}
	assert.Equal(t, strings.Join(want, "\n"), strings.Join(parts, "\n"))
	assert.NotContains(t, strings.Join(parts, "\n"), "\x1b")
	assert.Empty(t, theme.Footer([2]string{"--detail", "everything"}), "plain mode suppresses the footer")
}

// stripANSI removes escape sequences so a test can assert on layout alone.
func stripANSI(s string) string { return ansi.Strip(s) }
