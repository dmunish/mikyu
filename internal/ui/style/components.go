package style

import (
	"image/color"
	"math"
	"strings"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/charmbracelet/x/ansi"
)

// Three lipgloss behaviours the primitives below rely on, each verified by
// probe rather than assumed. A Style's Width already counts its border, so a
// bordered block is exactly as wide as the Width it is given and must not be
// reduced by two. Text is measured with lipgloss.Width, never len, or an
// ANSI-styled cell skews the layout. And table.New defaults to a full grid, so
// both render modes state the borders they want explicitly.

// Gauges are drawn with a fixed-width lane so a column of them lines up, and
// the caller prints the number separately in its own right-aligned lane.
const (
	gaugeFull    = "▓"
	gaugeEmpty   = "·"
	gaugeTick    = "▏"
	gaugeWarnAt  = 0.60
	gaugeDangerA = 0.85
)

// titledBoxChrome is the width a TitledBox spends on its own top edge: the
// "╭─ " opener, the space before the fill, the corner, and one fill dash.
const titledBoxChrome = 6

// TitledBox draws a card with its title set into the top edge, right title at the corner.
func (t Theme) TitledBox(title, right, body string, width int, titleColor color.Color) string {
	if t.Plain {
		head := title
		if right != "" {
			head = title + " · " + right
		}
		return joinNonEmpty(head, body)
	}
	edge := lipgloss.NewStyle().Foreground(titleColor)
	rightW := 0
	if right != "" {
		right = ansi.Truncate(right, max(1, width-titledBoxChrome-3), "…")
		rightW = lipgloss.Width(right)
	}
	budget := max(1, width-titledBoxChrome-rightW-1)
	shown := ansi.Truncate(title, budget, "…")
	head := edge.Render("╭─ ") + shown + edge.Render(" ")
	gap := width - (titledBoxChrome - 2) - lipgloss.Width(shown) - 1
	if right != "" {
		gap -= rightW + 1
	}
	if gap < 1 {
		gap = 1
	}
	top := head + edge.Render(strings.Repeat("─", gap))
	if right != "" {
		top += right + edge.Render(" ")
	}
	top += edge.Render("╮")
	rest := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder(), false, true, true, true).
		BorderForeground(titleColor).
		Width(width).
		Padding(0, 1).
		Render(body)
	return top + "\n" + rest
}

// GradientHeader is the single gradient surface on a screen (invariant I7).
func (t Theme) GradientHeader(title, subtitle1, subtitle2 string, width int) string {
	lines := []string{title, subtitle1, subtitle2}
	if t.Plain {
		return strings.Join(nonEmpty(lines), "\n")
	}
	body := lipgloss.NewStyle().Padding(0, 2).Render(strings.Join(lines, "\n"))
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForegroundBlend(t.Accent, t.Accent2).
		Width(width).
		Render(body)
}

// Stat is one value-and-label pair in a StatBand, carrying the color of its value.
type Stat struct {
	Value string
	Label string
	Color color.Color
}

// StatBand renders big values over dim labels, separated by a two-space gutter.
func (t Theme) StatBand(stats []Stat) string {
	if len(stats) == 0 {
		return ""
	}
	value := t.emph(t.Text)
	label := t.Paint(t.Label).Transform(strings.ToUpper)
	blocks := make([]string, 0, len(stats)*2-1)
	for i, stat := range stats {
		if i > 0 {
			blocks = append(blocks, "  ")
		}
		painted := value
		if stat.Color != nil {
			painted = t.emph(stat.Color)
		}
		blocks = append(blocks, lipgloss.JoinVertical(lipgloss.Center,
			painted.Render(stat.Value), label.Render(stat.Label)))
	}
	return lipgloss.JoinHorizontal(lipgloss.Bottom, blocks...)
}

// KVSection renders label/value rows, every value in its own right-aligned lane, which is where invariant I1 is enforced.
func (t Theme) KVSection(title string, rows [][2]string, labelWidth, valueWidth int, twoCol bool) string {
	if len(rows) == 0 {
		return ""
	}
	if !twoCol {
		return t.kvBlock(title, rows, labelWidth, valueWidth)
	}
	half := (len(rows) + 1) / 2
	left := t.kvBlock("", rows[:half], labelWidth, valueWidth)
	right := t.kvBlock("", rows[half:], labelWidth, valueWidth)
	gutter := strings.Repeat(" ", 4)
	body := lipgloss.JoinHorizontal(lipgloss.Top, left, gutter, right)
	if strings.TrimSpace(right) == "" {
		body = left
	}
	return joinNonEmpty(t.sectionTitle(title), body)
}

// DataTable lays out a table, right-aligning the columns the caller marks numeric and never transforming the headers.
func (t Theme) DataTable(headers []string, rows [][]string, numeric map[int]bool) string {
	if len(headers) == 0 {
		return ""
	}
	tbl := table.New().
		StyleFunc(func(row, col int) lipgloss.Style {
			cell := lipgloss.NewStyle().Padding(0, 1).TabWidth(0)
			switch {
			case row == table.HeaderRow:
				return t.emph(t.Accent).Padding(0, 1).TabWidth(0)
			case numeric[col]:
				return cell.Align(lipgloss.Right)
			default:
				return cell
			}
		}).
		Headers(headers...).
		Rows(rows...)
	// A pipe gets no rule; a terminal gets these two and no row rules, and with
	// no rows the two would land on top of each other, so keep only one.
	headerRule := !t.Plain && len(rows) > 0
	tbl = tbl.
		BorderTop(false).BorderLeft(false).BorderRight(false).BorderColumn(false).
		BorderRow(false).BorderHeader(headerRule).BorderBottom(!t.Plain)
	if !t.Plain {
		tbl = tbl.BorderStyle(t.Paint(t.Label))
	}
	return tbl.String()
}

// Gauge renders frac as a fixed-width bar, where ok marks a legitimately full bar such as a hot cache.
func (t Theme) Gauge(frac float64, width int, ok bool) string {
	if width < 1 {
		return ""
	}
	if frac < 0 || math.IsNaN(frac) {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	filled := int(frac*float64(width) + 0.5)
	bar := strings.Repeat(gaugeFull, filled) + strings.Repeat(gaugeEmpty, width-filled)
	return t.Paint(t.gaugeColor(frac, ok)).Render(gaugeTick + bar)
}

// Footer renders dim prose with accent command spans, or nothing in plain mode.
func (t Theme) Footer(hints ...[2]string) string {
	if t.Plain || len(hints) == 0 {
		return ""
	}
	dim := t.Paint(t.Label)
	parts := make([]string, 0, len(hints))
	for _, hint := range hints {
		parts = append(parts, t.emph(t.Accent).Render(hint[0])+" "+dim.Render(hint[1]))
	}
	return dim.Render(strings.Join(parts, dim.Render(" · ")))
}

// gaugeColor grades a bar by how full it is, or trusts the caller's ok flag.
func (t Theme) gaugeColor(frac float64, ok bool) color.Color {
	switch {
	case ok:
		return t.Ok
	case frac >= gaugeDangerA:
		return t.Danger
	case frac >= gaugeWarnAt:
		return t.Warn
	default:
		return t.Label
	}
}

// kvBlock renders one column of a key-value section, title optional.
func (t Theme) kvBlock(title string, rows [][2]string, labelWidth, valueWidth int) string {
	labelStyle := t.Paint(t.Label)
	valueStyle := lipgloss.NewStyle()
	lines := make([]string, 0, len(rows)+1)
	if title != "" {
		lines = append(lines, t.sectionTitle(title))
	}
	for _, row := range rows {
		lines = append(lines, labelStyle.Width(labelWidth).Render(row[0])+" "+
			valueStyle.Width(valueWidth).Align(lipgloss.Right).Render(row[1]))
	}
	return strings.Join(lines, "\n")
}

// sectionTitle renders an uppercase accent heading.
func (t Theme) sectionTitle(title string) string {
	return t.emph(t.Accent).Transform(strings.ToUpper).Render(title)
}

// SectionTitle renders an uppercase accent heading for a standalone section.
func (t Theme) SectionTitle(title string) string { return t.sectionTitle(title) }

// joinNonEmpty joins the non-empty parts with newlines.
func joinNonEmpty(parts ...string) string {
	return strings.Join(nonEmpty(parts), "\n")
}

// nonEmpty drops blank parts so a section never renders a hole.
func nonEmpty(parts []string) []string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	return kept
}
