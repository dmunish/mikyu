package style

import (
	"image/color"
	"math"
	"strings"

	"charm.land/lipgloss/v2"
)

// Gauges are drawn with a fixed-width lane so a column of them lines up, and
// the caller prints the number separately in its own right-aligned lane.
const (
	gaugeFull    = "▓"
	gaugeEmpty   = "·"
	gaugeTick    = "▏"
	gaugeWarnAt  = 0.60
	gaugeDangerA = 0.85
)

// TitledBox draws a card with its title set into the top edge, the btop form.
func (t Theme) TitledBox(title, body string, width int, titleColor color.Color) string {
	// Lipgloss cannot set a title inside a border, so the top edge is drawn by
	// hand and the other three sides come from lipgloss. Measure the title with
	// lipgloss.Width, never len, or an ANSI-styled title skews the edge. A
	// Style's Width already counts its border, so width passes through unchanged
	// to keep the drawn edge and the boxed body the same size.
	if t.Plain {
		return joinNonEmpty(title, body)
	}
	edge := lipgloss.NewStyle().Foreground(titleColor)
	titled := edge.Render("╭─ ") + title + edge.Render(" ")
	gap := width - lipgloss.Width(titled) - 1
	if gap < 1 {
		gap = 1
	}
	top := titled + edge.Render(strings.Repeat("─", gap)+"╮")
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

// StatBand renders big values over dim labels, separated by a two-space gutter.
func (t Theme) StatBand(stats [][2]string) string {
	if len(stats) == 0 {
		return ""
	}
	value := t.emph(t.Text)
	label := t.paint(t.Label).Transform(strings.ToUpper)
	blocks := make([]string, 0, len(stats)*2-1)
	for i, stat := range stats {
		if i > 0 {
			blocks = append(blocks, "  ")
		}
		blocks = append(blocks, lipgloss.JoinVertical(lipgloss.Center, value.Render(stat[0]), label.Render(stat[1])))
	}
	return lipgloss.JoinHorizontal(lipgloss.Bottom, blocks...)
}

// KVSection renders label/value rows with every value in its own right-aligned lane.
func (t Theme) KVSection(title string, rows [][2]string, labelWidth, valueWidth int, twoCol bool) string {
	// That lane is where invariant I1 is enforced, and it holds in plain mode
	// too, so a piped render keeps the alignment a rich one has.
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

// DataTable lays out a table, right-aligning the columns the caller marks numeric.
func (t Theme) DataTable(headers []string, rows [][]string, numeric map[int]bool) string {
	// Headers render as the caller's literal strings, never transformed, so a
	// script can grep them (invariant I5).
	if len(headers) == 0 {
		return ""
	}
	widths := columnWidths(headers, rows)
	var b strings.Builder
	b.WriteString(t.headerRow(headers, widths, numeric))
	if !t.Plain {
		b.WriteString("\n")
		b.WriteString(t.rule(widths))
	}
	for _, row := range rows {
		b.WriteString("\n")
		b.WriteString(t.dataRow(row, widths, numeric))
	}
	if !t.Plain && len(rows) > 0 {
		b.WriteString("\n")
		b.WriteString(t.rule(widths))
	}
	return b.String()
}

// Gauge renders frac as a fixed-width bar, trusting ok over the fullness grading.
func (t Theme) Gauge(frac float64, width int, ok bool) string {
	// ok marks a bar that is legitimately full, such as a hot read cache, so
	// fullness reads as health rather than risk.
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
	return t.paint(t.gaugeColor(frac, ok)).Render(gaugeTick + bar)
}

// Footer renders dim prose with accent command spans, or nothing in plain mode.
func (t Theme) Footer(hints ...[2]string) string {
	if t.Plain || len(hints) == 0 {
		return ""
	}
	dim := t.paint(t.Label)
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
	labelStyle := t.paint(t.Label)
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

// headerRow renders the caller-supplied column headers verbatim, styled once
// for the whole row so a table costs two escapes per row rather than per cell.
func (t Theme) headerRow(headers []string, widths []int, numeric map[int]bool) string {
	cells := make([]string, len(headers))
	for i, header := range headers {
		cells[i] = padCell(header, widths[i], false)
	}
	return t.emph(t.Accent).Render(strings.Join(cells, " "))
}

// dataRow renders one row, honoring the caller's per-column alignment.
func (t Theme) dataRow(row []string, widths []int, numeric map[int]bool) string {
	cells := make([]string, len(widths))
	for i := range widths {
		cell := ""
		if i < len(row) {
			cell = row[i]
		}
		cells[i] = padCell(cell, widths[i], numeric[i])
	}
	return strings.Join(cells, " ")
}

// rule renders a horizontal separator spanning the table.
func (t Theme) rule(widths []int) string {
	total := 0
	for i, width := range widths {
		if i > 0 {
			total++
		}
		total += width
	}
	return t.paint(t.Label).Render(strings.Repeat("─", total))
}

// columnWidths measures the widest cell in each column, header included.
func columnWidths(headers []string, rows [][]string) []int {
	widths := make([]int, len(headers))
	for i, header := range headers {
		widths[i] = lipgloss.Width(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				if width := lipgloss.Width(cell); width > widths[i] {
					widths[i] = width
				}
			}
		}
	}
	return widths
}

// padCell pads text to width, on the right when rightAlign is set.
func padCell(text string, width int, rightAlign bool) string {
	gap := width - lipgloss.Width(text)
	if gap <= 0 {
		return text
	}
	if rightAlign {
		return strings.Repeat(" ", gap) + text
	}
	return text + strings.Repeat(" ", gap)
}

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
