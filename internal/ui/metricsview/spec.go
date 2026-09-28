package metricsview

import (
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// specValue is one spec field's value as read from a sparse layer.
type specValue struct {
	kind spec.Kind
	num  int64
	text string
}

// render formats the value the way the rest of the view speaks: humanized
// bytes and durations, "unlimited" for the zeros that mean unbounded.
func (v specValue) render(f human.Formatter) string {
	switch v.kind {
	case spec.KindBytes:
		if v.num == 0 {
			return "unlimited"
		}
		return f.Bytes(v.num)
	case spec.KindDuration:
		if v.num == 0 {
			return "unlimited"
		}
		return f.Duration(time.Duration(v.num))
	case spec.KindUint32, spec.KindInt:
		return f.Count(v.num)
	default:
		return v.text
	}
}

// specField names one spec row and reads it from any sparse layer.
type specField struct {
	group   string
	label   string
	logOnly bool
	get     func(*spec.Spec) (specValue, bool)
}

// specGroups orders the sub-titles the spec sections render in.
var specGroups = []string{"storage", "flush", "retention", "task"}

// specFields is the rendered spec surface: one row per tunable, in wire order.
var specFields = []specField{
	{"storage", "durability", false, enumField(func(s *spec.Spec) *spec.Durability { return s.Storage.Durability })},
	{"storage", "encryption", false, enumField(func(s *spec.Spec) *spec.Encryption { return s.Storage.Encryption })},
	{"storage", "compaction", true, enumField(func(s *spec.Spec) *spec.Compaction { return s.Storage.Compaction })},
	{"storage", "max-segment", false, bytesField(func(s *spec.Spec) *int64 { return s.Storage.MaxSegment })},
	{"storage", "max-segment-age", false, durationField(func(s *spec.Spec) *spec.Duration { return s.Storage.MaxSegmentAge })},
	{"storage", "max-payload", false, bytesField(func(s *spec.Spec) *int64 { return s.Storage.MaxPayload })},
	{"storage", "max-cache", false, bytesField(func(s *spec.Spec) *int64 { return s.Storage.MaxCache })},
	{"flush", "max-bytes", false, bytesField(func(s *spec.Spec) *int64 { return s.Flush.MaxBytes })},
	{"flush", "max-wait", false, durationField(func(s *spec.Spec) *spec.Duration { return s.Flush.MaxWait })},
	{"retention", "max-age", false, durationField(func(s *spec.Spec) *spec.Duration { return s.Retention.MaxAge })},
	{"retention", "max-bytes", false, bytesField(func(s *spec.Spec) *int64 { return s.Retention.MaxBytes })},
	{"task", "lease", false, durationField(func(s *spec.Spec) *spec.Duration { return s.Task.Lease })},
	{"task", "max-attempts", false, countField(func(s *spec.Spec) *uint32 { return s.Task.MaxAttempts })},
	{"task", "max-in-flight", false, countField(func(s *spec.Spec) *uint32 { return s.Task.MaxInFlight })},
	{"task", "max-dead-letters", false, countField(func(s *spec.Spec) *uint32 { return s.Task.MaxDeadLetters })},
}

// enumField reads an enum-valued spec field.
func enumField[T ~string](pick func(*spec.Spec) *T) func(*spec.Spec) (specValue, bool) {
	return func(s *spec.Spec) (specValue, bool) {
		if p := pick(s); p != nil {
			return specValue{kind: spec.KindEnum, text: string(*p)}, true
		}
		return specValue{}, false
	}
}

// bytesField reads a byte-valued spec field.
func bytesField(pick func(*spec.Spec) *int64) func(*spec.Spec) (specValue, bool) {
	return func(s *spec.Spec) (specValue, bool) {
		if p := pick(s); p != nil {
			return specValue{kind: spec.KindBytes, num: *p}, true
		}
		return specValue{}, false
	}
}

// durationField reads a duration-valued spec field.
func durationField(pick func(*spec.Spec) *spec.Duration) func(*spec.Spec) (specValue, bool) {
	return func(s *spec.Spec) (specValue, bool) {
		if p := pick(s); p != nil {
			return specValue{kind: spec.KindDuration, num: int64(p.Duration())}, true
		}
		return specValue{}, false
	}
}

// countField reads a uint32-valued spec field.
func countField(pick func(*spec.Spec) *uint32) func(*spec.Spec) (specValue, bool) {
	return func(s *spec.Spec) (specValue, bool) {
		if p := pick(s); p != nil {
			return specValue{kind: spec.KindUint32, num: int64(*p)}, true
		}
		return specValue{}, false
	}
}

// sessionSpec renders the session defaults queue specs resolve against, sparse:
// only the fields the session sets appear (invariant I8).
func sessionSpec(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	parts := []string{}
	for _, group := range specGroups {
		var rows [][2]string
		for _, field := range specFieldsOf(group) {
			if v, ok := field.get(&snap.Session); ok {
				rows = append(rows, [2]string{field.label, v.render(f)})
			}
		}
		if len(rows) == 0 {
			continue
		}
		parts = append(parts, kv(t, group, rows, false, width))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(nonEmpty(append([]string{t.SectionTitle("effective spec")}, parts...)), "\n")
}

// queueSpecCard renders one queue's effective spec with the layer-diff markers
// of invariant I11: silence when the queue sets nothing, a faint tag when it
// matches the session, an override note when it disagrees.
func queueSpecCard(q metrics.QueueView, session spec.Spec, t style.Theme, f human.Formatter, width int) string {
	var groups []string
	for _, group := range specGroups {
		type row struct{ label, value, marker string }
		var rows []row
		for _, field := range specFieldsOf(group) {
			if q.IsTask() && field.logOnly {
				continue
			}
			eff, set := field.get(&q.Effective)
			if !set {
				continue
			}
			rows = append(rows, row{field.label, eff.render(f), specMarker(q, session, field, t, f)})
		}
		if len(rows) == 0 {
			continue
		}
		labelWidth, valueWidth := 0, 0
		for _, r := range rows {
			labelWidth = max(labelWidth, lipgloss.Width(r.label))
			valueWidth = max(valueWidth, lipgloss.Width(r.value))
		}
		cells := make([][2]string, 0, len(rows))
		for _, r := range rows {
			value := strings.Repeat(" ", valueWidth-lipgloss.Width(r.value)) + r.value
			if r.marker != "" {
				value += "  " + r.marker
			}
			cells = append(cells, [2]string{r.label, value})
		}
		groups = append(groups, t.KVSection(group, cells, labelWidth, 0, false))
	}
	body := strings.Join(nonEmpty(groups), "\n")
	if body == "" {
		return ""
	}
	return t.TitledBox("EFFECTIVE SPEC — "+q.Name, "", body, width, t.Accent)
}

// specMarker renders the layer-diff marker for one field: empty when the queue
// sets nothing, a faint session tag on agreement, an override note otherwise.
func specMarker(q metrics.QueueView, session spec.Spec, field specField, t style.Theme, f human.Formatter) string {
	stored, set := field.get(&q.Spec)
	if !set {
		return ""
	}
	sessionValue, sessionSet := field.get(&session)
	if sessionSet && sessionValue == stored {
		return t.Paint(t.Label).Render("· session")
	}
	marker := t.Paint(t.Accent).Render("← override")
	if sessionSet {
		marker += t.Paint(t.Accent).Render(" (session: " + sessionValue.render(f) + ")")
	}
	return marker
}

// specFieldsOf returns one group's fields in declaration order.
func specFieldsOf(group string) []specField {
	var out []specField
	for _, field := range specFields {
		if field.group == group {
			out = append(out, field)
		}
	}
	return out
}

// kv renders a section whose label and value lanes size to their widest cells.
// A two-column layout is only kept when both columns fit the given width.
func kv(t style.Theme, title string, rows [][2]string, twoCol bool, width int) string {
	if len(rows) == 0 {
		return ""
	}
	labelWidth, valueWidth := kvWidths(rows)
	if twoCol {
		twoCol = 2*(labelWidth+1+valueWidth)+4 <= width
	}
	return t.KVSection(title, rows, labelWidth, valueWidth, twoCol)
}

// kvWidths measures the widest label and value so every row shares the lanes.
func kvWidths(rows [][2]string) (labelWidth, valueWidth int) {
	for _, row := range rows {
		labelWidth = max(labelWidth, lipgloss.Width(row[0]))
		valueWidth = max(valueWidth, lipgloss.Width(row[1]))
	}
	return labelWidth, valueWidth
}
