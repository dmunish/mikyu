// Package style holds the shared color palette and text styles used by
// the CLI, the REPL, and the help renderer.
package style

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

// Palette names every color token used across the UI.
type Palette struct {
	Primary  color.Color   // amber: headers, prompts, labels
	Danger   color.Color   // pink: irreversible ops, required args
	Success  color.Color   // green: confirmations
	Error    color.Color   // red: errors
	Warning  color.Color   // amber: warnings
	Muted    color.Color   // grey: dim prose, secondary text
	Subtle   color.Color   // light grey: sub-labels, flag kinds
	Bright   color.Color   // near-white: primary values, verb names
	Gradient []color.Color // top-to-bottom fire ramp behind the banner
}

// Default is the shipped palette.
var Default = Palette{
	Primary: lipgloss.Color("#ffca86"),
	Danger:  lipgloss.Color("#e33f6a"),
	Success: lipgloss.Color("42"),
	Error:   lipgloss.Color("196"),
	Warning: lipgloss.Color("214"),
	Muted:   lipgloss.Color("243"),
	Subtle:  lipgloss.Color("245"),
	Bright:  lipgloss.Color("255"),
	Gradient: []color.Color{
		lipgloss.Color("#ffca86"),
		lipgloss.Color("#ff7a55"),
		lipgloss.Color("#e33f6a"),
		lipgloss.Color("#a12789"),
		lipgloss.Color("#5b0f89"),
		lipgloss.Color("#170d44"),
	},
}

// HelpStyles bundles every style a help page renders. The zero value is
// the no-color form; every field is a no-op lipgloss.Style.
type HelpStyles struct {
	Header      lipgloss.Style
	Name        lipgloss.Style
	Arg         lipgloss.Style
	RequiredArg lipgloss.Style
	OptionalArg lipgloss.Style
	Summary     lipgloss.Style
	Section     lipgloss.Style
	FlagSection lipgloss.Style
	FlagName    lipgloss.Style
	FlagDoc     lipgloss.Style
	FlagDefault lipgloss.Style
	Dim         lipgloss.Style
}

// NewHelpStyles returns help styles configured for color or plain output.
func NewHelpStyles(color bool) HelpStyles {
	if !color {
		return HelpStyles{}
	}
	return HelpStyles{
		Header:      lipgloss.NewStyle().Foreground(Default.Primary).Bold(true),
		Name:        lipgloss.NewStyle().Foreground(Default.Bright).Bold(true),
		Arg:         lipgloss.NewStyle().Foreground(Default.Subtle),
		RequiredArg: lipgloss.NewStyle().Foreground(Default.Danger),
		OptionalArg: lipgloss.NewStyle().Foreground(Default.Muted),
		Summary:     lipgloss.NewStyle().Foreground(Default.Muted),
		Section:     lipgloss.NewStyle().Foreground(Default.Primary).Bold(true),
		FlagSection: lipgloss.NewStyle().Foreground(Default.Primary),
		FlagName:    lipgloss.NewStyle().Foreground(Default.Bright),
		FlagDoc:     lipgloss.NewStyle().Foreground(Default.Muted),
		FlagDefault: lipgloss.NewStyle().Foreground(Default.Subtle),
		Dim:         lipgloss.NewStyle().Foreground(Default.Muted),
	}
}
