package command

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

// AccentColor is the warm amber used for headers, prompts, and field labels.
var AccentColor = lipgloss.Color("#ffca86")

// DangerColor is the hot pink used for required args and irreversible tags.
var DangerColor = lipgloss.Color("#e33f6a")

// LogoGradient is the top-to-bottom fire ramp behind the REPL banner.
var LogoGradient = []color.Color{
	lipgloss.Color("#ffca86"),
	lipgloss.Color("#ff7a55"),
	lipgloss.Color("#e33f6a"),
	lipgloss.Color("#a12789"),
	lipgloss.Color("#5b0f89"),
	lipgloss.Color("#170d44"),
}

// helpStyles bundles every style a help page uses. A zero value is the
// no-color form: every field is a no-op lipgloss.Style.
type helpStyles struct {
	header    lipgloss.Style
	name      lipgloss.Style
	arg       lipgloss.Style
	reqArg    lipgloss.Style
	summary   lipgloss.Style
	section   lipgloss.Style
	flagName  lipgloss.Style
	flagDoc   lipgloss.Style
	flagDef   lipgloss.Style
	dim       lipgloss.Style
	usage     lipgloss.Style
	example   lipgloss.Style
	exampleC  lipgloss.Style
}

// newHelpStyles returns a helpStyles configured for color or plain output.
func newHelpStyles(color bool) helpStyles {
	if !color {
		return helpStyles{}
	}
	return helpStyles{
		header:   lipgloss.NewStyle().Foreground(AccentColor).Bold(true),
		name:     lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true),
		arg:      lipgloss.NewStyle().Foreground(lipgloss.Color("245")),
		reqArg:   lipgloss.NewStyle().Foreground(DangerColor),
		summary:  lipgloss.NewStyle().Foreground(lipgloss.Color("243")),
		section:  lipgloss.NewStyle().Foreground(AccentColor).Bold(true),
		flagName: lipgloss.NewStyle().Foreground(lipgloss.Color("255")),
		flagDoc:  lipgloss.NewStyle().Foreground(lipgloss.Color("243")),
		flagDef:  lipgloss.NewStyle().Foreground(lipgloss.Color("245")),
		dim:      lipgloss.NewStyle().Foreground(lipgloss.Color("243")),
		usage:    lipgloss.NewStyle().Foreground(lipgloss.Color("255")),
		example:  lipgloss.NewStyle().Foreground(AccentColor),
		exampleC: lipgloss.NewStyle().Foreground(lipgloss.Color("243")),
	}
}