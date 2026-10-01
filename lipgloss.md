## ![](https://pkg.go.dev/static/shared/icon/chrome_reader_mode_gm_grey_24dp.svg)  README  [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#section-readme "Go to Readme")

### Lip Gloss

![](https://github.com/user-attachments/assets/d13bbe1a-d2b2-4d18-9302-419a0bc3f579)

[![Latest Release](https://img.shields.io/github/release/charmbracelet/lipgloss.svg)](https://github.com/charmbracelet/lipgloss/releases)[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](https://pkg.go.dev/charm.land/lipgloss/v2?tab=doc)[![Build Status](https://github.com/charmbracelet/lipgloss/workflows/build/badge.svg)](https://github.com/charmbracelet/lipgloss/actions)

Style definitions for nice terminal layouts. Built with TUIs in mind.

![Lip Gloss example](https://github.com/user-attachments/assets/92560e60-d70e-4ce0-b39e-a60bb933356b)

Lip Gloss takes an expressive, declarative approach to terminal rendering.
Users familiar with CSS will feel at home with Lip Gloss.

```
import "charm.land/lipgloss/v2"

var style = lipgloss.NewStyle().
    Bold(true).
    Foreground(lipgloss.Color("#FAFAFA")).
    Background(lipgloss.Color("#7D56F4")).
    PaddingTop(2).
    PaddingLeft(4).
    Width(22)

lipgloss.Println(style.Render("Hello, kitty"))
```

#### Installation

```
go get charm.land/lipgloss/v2
```

> \[!TIP\]
>
> Upgrading from v1? Check out the [upgrade guide](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/UPGRADE_GUIDE_V2.md), or
> point your LLM at it and let it go to town.

#### Colors

Lip Gloss supports the following color profiles:

##### ANSI 16 colors (4-bit)

```
lipgloss.Color("5")  // magenta
lipgloss.Color("9")  // red
lipgloss.Color("12") // light blue
```

##### ANSI 256 Colors (8-bit)

```
lipgloss.Color("86")  // aqua
lipgloss.Color("201") // hot pink
lipgloss.Color("202") // orange
```

##### True Color (16,777,216 colors; 24-bit)

```
lipgloss.Color("#0000FF") // good ol' 100% blue
lipgloss.Color("#04B575") // a green
lipgloss.Color("#3C3C3C") // a dark gray
```

...as well as a 1-bit ASCII profile, which is black and white only.

There are also named constants for the 16 standard ANSI colors:

```
lipgloss.Black
lipgloss.Red
lipgloss.Green
lipgloss.Yellow
lipgloss.Blue
lipgloss.Magenta
lipgloss.Cyan
lipgloss.White
lipgloss.BrightBlack
lipgloss.BrightRed
lipgloss.BrightGreen
lipgloss.BrightYellow
lipgloss.BrightBlue
lipgloss.BrightMagenta
lipgloss.BrightCyan
lipgloss.BrightWhite
```

##### Automatically Downsampling Colors

Some users don't have Truecolor terminals. Other times, output might not
support color at all (for example, in logs). Lip Gloss was designed to handle
this gracefully by automatically downsampling colors to the best available
profile.

If you're using Lip Gloss with Bubble Tea, there’s nothing to do. If you're
using Lip Gloss standalone, just use `lipgloss.Println` or `lipgloss.Sprint`
(and their variants).

For more, see [advanced color usage](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#readme-advanced-color-usage).

##### Color Utilities

Lip Gloss ships with a handful of handy tools for working with colors:

```
c := lipgloss.Color("#EB4268")      // Sriracha sauce color
dark := lipgloss.Darken(c, 0.5)     // dark Sriracha sauce
light := lipgloss.Lighten(c, 0.35)  // light Sriracha sauce
green := lipgloss.Complementary(c)  // greenish Sriracha sauce
withAlpha := lipgloss.Alpha(c, 0.2) // watered down Sriracha sauce
```

##### Advanced Color Tooling

Lip Gloss also supports color blending, automatically choosing light or dark
variants of colors at runtime, and a lot more. For details, see [Advanced Color\\
Usage](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#readme-advanced-color-usage) and [the docs](https://pkg.go.dev/charm.land/lipgloss/v2?tab=doc).

#### Inline Formatting

Lip Gloss supports the usual ANSI text formatting options:

```
var style = lipgloss.NewStyle().
    Bold(true).
    Italic(true).
    Faint(true).
    Blink(true).
    Strikethrough(true).
    Underline(true).
    Reverse(true)
```

##### Underline Styles

Beyond simple on/off, underlines support multiple styles and custom colors:

```
s := lipgloss.NewStyle().
    UnderlineStyle(lipgloss.UnderlineCurly).
    UnderlineColor(lipgloss.Color("#FF0000"))
```

Available styles: `UnderlineNone`, `UnderlineSingle`, `UnderlineDouble`,
`UnderlineCurly`, `UnderlineDotted`, `UnderlineDashed`.

##### Hyperlinks

Styles can render clickable hyperlinks in supporting terminals:

```
s := lipgloss.NewStyle().
    Foreground(lipgloss.Color("#7B2FBE")).
    Hyperlink("https://charm.land")

lipgloss.Println(s.Render("Visit Charm"))
```

In unsupported terminals this will degrade gracefully and hyperlinks will
simply not render.

#### Block-Level Formatting

Lip Gloss also supports rules for block-level formatting:

```
// Padding
var style = lipgloss.NewStyle().
    PaddingTop(2).
    PaddingRight(4).
    PaddingBottom(2).
    PaddingLeft(4)

// Margins
var style = lipgloss.NewStyle().
    MarginTop(2).
    MarginRight(4).
    MarginBottom(2).
    MarginLeft(4)
```

There is also shorthand syntax for margins and padding, which follows the same
format as CSS:

```
// 2 cells on all sides
lipgloss.NewStyle().Padding(2)

// 2 cells on the top and bottom, 4 cells on the left and right
lipgloss.NewStyle().Margin(2, 4)

// 1 cell on the top, 4 cells on the sides, 2 cells on the bottom
lipgloss.NewStyle().Padding(1, 4, 2)

// Clockwise, starting from the top: 2 cells on the top, 4 on the right, 3 on
// the bottom, and 1 on the left
lipgloss.NewStyle().Margin(2, 4, 3, 1)
```

You can also customize the characters used for padding and margin fill:

```
s := lipgloss.NewStyle().
    Padding(1, 2).
    PaddingChar('·').
    Margin(1, 2).
    MarginChar('░')
```

#### Aligning Text

You can align paragraphs of text to the left, right, or center.

```
var style = lipgloss.NewStyle().
    Width(24).
    Align(lipgloss.Left).  // align it left
    Align(lipgloss.Right). // no wait, align it right
    Align(lipgloss.Center) // just kidding, align it in the center
```

#### Width and Height

Setting a minimum width and height is simple and straightforward.

```
var style = lipgloss.NewStyle().
    SetString("What’s for lunch?").
    Width(24).
    Height(32).
    Foreground(lipgloss.Color("63"))
```

#### Borders

Adding borders is easy:

```
// Add a purple, rectangular border
var style = lipgloss.NewStyle().
    BorderStyle(lipgloss.NormalBorder()).
    BorderForeground(lipgloss.Color("63"))

// Set a rounded, yellow-on-purple border to the top and left
var anotherStyle = lipgloss.NewStyle().
    BorderStyle(lipgloss.RoundedBorder()).
    BorderForeground(lipgloss.Color("228")).
    BorderBackground(lipgloss.Color("63")).
    BorderTop(true).
    BorderLeft(true)

// Make your own border
var myCuteBorder = lipgloss.Border{
    Top:         "._.:*:",
    Bottom:      "._.:*:",
    Left:        "|*",
    Right:       "|*",
    TopLeft:     "*",
    TopRight:    "*",
    BottomLeft:  "*",
    BottomRight: "*",
}
```

There are also shorthand functions for defining borders, which follow a similar
pattern to the margin and padding shorthand functions.

```
// Add a thick border to the top and bottom
lipgloss.NewStyle().
    Border(lipgloss.ThickBorder(), true, false)

// Add a double border to the top and left sides. Rules are set clockwise
// from top.
lipgloss.NewStyle().
    Border(lipgloss.DoubleBorder(), true, false, false, true)
```

You can also pass multiple colors to a border for a gradient effect:

```
s := lipgloss.NewStyle().
    Border(lipgloss.RoundedBorder()).
    BorderForegroundBlend(lipgloss.Color("#FF0000"), lipgloss.Color("#0000FF"))
```

For more on borders see [the docs](https://pkg.go.dev/charm.land/lipgloss/v2#Border).

#### Copying Styles

Just use assignment:

```
style := lipgloss.NewStyle().Foreground(lipgloss.Color("219"))

copiedStyle := style // this is a true copy

wildStyle := style.Blink(true) // this is also true copy, with blink added
```

Since `Style` is a pure value type, assigning a style to another effectively
creates a new copy of the style without mutating the original.

#### Inheritance

Styles can inherit rules from other styles. When inheriting, only unset rules
on the receiver are inherited.

```
var styleA = lipgloss.NewStyle().
    Foreground(lipgloss.Color("229")).
    Background(lipgloss.Color("63"))

// Only the background color will be inherited here, because the foreground
// color will have been already set:
var styleB = lipgloss.NewStyle().
    Foreground(lipgloss.Color("201")).
    Inherit(styleA)
```

#### Unsetting Rules

All rules can be unset:

```
var style = lipgloss.NewStyle().
    Bold(true).                        // make it bold
    UnsetBold().                       // jk don't make it bold
    Background(lipgloss.Color("227")). // yellow background
    UnsetBackground()                  // never mind
```

When a rule is unset, it won’t be inherited or copied.

#### Enforcing Rules

Sometimes, such as when developing a component, you want to make sure style
definitions respect their intended purpose in the UI. This is where `Inline`
and `MaxWidth`, and `MaxHeight` come in:

```
// Force rendering onto a single line, ignoring margins, padding, and borders.
someStyle.Inline(true).Render("yadda yadda")

// Also limit rendering to five cells
someStyle.Inline(true).MaxWidth(5).Render("yadda yadda")

// Limit rendering to a 5x5 cell block
someStyle.MaxWidth(5).MaxHeight(5).Render("yadda yadda")
```

#### Tabs

The tab character (`\t`) is rendered differently in different terminals (often
as 8 spaces, sometimes 4). Because of this inconsistency, Lip Gloss converts
tabs to 4 spaces at render time. This behavior can be changed on a per-style
basis, however:

```
style := lipgloss.NewStyle() // tabs will render as 4 spaces, the default
style = style.TabWidth(2)    // render tabs as 2 spaces
style = style.TabWidth(0)    // remove tabs entirely
style = style.TabWidth(lipgloss.NoTabConversion) // leave tabs intact
```

#### Wrapping

The `Wrap` function wraps text while preserving ANSI styles and hyperlinks
across line boundaries:

```
wrapped := lipgloss.Wrap(styledText, 40, " ")
```

#### Rendering

Generally, you just call the `Render(string...)` method on a `lipgloss.Style`:

```
style := lipgloss.NewStyle().Bold(true).SetString("Hello,")
lipgloss.Println(style.Render("kitty.")) // Hello, kitty.
lipgloss.Println(style.Render("puppy.")) // Hello, puppy.
```

But you could also use the Stringer interface:

```
var style = lipgloss.NewStyle().SetString("你好，猫咪。").Bold(true)
lipgloss.Println(style) // 你好，猫咪。
```

#### Utilities

In addition to pure styling, Lip Gloss also ships with some utilities to help
assemble your layouts.

##### Compositing

![xx](https://github.com/user-attachments/assets/1921bac6-2408-436a-9d9e-7930fe4c6ec9)

Lip Gloss includes a powerful, cell-based compositor for rendering layered
content:

```
// Create some layers.
a := lipgloss.NewLayer(pickles).X(4).Y(2).Z(1)
b := lipgloss.NewLayer(bitterMelon).X(22).Y(1)
c := lipgloss.NewLayer(sriracha).X(11).Y(7)

// Composite 'em and render.
output := compositor.Compose(a, b, c).Render()
```

For a more thorough example, see [the canvas\\
example](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/examples/canvas/main.go). For reference, including how to detect
mouse clicks on layers, see [the docs](https://pkg.go.dev/charm.land/lipgloss/v2?tab=doc).

##### Joining Paragraphs

Horizontally and vertically joining paragraphs is a cinch.

```
// Horizontally join three paragraphs along their bottom edges
lipgloss.JoinHorizontal(lipgloss.Bottom, paragraphA, paragraphB, paragraphC)

// Vertically join two paragraphs along their center axes
lipgloss.JoinVertical(lipgloss.Center, paragraphA, paragraphB)

// Horizontally join three paragraphs, with the shorter ones aligning 20%
// from the top of the tallest
lipgloss.JoinHorizontal(0.2, paragraphA, paragraphB, paragraphC)
```

##### Measuring Width and Height

Sometimes you’ll want to know the width and height of text blocks when building
your layouts.

```
// Render a block of text.
var style = lipgloss.NewStyle().
    Width(40).
    Padding(2)
var block string = style.Render(someLongString)

// Get the actual, physical dimensions of the text block.
width := lipgloss.Width(block)
height := lipgloss.Height(block)

// Here's a shorthand function.
w, h := lipgloss.Size(block)
```

##### Blending Colors

You can blend colors in one or two dimensions for gradient effects:

```
// 1-dimentinoal gradient
colors := lipgloss.Blend1D(10, lipgloss.Color("#FF0000"), lipgloss.Color("#0000FF"))

// 2-dimensional gradient with rotation
colors := lipgloss.Blend2D(80, 24, 45.0, color1, color2, color3)
```

##### Placing Text in Whitespace

Sometimes you’ll simply want to place a block of text in whitespace. This is
a lightweight alternative to compositing.

```
// Center a paragraph horizontally in a space 80 cells wide. The height of
// the block returned will be as tall as the input paragraph.
block := lipgloss.PlaceHorizontal(80, lipgloss.Center, fancyStyledParagraph)

// Place a paragraph at the bottom of a space 30 cells tall. The width of
// the text block returned will be as wide as the input paragraph.
block := lipgloss.PlaceVertical(30, lipgloss.Bottom, fancyStyledParagraph)

// Place a paragraph in the bottom right corner of a 30x80 cell space.
block := lipgloss.Place(30, 80, lipgloss.Right, lipgloss.Bottom, fancyStyledParagraph)
```

You can also style the whitespace. For details, see [the docs](https://pkg.go.dev/charm.land/lipgloss/v2?tab=doc).

#### Rendering Tables

Lip Gloss ships with a table rendering sub-package.

```
import "charm.land/lipgloss/v2/table"
```

Define some rows of data.

```
rows := [][]string{
    {"Chinese", "您好", "你好"},
    {"Japanese", "こんにちは", "やあ"},
    {"Arabic", "أهلين", "أهلا"},
    {"Russian", "Здравствуйте", "Привет"},
    {"Spanish", "Hola", "¿Qué tal?"},
}
```

Use the table package to style and render the table.

```
var (
    purple    = lipgloss.Color("99")
    gray      = lipgloss.Color("245")
    lightGray = lipgloss.Color("241")

    headerStyle  = lipgloss.NewStyle().Foreground(purple).Bold(true).Align(lipgloss.Center)
    cellStyle    = lipgloss.NewStyle().Padding(0, 1).Width(14)
    oddRowStyle  = cellStyle.Foreground(gray)
    evenRowStyle = cellStyle.Foreground(lightGray)
)

t := table.New().
    Border(lipgloss.NormalBorder()).
    BorderStyle(lipgloss.NewStyle().Foreground(purple)).
    StyleFunc(func(row, col int) lipgloss.Style {
        switch {
        case row == table.HeaderRow:
            return headerStyle
        case row%2 == 0:
            return evenRowStyle
        default:
            return oddRowStyle
        }
    }).
    Headers("LANGUAGE", "FORMAL", "INFORMAL").
    Rows(rows...)

// You can also add tables row-by-row
t.Row("English", "You look absolutely fabulous.", "How's it going?")
```

Print the table.

```
lipgloss.Println(t)
```

![Table Example](https://github.com/charmbracelet/lipgloss/assets/42545625/6e4b70c4-f494-45da-a467-bdd27df30d5d)

##### Table Borders

There are helpers to generate tables in markdown or ASCII style:

###### Markdown Table

```
table.New().Border(lipgloss.MarkdownBorder()).BorderTop(false).BorderBottom(false)
```

```
| LANGUAGE |    FORMAL    | INFORMAL  |
|----------|--------------|-----------|
| Chinese  | Nǐn hǎo      | Nǐ hǎo    |
| French   | Bonjour      | Salut     |
| Russian  | Zdravstvuyte | Privet    |
| Spanish  | Hola         | ¿Qué tal? |
```

###### ASCII Table

```
table.New().Border(lipgloss.ASCIIBorder())
```

```
+----------+--------------+-----------+
| LANGUAGE |    FORMAL    | INFORMAL  |
+----------+--------------+-----------+
| Chinese  | Nǐn hǎo      | Nǐ hǎo    |
| French   | Bonjour      | Salut     |
| Russian  | Zdravstvuyte | Privet    |
| Spanish  | Hola         | ¿Qué tal? |
+----------+--------------+-----------+
```

For more on tables see [the docs](https://pkg.go.dev/charm.land/lipgloss/v2?tab=doc) and [examples](https://github.com/charmbracelet/lipgloss/tree/master/examples/table).

#### Rendering Lists

Lip Gloss ships with a list rendering sub-package.

```
import "charm.land/lipgloss/v2/list"
```

Define a new list.

```
l := list.New("A", "B", "C")
```

Print the list.

```
lipgloss.Println(l)

// • A
// • B
// • C
```

Lists have the ability to nest.

```
l := list.New(
    "A", list.New("Artichoke"),
    "B", list.New("Baking Flour", "Bananas", "Barley", "Bean Sprouts"),
    "C", list.New("Cashew Apple", "Cashews", "Coconut Milk", "Curry Paste", "Currywurst"),
    "D", list.New("Dill", "Dragonfruit", "Dried Shrimp"),
    "E", list.New("Eggs"),
    "F", list.New("Fish Cake", "Furikake"),
    "J", list.New("Jicama"),
    "K", list.New("Kohlrabi"),
    "L", list.New("Leeks", "Lentils", "Licorice Root"),
)
```

Print the list.

```
lipgloss.Println(l)
```

![image](https://github.com/charmbracelet/lipgloss/assets/42545625/0dc9f440-0748-4151-a3b0-7dcf29dfcdb0)

Lists can be customized via their enumeration function as well as using
`lipgloss.Style`s.

```
enumeratorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("99")).MarginRight(1)
itemStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("212")).MarginRight(1)

l := list.New(
    "Glossier",
    "Claire's Boutique",
    "Nyx",
    "Mac",
    "Milk",
    ).
    Enumerator(list.Roman).
    EnumeratorStyle(enumeratorStyle).
    ItemStyle(itemStyle)
```

Print the list.

![List example](https://github.com/charmbracelet/lipgloss/assets/42545625/360494f1-57fb-4e13-bc19-0006efe01561)

In addition to the predefined enumerators (`Arabic`, `Alphabet`, `Roman`, `Bullet`, `Tree`),
you may also define your own custom enumerator:

```
l := list.New("Duck", "Duck", "Duck", "Duck", "Goose", "Duck", "Duck")

func DuckDuckGooseEnumerator(l list.Items, i int) string {
    if l.At(i).Value() == "Goose" {
        return "Honk →"
    }
    return ""
}

l = l.Enumerator(DuckDuckGooseEnumerator)
```

Print the list:

![image](https://github.com/charmbracelet/lipgloss/assets/42545625/157aaf30-140d-4948-9bb4-dfba46e5b87e)

If you need, you can also build lists incrementally:

```
l := list.New()

for i := 0; i < repeat; i++ {
    l.Item("Lip Gloss")
}
```

#### Rendering Trees

Lip Gloss ships with a tree rendering sub-package.

```
import "charm.land/lipgloss/v2/tree"
```

Define a new tree.

```
t := tree.Root(".").
    Child("A", "B", "C")
```

Print the tree.

```
lipgloss.Println(t)

// .
// ├── A
// ├── B
// └── C
```

Trees have the ability to nest.

```
t := tree.Root(".").
    Child("macOS").
    Child(
        tree.New().
            Root("Linux").
            Child("NixOS").
            Child("Arch Linux (btw)").
            Child("Void Linux"),
        ).
    Child(
        tree.New().
            Root("BSD").
            Child("FreeBSD").
            Child("OpenBSD"),
    )
```

Print the tree.

```
lipgloss.Println(t)
```

![Tree Example (simple)](https://github.com/user-attachments/assets/5ef14eb8-a5d4-4f94-8834-e15d1e714f89)

Trees can be customized via their enumeration function as well as using
`lipgloss.Style`s.

```
enumeratorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("63")).MarginRight(1)
rootStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("35"))
itemStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("212"))

t := tree.
    Root("⁜ Makeup").
    Child(
        "Glossier",
        "Fenty Beauty",
        tree.New().Child(
            "Gloss Bomb Universal Lip Luminizer",
            "Hot Cheeks Velour Blushlighter",
        ),
        "Nyx",
        "Mac",
        "Milk",
    ).
    Enumerator(tree.RoundedEnumerator).
    EnumeratorStyle(enumeratorStyle).
    RootStyle(rootStyle).
    ItemStyle(itemStyle)
```

Print the tree.

![Tree Example (makeup)](https://github.com/user-attachments/assets/06d12d87-744a-4c89-bd98-45de9094a97e)

The predefined enumerators for trees are `DefaultEnumerator` and `RoundedEnumerator`.

If you need, you can also build trees incrementally:

```
t := tree.New()

for i := 0; i < repeat; i++ {
    t.Child("Lip Gloss")
}
```

#### Advanced Color Usage

One of the most powerful features of Lip Gloss is the ability to render
different colors at runtime depending on the user's terminal and environment,
allowing you to present the best possible user experience.

This section shows you how to do exactly that.

Migrating from v1?

The `compat` package provides `AdaptiveColor`, `CompleteColor`, and
`CompleteAdaptiveColor` for a quicker migration from v1. These work by
looking at `stdin` and `stdout` on a global basis:

```
import "charm.land/lipgloss/v2/compat"

color := compat.AdaptiveColor{
    Light: lipgloss.Color("#f1f1f1"),
    Dark:  lipgloss.Color("#cccccc"),
}
```

Note that we don't recommend this for new code as it removes the purity from
Lip Gloss, computationally speaking, as it removes transparency around when
I/O happens, which could cause Lip Gloss to compete for resources (like stdin)
with other tools.

##### Adaptive Colors

You can render different colors at runtime depending on whether the terminal
has a light or dark background:

```
hasDarkBG := lipgloss.HasDarkBackground(os.Stdin, os.Stdout)
lightDark := lipgloss.LightDark(hasDarkBG)

myColor := lightDark(lipgloss.Color("#D7FFAE"), lipgloss.Color("#D75FEE"))
```

###### With Bubble Tea

In Bubble Tea, request the background color, listen for a
`BackgroundColorMsg`, and respond accordingly:

```
func (m model) Init() tea.Cmd {
    // First, send a Cmd to request the terminal background color.
    return tea.RequestBackgroundColor
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
    switch msg := msg.(type) {
    case tea.BackgroundColorMsg:
        // Great, we have the background color. Now we can set up our styles
        // against the color.
        m.styles = newStyles(msg.IsDark())
        return m, nil
    }
}

func newStyles(bgIsDark bool) styles {
    // A little ternary function that will return the appropriate color
    // based on the background color.
    lightDark := lipgloss.LightDark(bgIsDark)

    return styles{
        myHotStyle: lipgloss.NewStyle().Foreground(lightDark(
            lipgloss.Color("#f1f1f1"),
            lipgloss.Color("#333333"),
        )),
    }
}
```

###### Standalone

If you’re not using Bubble Tea you can perform the query manually:

```
// What's the background color?
hasDarkBG := lipgloss.HasDarkBackground(os.Stdin, os.Stderr)

// A helper function that will return the appropriate color based on the
// background.
lightDark := lipgloss.LightDark(hasDarkBG)

// A couple colors with light and dark variants.
thisColor := lightDark(lipgloss.Color("#C5ADF9"), lipgloss.Color("#864EFF"))
thatColor := lightDark(lipgloss.Color("#37CD96"), lipgloss.Color("#22C78A"))

a := lipgloss.NewStyle().Foreground(thisColor).Render("this")
b := lipgloss.NewStyle().Foreground(thatColor).Render("that")

// Render the appropriate colors at runtime:
lipgloss.Fprintf(os.Stderr, "my fave colors are %s and %s", a, b)
```

##### Complete Colors

In some cases where you may want to specify exact values for each color profile
(ANSI 16, ANSI 156, and TrueColor). For these cases, use the `Complete` helper:

```
// You'll need the colorprofile package.
import "github.com/charmbracelet/colorprofile"

// Get the color profile.
profile := colorprofile.Detect(os.Stdout, os.Environ())

// Create a function for rendering the appropriate color based on the profile.
var completeColor := lipgloss.Complete(profile)

// Now we'll choose the appropriate color at runtime.
myColor := completeColor(ansiColor, ansi256Color, trueColor)
```

##### Color Downsampling

One of the best things about Lip Gloss is that it can automatically downsample
colors to the best available profile, stripping colors (and ANSI) entirely when
output is not a TTY.

If you’re using Lip Gloss with Bubble Tea there’s nothing to do here:
downsampling is built into Bubble Tea v2. If you’re not using Bubble Tea, use
the Lip Gloss writer functions, which are a drop-in replacement for the `fmt`
package:

```
s := lipgloss.NewStyle()
    .Foreground(lipgloss.Color("#EB4268"))
    .Render("Hello!")

// Downsample if needed and print to stdout.
lipgloss.Println(s)

// Render to a variable.
downsampled := lipgloss.Sprint(s)

// Print to stderr.
lipgloss.Fprint(os.Stderr, s)
```

The full set: `Print`, `Println`, `Printf`, `Fprint`, `Fprintln`, `Fprintf`,
`Sprint`, `Sprintln`, `Sprintf`.

Need more control? Check out
[Colorprofile](https://github.com/charmbracelet/colorprofile), which Lip Gloss
uses under the hood.

#### What about [Bubble Tea](https://github.com/charmbracelet/bubbletea)?

Lip Gloss doesn’t replace Bubble Tea. Rather, it is an excellent Bubble Tea
companion. It was designed to make assembling terminal user interface views as
simple and fun as possible so that you can focus on building your application
instead of concerning yourself with low-level layout details.

In simple terms, you can use Lip Gloss to help build your Bubble Tea views.

#### Rendering Markdown

For a more document-centric rendering solution with support for things like
lists, tables, and syntax-highlighted code have a look at [Glamour](https://github.com/charmbracelet/glamour),
the stylesheet-based Markdown renderer.

#### Contributing

See [contributing](https://github.com/charmbracelet/lipgloss/contribute).

#### Feedback

We’d love to hear your thoughts on this project. Feel free to drop us a note!

- [Discord](https://charm.land/chat)
- [Matrix](https://charm.land/matrix)

#### License

[MIT](https://github.com/charmbracelet/lipgloss/raw/master/LICENSE)

* * *

Part of [Charm](https://charm.land/).

[![The Charm logo](https://stuff.charm.sh/charm-banner-next.jpg)](https://charm.land/)

Charm热爱开源 • Charm loves open source

Expand ▾Collapse ▴

## ![](https://pkg.go.dev/static/shared/icon/code_gm_grey_24dp.svg)  Documentation  [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#section-documentation "Go to Documentation")

[Rendered for](https://go.dev/about#build-context)linux/amd64windows/amd64darwin/amd64js/wasm

### Overview [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#pkg-overview "Go to Overview")

Package lipgloss provides style definitions for nice terminal layouts. Built
with TUIs in mind.

### Index [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#pkg-index "Go to Index")

- [Constants](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#pkg-constants)
- [Variables](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#pkg-variables)
- [func Alpha(c color.Color, alpha float64) color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Alpha)
- [func BackgroundColor(in term.File, out term.File) (bg color.Color, err error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#BackgroundColor)
- [func Blend1D(steps int, stops ...color.Color) \[\]color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Blend1D)
- [func Blend2D(width, height int, angle float64, stops ...color.Color) \[\]color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Blend2D)
- [func Color(s string) color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Color)
- [func Complementary(c color.Color) color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Complementary)
- [func Darken(c color.Color, percent float64) color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Darken)
- [func EnableLegacyWindowsANSI(\*os.File)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#EnableLegacyWindowsANSI)
- [func Fprint(w io.Writer, v ...any) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Fprint)
- [func Fprintf(w io.Writer, format string, v ...any) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Fprintf)
- [func Fprintln(w io.Writer, v ...any) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Fprintln)
- [func HasDarkBackground(in term.File, out term.File) bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#HasDarkBackground)
- [func Height(str string) int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Height)
- [func JoinHorizontal(pos Position, strs ...string) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#JoinHorizontal)
- [func JoinVertical(pos Position, strs ...string) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#JoinVertical)
- [func Lighten(c color.Color, percent float64) color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Lighten)
- [func Place(width, height int, hPos, vPos Position, str string, opts ...WhitespaceOption) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Place)
- [func PlaceHorizontal(width int, pos Position, str string, opts ...WhitespaceOption) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#PlaceHorizontal)
- [func PlaceVertical(height int, pos Position, str string, opts ...WhitespaceOption) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#PlaceVertical)
- [func Print(v ...any) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Print)
- [func Printf(format string, v ...any) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Printf)
- [func Println(v ...any) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Println)
- [func Size(str string) (width, height int)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Size)
- [func Sprint(v ...any) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Sprint)
- [func Sprintf(format string, v ...any) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Sprintf)
- [func Sprintln(v ...any) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Sprintln)
- [func StyleRanges(s string, ranges ...Range) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#StyleRanges)
- [func StyleRunes(str string, indices \[\]int, matched, unmatched Style) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#StyleRunes)
- [func Width(str string) (width int)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Width)
- [func Wrap(s string, width int, breakpoints string) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Wrap)
- [type ANSIColor](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#ANSIColor)
- [type Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Border)
  - [func ASCIIBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#ASCIIBorder)
  - [func BlockBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#BlockBorder)
  - [func DoubleBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#DoubleBorder)
  - [func HiddenBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#HiddenBorder)
  - [func InnerHalfBlockBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#InnerHalfBlockBorder)
  - [func MarkdownBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#MarkdownBorder)
  - [func NormalBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NormalBorder)
  - [func OuterHalfBlockBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#OuterHalfBlockBorder)
  - [func RoundedBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#RoundedBorder)
  - [func ThickBorder() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#ThickBorder)
  - [func (b Border) GetBottomSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Border.GetBottomSize)
  - [func (b Border) GetLeftSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Border.GetLeftSize)
  - [func (b Border) GetRightSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Border.GetRightSize)
  - [func (b Border) GetTopSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Border.GetTopSize)
- [type Canvas](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas)
  - [func NewCanvas(width, height int) \*Canvas](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NewCanvas)
  - [func (c \*Canvas) Bounds() uv.Rectangle](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Bounds)
  - [func (c \*Canvas) CellAt(x int, y int) \*uv.Cell](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.CellAt)
  - [func (c \*Canvas) Clear()](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Clear)
  - [func (c \*Canvas) Compose(drawer uv.Drawable) \*Canvas](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Compose)
  - [func (c \*Canvas) Draw(scr uv.Screen, area uv.Rectangle)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Draw)
  - [func (c \*Canvas) Height() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Height)
  - [func (c \*Canvas) Render() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Render)
  - [func (c \*Canvas) Resize(width, height int)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Resize)
  - [func (c \*Canvas) SetCell(x int, y int, cell \*uv.Cell)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.SetCell)
  - [func (c \*Canvas) Width() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.Width)
  - [func (c \*Canvas) WidthMethod() uv.WidthMethod](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas.WidthMethod)
- [type CompleteFunc](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#CompleteFunc)
  - [func Complete(p colorprofile.Profile) CompleteFunc](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Complete)
- [type Compositor](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor)
  - [func NewCompositor(layers ...\*Layer) \*Compositor](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NewCompositor)
  - [func (c \*Compositor) AddLayers(layers ...\*Layer) \*Compositor](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.AddLayers)
  - [func (c \*Compositor) Bounds() image.Rectangle](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.Bounds)
  - [func (c \*Compositor) Draw(scr uv.Screen, area image.Rectangle)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.Draw)
  - [func (c \*Compositor) GetLayer(id string) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.GetLayer)
  - [func (c \*Compositor) Hit(x, y int) LayerHit](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.Hit)
  - [func (c \*Compositor) Refresh()](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.Refresh)
  - [func (c \*Compositor) Render() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Compositor.Render)
- [type Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer)
  - [func NewLayer(content string, layers ...\*Layer) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NewLayer)
  - [func (l \*Layer) AddLayers(layers ...\*Layer) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.AddLayers)
  - [func (l \*Layer) Draw(scr uv.Screen, area uv.Rectangle)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.Draw)
  - [func (l \*Layer) GetContent() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.GetContent)
  - [func (l \*Layer) GetID() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.GetID)
  - [func (l \*Layer) GetLayer(id string) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.GetLayer)
  - [func (l \*Layer) GetX() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.GetX)
  - [func (l \*Layer) GetY() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.GetY)
  - [func (l \*Layer) GetZ() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.GetZ)
  - [func (l \*Layer) Height() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.Height)
  - [func (l \*Layer) ID(id string) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.ID)
  - [func (l \*Layer) MaxZ() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.MaxZ)
  - [func (l \*Layer) Width() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.Width)
  - [func (l \*Layer) X(x int) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.X)
  - [func (l \*Layer) Y(y int) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.Y)
  - [func (l \*Layer) Z(z int) \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer.Z)
- [type LayerHit](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LayerHit)
  - [func (lh LayerHit) Bounds() image.Rectangle](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LayerHit.Bounds)
  - [func (lh LayerHit) Empty() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LayerHit.Empty)
  - [func (lh LayerHit) ID() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LayerHit.ID)
  - [func (lh LayerHit) Layer() \*Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LayerHit.Layer)
- [type LightDarkFunc](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LightDarkFunc)
  - [func LightDark(isDark bool) LightDarkFunc](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LightDark)
- [type NoColor](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NoColor)
  - [func (n NoColor) RGBA() (r, g, b, a uint32)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NoColor.RGBA)
- [type Position](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Position)
- [type RGBColor](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#RGBColor)
  - [func (c RGBColor) RGBA() (r, g, b, a uint32)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#RGBColor.RGBA)
- [type Range](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Range)
  - [func NewRange(start, end int, style Style) Range](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NewRange)
- [type Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style)
  - [func NewStyle() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NewStyle)
  - [func (s Style) Align(p ...Position) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Align)
  - [func (s Style) AlignHorizontal(p Position) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.AlignHorizontal)
  - [func (s Style) AlignVertical(p Position) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.AlignVertical)
  - [func (s Style) Background(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Background)
  - [func (s Style) Blink(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Blink)
  - [func (s Style) Bold(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Bold)
  - [func (s Style) Border(b Border, sides ...bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Border)
  - [func (s Style) BorderBackground(c ...color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderBackground)
  - [func (s Style) BorderBottom(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderBottom)
  - [func (s Style) BorderBottomBackground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderBottomBackground)
  - [func (s Style) BorderBottomForeground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderBottomForeground)
  - [func (s Style) BorderForeground(c ...color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderForeground)
  - [func (s Style) BorderForegroundBlend(c ...color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderForegroundBlend)
  - [func (s Style) BorderForegroundBlendOffset(v int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderForegroundBlendOffset)
  - [func (s Style) BorderLeft(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderLeft)
  - [func (s Style) BorderLeftBackground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderLeftBackground)
  - [func (s Style) BorderLeftForeground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderLeftForeground)
  - [func (s Style) BorderRight(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderRight)
  - [func (s Style) BorderRightBackground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderRightBackground)
  - [func (s Style) BorderRightForeground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderRightForeground)
  - [func (s Style) BorderStyle(b Border) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderStyle)
  - [func (s Style) BorderTop(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderTop)
  - [func (s Style) BorderTopBackground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderTopBackground)
  - [func (s Style) BorderTopForeground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.BorderTopForeground)
  - [func (s Style) ColorWhitespace(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.ColorWhitespace) deprecated
  - [func (s Style) Copy() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Copy) deprecated
  - [func (s Style) Faint(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Faint)
  - [func (s Style) Foreground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Foreground)
  - [func (s Style) GetAlign() Position](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetAlign)
  - [func (s Style) GetAlignHorizontal() Position](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetAlignHorizontal)
  - [func (s Style) GetAlignVertical() Position](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetAlignVertical)
  - [func (s Style) GetBackground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBackground)
  - [func (s Style) GetBlink() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBlink)
  - [func (s Style) GetBold() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBold)
  - [func (s Style) GetBorder() (b Border, top, right, bottom, left bool)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorder)
  - [func (s Style) GetBorderBottom() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderBottom)
  - [func (s Style) GetBorderBottomBackground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderBottomBackground)
  - [func (s Style) GetBorderBottomForeground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderBottomForeground)
  - [func (s Style) GetBorderBottomSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderBottomSize)
  - [func (s Style) GetBorderForegroundBlend() \[\]color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderForegroundBlend)
  - [func (s Style) GetBorderForegroundBlendOffset() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderForegroundBlendOffset)
  - [func (s Style) GetBorderLeft() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderLeft)
  - [func (s Style) GetBorderLeftBackground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderLeftBackground)
  - [func (s Style) GetBorderLeftForeground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderLeftForeground)
  - [func (s Style) GetBorderLeftSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderLeftSize)
  - [func (s Style) GetBorderRight() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderRight)
  - [func (s Style) GetBorderRightBackground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderRightBackground)
  - [func (s Style) GetBorderRightForeground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderRightForeground)
  - [func (s Style) GetBorderRightSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderRightSize)
  - [func (s Style) GetBorderStyle() Border](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderStyle)
  - [func (s Style) GetBorderTop() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderTop)
  - [func (s Style) GetBorderTopBackground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderTopBackground)
  - [func (s Style) GetBorderTopForeground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderTopForeground)
  - [func (s Style) GetBorderTopSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderTopSize)
  - [func (s Style) GetBorderTopWidth() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetBorderTopWidth) deprecated
  - [func (s Style) GetColorWhitespace() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetColorWhitespace)
  - [func (s Style) GetFaint() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetFaint)
  - [func (s Style) GetForeground() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetForeground)
  - [func (s Style) GetFrameSize() (x, y int)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetFrameSize)
  - [func (s Style) GetHeight() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetHeight)
  - [func (s Style) GetHorizontalBorderSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetHorizontalBorderSize)
  - [func (s Style) GetHorizontalFrameSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetHorizontalFrameSize)
  - [func (s Style) GetHorizontalMargins() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetHorizontalMargins)
  - [func (s Style) GetHorizontalPadding() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetHorizontalPadding)
  - [func (s Style) GetHyperlink() (link, params string)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetHyperlink)
  - [func (s Style) GetInline() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetInline)
  - [func (s Style) GetItalic() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetItalic)
  - [func (s Style) GetMargin() (top, right, bottom, left int)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMargin)
  - [func (s Style) GetMarginBottom() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMarginBottom)
  - [func (s Style) GetMarginChar() rune](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMarginChar)
  - [func (s Style) GetMarginLeft() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMarginLeft)
  - [func (s Style) GetMarginRight() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMarginRight)
  - [func (s Style) GetMarginTop() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMarginTop)
  - [func (s Style) GetMaxHeight() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMaxHeight)
  - [func (s Style) GetMaxWidth() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetMaxWidth)
  - [func (s Style) GetPadding() (top, right, bottom, left int)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetPadding)
  - [func (s Style) GetPaddingBottom() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetPaddingBottom)
  - [func (s Style) GetPaddingChar() rune](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetPaddingChar)
  - [func (s Style) GetPaddingLeft() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetPaddingLeft)
  - [func (s Style) GetPaddingRight() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetPaddingRight)
  - [func (s Style) GetPaddingTop() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetPaddingTop)
  - [func (s Style) GetReverse() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetReverse)
  - [func (s Style) GetStrikethrough() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetStrikethrough)
  - [func (s Style) GetStrikethroughSpaces() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetStrikethroughSpaces)
  - [func (s Style) GetTabWidth() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetTabWidth)
  - [func (s Style) GetTransform() func(string) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetTransform)
  - [func (s Style) GetUnderline() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetUnderline)
  - [func (s Style) GetUnderlineColor() color.Color](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetUnderlineColor)
  - [func (s Style) GetUnderlineSpaces() bool](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetUnderlineSpaces)
  - [func (s Style) GetUnderlineStyle() Underline](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetUnderlineStyle)
  - [func (s Style) GetVerticalBorderSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetVerticalBorderSize)
  - [func (s Style) GetVerticalFrameSize() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetVerticalFrameSize)
  - [func (s Style) GetVerticalMargins() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetVerticalMargins)
  - [func (s Style) GetVerticalPadding() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetVerticalPadding)
  - [func (s Style) GetWidth() int](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.GetWidth)
  - [func (s Style) Height(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Height)
  - [func (s Style) Hyperlink(link string, params ...string) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Hyperlink)
  - [func (s Style) Inherit(i Style) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Inherit)
  - [func (s Style) Inline(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Inline)
  - [func (s Style) Italic(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Italic)
  - [func (s Style) Margin(i ...int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Margin)
  - [func (s Style) MarginBackground(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MarginBackground)
  - [func (s Style) MarginBottom(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MarginBottom)
  - [func (s Style) MarginChar(r rune) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MarginChar)
  - [func (s Style) MarginLeft(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MarginLeft)
  - [func (s Style) MarginRight(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MarginRight)
  - [func (s Style) MarginTop(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MarginTop)
  - [func (s Style) MaxHeight(n int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MaxHeight)
  - [func (s Style) MaxWidth(n int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.MaxWidth)
  - [func (s Style) Padding(i ...int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Padding)
  - [func (s Style) PaddingBottom(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.PaddingBottom)
  - [func (s Style) PaddingChar(r rune) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.PaddingChar)
  - [func (s Style) PaddingLeft(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.PaddingLeft)
  - [func (s Style) PaddingRight(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.PaddingRight)
  - [func (s Style) PaddingTop(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.PaddingTop)
  - [func (s Style) Render(strs ...string) string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Render)
  - [func (s Style) Reverse(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Reverse)
  - [func (s Style) SetString(strs ...string) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.SetString)
  - [func (s Style) Strikethrough(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Strikethrough)
  - [func (s Style) StrikethroughSpaces(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.StrikethroughSpaces)
  - [func (s Style) String() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.String)
  - [func (s Style) TabWidth(n int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.TabWidth)
  - [func (s Style) Transform(fn func(string) string) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Transform)
  - [func (s Style) Underline(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Underline)
  - [func (s Style) UnderlineColor(c color.Color) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnderlineColor)
  - [func (s Style) UnderlineSpaces(v bool) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnderlineSpaces)
  - [func (s Style) UnderlineStyle(u Underline) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnderlineStyle)
  - [func (s Style) UnsetAlign() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetAlign)
  - [func (s Style) UnsetAlignHorizontal() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetAlignHorizontal)
  - [func (s Style) UnsetAlignVertical() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetAlignVertical)
  - [func (s Style) UnsetBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBackground)
  - [func (s Style) UnsetBlink() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBlink)
  - [func (s Style) UnsetBold() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBold)
  - [func (s Style) UnsetBorderBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderBackground)
  - [func (s Style) UnsetBorderBottom() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderBottom)
  - [func (s Style) UnsetBorderBottomBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderBottomBackground)
  - [func (s Style) UnsetBorderBottomForeground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderBottomForeground)
  - [func (s Style) UnsetBorderForeground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderForeground)
  - [func (s Style) UnsetBorderForegroundBlend() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderForegroundBlend)
  - [func (s Style) UnsetBorderForegroundBlendOffset() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderForegroundBlendOffset)
  - [func (s Style) UnsetBorderLeft() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderLeft)
  - [func (s Style) UnsetBorderLeftBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderLeftBackground)
  - [func (s Style) UnsetBorderLeftForeground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderLeftForeground)
  - [func (s Style) UnsetBorderRight() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderRight)
  - [func (s Style) UnsetBorderRightBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderRightBackground)
  - [func (s Style) UnsetBorderRightForeground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderRightForeground)
  - [func (s Style) UnsetBorderStyle() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderStyle)
  - [func (s Style) UnsetBorderTop() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderTop)
  - [func (s Style) UnsetBorderTopBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderTopBackground)
  - [func (s Style) UnsetBorderTopBackgroundColor() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderTopBackgroundColor) deprecated
  - [func (s Style) UnsetBorderTopForeground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetBorderTopForeground)
  - [func (s Style) UnsetColorWhitespace() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetColorWhitespace)
  - [func (s Style) UnsetFaint() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetFaint)
  - [func (s Style) UnsetForeground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetForeground)
  - [func (s Style) UnsetHeight() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetHeight)
  - [func (s Style) UnsetHyperlink() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetHyperlink)
  - [func (s Style) UnsetInline() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetInline)
  - [func (s Style) UnsetItalic() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetItalic)
  - [func (s Style) UnsetMarginBackground() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMarginBackground)
  - [func (s Style) UnsetMarginBottom() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMarginBottom)
  - [func (s Style) UnsetMarginLeft() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMarginLeft)
  - [func (s Style) UnsetMarginRight() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMarginRight)
  - [func (s Style) UnsetMarginTop() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMarginTop)
  - [func (s Style) UnsetMargins() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMargins)
  - [func (s Style) UnsetMaxHeight() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMaxHeight)
  - [func (s Style) UnsetMaxWidth() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetMaxWidth)
  - [func (s Style) UnsetPadding() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetPadding)
  - [func (s Style) UnsetPaddingBottom() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetPaddingBottom)
  - [func (s Style) UnsetPaddingChar() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetPaddingChar)
  - [func (s Style) UnsetPaddingLeft() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetPaddingLeft)
  - [func (s Style) UnsetPaddingRight() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetPaddingRight)
  - [func (s Style) UnsetPaddingTop() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetPaddingTop)
  - [func (s Style) UnsetReverse() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetReverse)
  - [func (s Style) UnsetStrikethrough() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetStrikethrough)
  - [func (s Style) UnsetStrikethroughSpaces() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetStrikethroughSpaces)
  - [func (s Style) UnsetString() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetString)
  - [func (s Style) UnsetTabWidth() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetTabWidth)
  - [func (s Style) UnsetTransform() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetTransform)
  - [func (s Style) UnsetUnderline() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetUnderline)
  - [func (s Style) UnsetUnderlineSpaces() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetUnderlineSpaces)
  - [func (s Style) UnsetWidth() Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnsetWidth)
  - [func (s Style) Value() string](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Value)
  - [func (s Style) Width(i int) Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Width)
- [type Underline](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Underline)
- [type WhitespaceOption](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WhitespaceOption)
  - [func WithWhitespaceChars(s string) WhitespaceOption](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WithWhitespaceChars)
  - [func WithWhitespaceStyle(s Style) WhitespaceOption](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WithWhitespaceStyle)
- [type WrapWriter](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WrapWriter)
  - [func NewWrapWriter(w io.Writer) \*WrapWriter](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NewWrapWriter)
  - [func (w \*WrapWriter) Close() error](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WrapWriter.Close)
  - [func (w \*WrapWriter) Link() uv.Link](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WrapWriter.Link)
  - [func (w \*WrapWriter) Style() uv.Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WrapWriter.Style)
  - [func (w \*WrapWriter) Write(p \[\]byte) (int, error)](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WrapWriter.Write)

### Constants [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#pkg-constants "Go to Constants")

[View Source](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go#L23)

```
const (
	Black ansi.BasicColor = iota
	Red
	Green
	Yellow
	Blue
	Magenta
	Cyan
	White

	BrightBlack
	BrightRed
	BrightGreen
	BrightYellow
	BrightBlue
	BrightMagenta
	BrightCyan
	BrightWhite
)
```

4-bit color constants.

[View Source](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go#L119)

```
const (
	// UnderlineNone is no underline.
	UnderlineNone = ansi.UnderlineNone
	// UnderlineSingle is a single underline. This is the default when underline is enabled.
	UnderlineSingle = ansi.UnderlineSingle
	// UnderlineDouble is a double underline.
	UnderlineDouble = ansi.UnderlineDouble
	// UnderlineCurly is a curly underline.
	UnderlineCurly = ansi.UnderlineCurly
	// UnderlineDotted is a dotted underline.
	UnderlineDotted = ansi.UnderlineDotted
	// UnderlineDashed is a dashed underline.
	UnderlineDashed = ansi.UnderlineDashed
)
```

[View Source](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go#L11)

```
const (
	// NBSP is the non-breaking space rune.
	NBSP = '\u00A0'
)
```

[View Source](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go#L770)

```
const NoTabConversion = -1
```

NoTabConversion can be passed to [Style.TabWidth](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.TabWidth) to disable the replacement
of tabs with spaces at render time.

### Variables [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#pkg-variables "Go to Variables")

[View Source](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go#L14)

```
var Writer = colorprofile.NewWriter(os.Stdout, os.Environ())
```

Writer is the default writer that prints to stdout, automatically
downsampling colors when necessary.

### Functions [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#pkg-functions "Go to Functions")

#### func [Alpha](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L292) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Alpha "Go to Alpha")

```
func Alpha(c color.Color, alpha float64) color.Color
```

Alpha adjusts the alpha value of a color using a 0-1 (clamped) float scale
0 = transparent, 1 = opaque.

#### func [BackgroundColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/query.go\#L34) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#BackgroundColor "Go to BackgroundColor")

```
func BackgroundColor(in term.File, out term.File) (bg color.Color, err error)
```

BackgroundColor queries the terminal's background color. Typically, you'll
want to query against stdin and either stdout or stderr, depending on what
you're writing to.

This function is intended for standalone Lip Gloss use only. If you're using
Bubble Tea, listen for tea.BackgroundColorMsg in your update function.

#### func [Blend1D](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/blending.go\#L18) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Blend1D "Go to Blend1D")

```
func Blend1D(steps int, stops ...color.Color) []color.Color
```

Blend1D blends a series of colors together in one linear dimension using multiple
stops, into the provided number of steps. Uses the "CIE L\*, a\*, b\*" (CIELAB) color-space.

Note that if any of the provided colors are completely transparent, we will
assume that the alpha value was lost in conversion from RGB -> RGBA, and we
will set the alpha to opaque, as it's not possible to blend something completely
transparent.

#### func [Blend2D](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/blending.go\#L114) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Blend2D "Go to Blend2D")

```
func Blend2D(width, height int, angle float64, stops ...color.Color) []color.Color
```

Blend2D blends a series of colors together in two linear dimensions using
multiple stops, into the provided width/height. Uses the "CIE L\*, a\*, b\*" (CIELAB)
color-space. The angle parameter controls the rotation of the gradient (0-360°),
where 0° is left-to-right, 45° is bottom-left to top-right (diagonal). The function
returns colors in a 1D row-major order (\[row1, row2, row3, ...\]).

Example of how to iterate over the result:

```
gradient := colors.Blend2D(width, height, 180, color1, color2, color3, ...)
gradientContent := strings.Builder{}
for y := range height {
	for x := range width {
		index := y*width + x
		gradientContent.WriteString(
			lipgloss.NewStyle().
				Background(gradient[index]).
				Render(" "),
		)
	}
	if y < height-1 { // End of row.
		gradientContent.WriteString("\n")
	}
}
```

Note that if any of the provided colors are completely transparent, we will
assume that the alpha value was lost in conversion from RGB -> RGBA, and we
will set the alpha to opaque, as it's not possible to blend something completely
transparent.

#### func [Color](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L68) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Color "Go to Color")

```
func Color(s string) color.Color
```

Color specifies a color by hex or ANSI256 value. For example:

```
ansiColor := lipgloss.Color("1") // The same as lipgloss.Red
ansi256Color := lipgloss.Color("21")
hexColor := lipgloss.Color("#0000ff")
```

#### func [Complementary](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L308) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Complementary "Go to Complementary")

```
func Complementary(c color.Color) color.Color
```

Complementary returns the complementary color (180° away on color wheel) of
the given color. This is useful for creating a contrasting color.

#### func [Darken](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L328) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Darken "Go to Darken")

```
func Darken(c color.Color, percent float64) color.Color
```

Darken takes a color and makes it darker by a specific percentage (0-1, clamped).

#### func [EnableLegacyWindowsANSI](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/ansi_unix.go\#L8) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#EnableLegacyWindowsANSI "Go to EnableLegacyWindowsANSI")

```
func EnableLegacyWindowsANSI(*os.File)
```

EnableLegacyWindowsANSI is only needed on Windows.

#### func [Fprint](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L67) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Fprint "Go to Fprint")

```
func Fprint(w io.Writer, v ...any) (int, error)
```

Fprint pritnts to the given writer, automatically downsampling colors when
necessary.

Example:

```
str := NewStyle().
    Foreground(lipgloss.Color("#6a00ff")).
    Render("guzzle")

Fprint(os.Stderr, "I %s horchata pretty much all the time.\n", str)
```

#### func [Fprintf](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L95) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Fprintf "Go to Fprintf")

```
func Fprintf(w io.Writer, format string, v ...any) (int, error)
```

Fprintf prints text to a writer, against the given format, automatically
downsampling colors when necessary.

Example:

```
str := NewStyle().
    Foreground(lipgloss.Color("#6a00ff")).
    Render("artichokes")

Fprintf(os.Stderr, "I really love %s!\n", food)
```

#### func [Fprintln](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L81) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Fprintln "Go to Fprintln")

```
func Fprintln(w io.Writer, v ...any) (int, error)
```

Fprintln prints to the given writer, automatically downsampling colors when
necessary, and ending with a trailing newline.

Example:

```
str := NewStyle().
    Foreground(lipgloss.Color("#6a00ff")).
    Render("Sandwich time!")

Fprintln(os.Stderr, str)
```

#### func [HasDarkBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/query.go\#L86) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#HasDarkBackground "Go to HasDarkBackground")

```
func HasDarkBackground(in term.File, out term.File) bool
```

HasDarkBackground detects whether the terminal has a light or dark
background.

Typically, you'll want to query against stdin and either stdout or stderr
depending on what you're writing to.

```
hasDarkBG := HasDarkBackground(os.Stdin, os.Stdout)
lightDark := LightDark(hasDarkBG)
myHotColor := lightDark("#ff0000", "#0000ff")
```

This is intended for use in standalone Lip Gloss only. In Bubble Tea, listen
for tea.BackgroundColorMsg in your Update function.

```
case tea.BackgroundColorMsg:
    hasDarkBackground = msg.IsDark()
```

By default, this function will return true if it encounters an error.

#### func [Height](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/size.go\#L29) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Height "Go to Height")

```
func Height(str string) int
```

Height returns height of a string in cells. This is done simply by
counting \\n characters. If your output has \\r\\n, that sequence will be
replaced with a \\n in [Style.Render](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Render).

#### func [JoinHorizontal](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/join.go\#L28) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#JoinHorizontal "Go to JoinHorizontal")

```
func JoinHorizontal(pos Position, strs ...string) string
```

JoinHorizontal is a utility function for horizontally joining two
potentially multi-lined strings along a vertical axis. The first argument is
the position, with 0 being all the way at the top and 1 being all the way
at the bottom.

If you just want to align to the top, center or bottom you may as well just
use the helper constants Top, Center, and Bottom.

Example:

```
blockB := "...\n...\n..."
blockA := "...\n...\n...\n...\n..."

// Join 20% from the top
str := lipgloss.JoinHorizontal(0.2, blockA, blockB)

// Join on the top edge
str := lipgloss.JoinHorizontal(lipgloss.Top, blockA, blockB)
```

#### func [JoinVertical](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/join.go\#L116) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#JoinVertical "Go to JoinVertical")

```
func JoinVertical(pos Position, strs ...string) string
```

JoinVertical is a utility function for vertically joining two potentially
multi-lined strings along a horizontal axis. The first argument is the
position, with 0 being all the way to the left and 1 being all the way to
the right.

If you just want to align to the left, right or center you may as well just
use the helper constants Left, Center, and Right.

Example:

```
blockB := "...\n...\n..."
blockA := "...\n...\n...\n...\n..."

// Join 20% from the top
str := lipgloss.JoinVertical(0.2, blockA, blockB)

// Join on the right edge
str := lipgloss.JoinVertical(lipgloss.Right, blockA, blockB)
```

#### func [Lighten](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L345) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Lighten "Go to Lighten")

```
func Lighten(c color.Color, percent float64) color.Color
```

Lighten makes a color lighter by a specific percentage (0-1, clamped).

#### func [Place](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/position.go\#L36) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Place "Go to Place")

```
func Place(width, height int, hPos, vPos Position, str string, opts ...WhitespaceOption) string
```

Place places a string or text block vertically in an unstyled box of a given
width or height.

#### func [PlaceHorizontal](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/position.go\#L43) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#PlaceHorizontal "Go to PlaceHorizontal")

```
func PlaceHorizontal(width int, pos Position, str string, opts ...WhitespaceOption) string
```

PlaceHorizontal places a string or text block horizontally in an unstyled
block of a given width. If the given width is shorter than the max width of
the string (measured by its longest line) this will be a noöp.

#### func [PlaceVertical](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/position.go\#L90) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#PlaceVertical "Go to PlaceVertical")

```
func PlaceVertical(height int, pos Position, str string, opts ...WhitespaceOption) string
```

PlaceVertical places a string or text block vertically in an unstyled block
of a given height. If the given height is shorter than the height of the
string (measured by its newlines) then this will be a noöp.

#### func [Print](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L53) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Print "Go to Print")

```
func Print(v ...any) (int, error)
```

Print to stdout, automatically downsampling colors when necessary.

Example:

```
str := NewStyle().
    Foreground(lipgloss.Color("#6a00ff")).
    Render("Who wants marmalade?\n")

Print(str)
```

#### func [Printf](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L40) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Printf "Go to Printf")

```
func Printf(format string, v ...any) (int, error)
```

Printf prints formatted text to stdout, automatically downsampling colors
when necessary.

Example:

```
str := NewStyle().
  Foreground(lipgloss.Color("#6a00ff")).
  Render("knuckle")

Printf("Time for a %s sandwich!\n", str)
```

#### func [Println](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L26) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Println "Go to Println")

```
func Println(v ...any) (int, error)
```

Println to stdout, automatically downsampling colors when necessary, ending
with a trailing newline.

Example:

```
str := NewStyle().
    Foreground(lipgloss.Color("#6a00ff")).
    Render("breakfast")

Println("Time for a", str, "sandwich!")
```

#### func [Size](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/size.go\#L36) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Size "Go to Size")

```
func Size(str string) (width, height int)
```

Size returns the width and height of the string in cells. ANSI sequences are
ignored and characters wider than one cell (such as Chinese characters and
emojis) are appropriately measured.

#### func [Sprint](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L110) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Sprint "Go to Sprint")

```
func Sprint(v ...any) string
```

Sprint returns a string for stdout, automatically downsampling colors when
necessary.

Example:

```
str := NewStyle().
	Faint(true).
    Foreground(lipgloss.Color("#6a00ff")).
    Render("I love to eat")

str = Sprint(str)
```

#### func [Sprintf](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L152) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Sprintf "Go to Sprintf")

```
func Sprintf(format string, v ...any) string
```

Sprintf returns a formatted string for stdout, automatically downsampling
colors when necessary.

Example:

```
str := NewStyle().
	Bold(true).
	Foreground(lipgloss.Color("#fccaee")).
	Render("Cantaloupe")

str = Sprintf("I really love %s!", str)
```

#### func [Sprintln](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go\#L131) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Sprintln "Go to Sprintln")

```
func Sprintln(v ...any) string
```

Sprintln returns a string for stdout, automatically downsampling colors when
necessary, and ending with a trailing newline.

Example:

```
str := NewStyle().
	Bold(true).
	Foreground(lipgloss.Color("#6a00ff")).
	Render("Yummy!")

str = Sprintln(str)
```

#### func [StyleRanges](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/ranges.go\#L11) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#StyleRanges "Go to StyleRanges")

```
func StyleRanges(s string, ranges ...Range) string
```

StyleRanges applying styling to ranges in a string. Existing styles will be
taken into account. Ranges should not overlap.

#### func [StyleRunes](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/runes.go\#L10) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#StyleRunes "Go to StyleRunes")

```
func StyleRunes(str string, indices []int, matched, unmatched Style) string
```

StyleRunes apply a given style to runes at the given indices in the string.
Note that you must provide styling options for both matched and unmatched
runes. Indices out of bounds will be ignored.

#### func [Width](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/size.go\#L15) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Width "Go to Width")

```
func Width(str string) (width int)
```

Width returns the cell width of characters in the string. ANSI sequences are
ignored and characters wider than one cell (such as Chinese characters and
emojis) are appropriately measured.

You should use this instead of len(string) or len(\[\]rune(string) as neither
will give you accurate results.

#### func [Wrap](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L12) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Wrap "Go to Wrap")

```
func Wrap(s string, width int, breakpoints string) string
```

Wrap wraps the given string to the given width, preserving ANSI styles and links.

### Types [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#pkg-types "Go to Types")

#### type [ANSIColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L161) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#ANSIColor "Go to ANSIColor")

```
type ANSIColor = ansi.IndexedColor
```

ANSIColor is a color specified by an ANSI256 color value.

Example usage:

```
colorA := lipgloss.ANSIColor(8)
colorB := lipgloss.ANSIColor(134)
```

#### type [Border](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L16) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Border "Go to Border")

```
type Border struct {
	Top          string
	Bottom       string
	Left         string
	Right        string
	TopLeft      string
	TopRight     string
	BottomLeft   string
	BottomRight  string
	MiddleLeft   string
	MiddleRight  string
	Middle       string
	MiddleTop    string
	MiddleBottom string
}
```

Border contains a series of values which comprise the various parts of a
border.

#### func [ASCIIBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L277) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#ASCIIBorder "Go to ASCIIBorder")

```
func ASCIIBorder() Border
```

ASCIIBorder returns a table border with ASCII characters.

#### func [BlockBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L233) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#BlockBorder "Go to BlockBorder")

```
func BlockBorder() Border
```

BlockBorder returns a border that takes the whole block.

#### func [DoubleBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L254) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#DoubleBorder "Go to DoubleBorder")

```
func DoubleBorder() Border
```

DoubleBorder returns a border comprised of two thin strokes.

#### func [HiddenBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L262) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#HiddenBorder "Go to HiddenBorder")

```
func HiddenBorder() Border
```

HiddenBorder returns a border that renders as a series of single-cell
spaces. It's useful for cases when you want to remove a standard border but
maintain layout positioning. This said, you can still apply a background
color to a hidden border.

#### func [InnerHalfBlockBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L243) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#InnerHalfBlockBorder "Go to InnerHalfBlockBorder")

```
func InnerHalfBlockBorder() Border
```

InnerHalfBlockBorder returns a half-block border that sits inside the frame.

#### func [MarkdownBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L272) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#MarkdownBorder "Go to MarkdownBorder")

```
func MarkdownBorder() Border
```

MarkdownBorder return a table border in markdown style.

Make sure to disable top and bottom border for the best result. This will
ensure that the output is valid markdown.

```
table.New().Border(lipgloss.MarkdownBorder()).BorderTop(false).BorderBottom(false)
```

#### func [NormalBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L223) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NormalBorder "Go to NormalBorder")

```
func NormalBorder() Border
```

NormalBorder returns a standard-type border with a normal weight and 90
degree corners.

#### func [OuterHalfBlockBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L238) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#OuterHalfBlockBorder "Go to OuterHalfBlockBorder")

```
func OuterHalfBlockBorder() Border
```

OuterHalfBlockBorder returns a half-block border that sits outside the frame.

#### func [RoundedBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L228) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#RoundedBorder "Go to RoundedBorder")

```
func RoundedBorder() Border
```

RoundedBorder returns a border with rounded corners.

#### func [ThickBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L249) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#ThickBorder "Go to ThickBorder")

```
func ThickBorder() Border
```

ThickBorder returns a border that's thicker than the one returned by
NormalBorder.

#### func (Border) [GetBottomSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L49) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Border.GetBottomSize "Go to Border.GetBottomSize")

```
func (b Border) GetBottomSize() int
```

GetBottomSize returns the width of the bottom border. If borders contain
runes of varying widths, the widest rune is returned. If no border exists on
the bottom edge, 0 is returned.

#### func (Border) [GetLeftSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L56) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Border.GetLeftSize "Go to Border.GetLeftSize")

```
func (b Border) GetLeftSize() int
```

GetLeftSize returns the width of the left border. If borders contain runes
of varying widths, the widest rune is returned. If no border exists on the
left edge, 0 is returned.

#### func (Border) [GetRightSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L42) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Border.GetRightSize "Go to Border.GetRightSize")

```
func (b Border) GetRightSize() int
```

GetRightSize returns the width of the right border. If borders contain
runes of varying widths, the widest rune is returned. If no border exists on
the right edge, 0 is returned.

#### func (Border) [GetTopSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go\#L35) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Border.GetTopSize "Go to Border.GetTopSize")

```
func (b Border) GetTopSize() int
```

GetTopSize returns the width of the top border. If borders contain runes of
varying widths, the widest rune is returned. If no border exists on the top
edge, 0 is returned.

#### type [Canvas](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L17) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas "Go to Canvas")

```
type Canvas struct {
	// contains filtered or unexported fields
}
```

Canvas is a cell-buffer that can be used to compose and draw \[uv.Drawable\]s
like \[Layer\]s.

Composed drawables are drawn onto the canvas in the order they were
composed, meaning later drawables will appear "on top" of earlier ones.

A canvas can read, modify, and render its cell contents.

It implements [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen) and [uv.Drawable](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Drawable).

#### func [NewCanvas](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L24) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NewCanvas "Go to NewCanvas")

```
func NewCanvas(width, height int) *Canvas
```

NewCanvas creates a new [Canvas](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas) with the given size.

#### func (\*Canvas) [Bounds](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L42) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Bounds "Go to Canvas.Bounds")

```
func (c *Canvas) Bounds() uv.Rectangle
```

Bounds implements [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen).

#### func (\*Canvas) [CellAt](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L57) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.CellAt "Go to Canvas.CellAt")

```
func (c *Canvas) CellAt(x int, y int) *uv.Cell
```

CellAt implements [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen).

#### func (\*Canvas) [Clear](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L37) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Clear "Go to Canvas.Clear")

```
func (c *Canvas) Clear()
```

Clear clears the canvas.

#### func (\*Canvas) [Compose](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L72) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Compose "Go to Canvas.Compose")

```
func (c *Canvas) Compose(drawer uv.Drawable) *Canvas
```

Compose composes a [Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer) or any [uv.Drawable](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Drawable) onto the [Canvas](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas).

#### func (\*Canvas) [Draw](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L81) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Draw "Go to Canvas.Draw")

```
func (c *Canvas) Draw(scr uv.Screen, area uv.Rectangle)
```

Draw draws the [Canvas](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Canvas) onto the given [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen) within the specified
area.

It implements [uv.Drawable](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Drawable).

#### func (\*Canvas) [Height](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L52) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Height "Go to Canvas.Height")

```
func (c *Canvas) Height() int
```

Height returns the height of the canvas.

#### func (\*Canvas) [Render](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L87) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Render "Go to Canvas.Render")

```
func (c *Canvas) Render() string
```

Render renders the canvas into a styled string. Any trailing spaces are
trimmed.

#### func (\*Canvas) [Resize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L32) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Resize "Go to Canvas.Resize")

```
func (c *Canvas) Resize(width, height int)
```

Resize resizes the canvas to the given width and height.

#### func (\*Canvas) [SetCell](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L62) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.SetCell "Go to Canvas.SetCell")

```
func (c *Canvas) SetCell(x int, y int, cell *uv.Cell)
```

SetCell implements [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen).

#### func (\*Canvas) [Width](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L47) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.Width "Go to Canvas.Width")

```
func (c *Canvas) Width() int
```

Width returns the width of the canvas.

#### func (\*Canvas) [WidthMethod](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go\#L67) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Canvas.WidthMethod "Go to Canvas.WidthMethod")

```
func (c *Canvas) WidthMethod() uv.WidthMethod
```

WidthMethod implements [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen).

#### type [CompleteFunc](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L250) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#CompleteFunc "Go to CompleteFunc")

```
type CompleteFunc func(ansi, ansi256, truecolor color.Color) color.Color
```

CompleteFunc is a function that returns the appropriate color based on the
given color profile.

Example usage:

```
p := colorprofile.Detect(os.Stderr, os.Environ())
complete := lipgloss.Complete(p)
color := complete(
	lipgloss.Color(1), // ANSI
	lipgloss.Color(124), // ANSI256
	lipgloss.Color("#ff34ac"), // TrueColor
)
fmt.Println("Ooh, pretty color: ", color)
```

For more info see [Complete](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Complete).

#### func [Complete](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L265) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Complete "Go to Complete")

```
func Complete(p colorprofile.Profile) CompleteFunc
```

Complete returns a function that will return the appropriate color based on
the given color profile.

Example usage:

```
p := colorprofile.Detect(os.Stderr, os.Environ())
complete := lipgloss.Complete(p)
color := complete(
    lipgloss.Color(1), // ANSI
    lipgloss.Color(124), // ANSI256
    lipgloss.Color("#ff34ac"), // TrueColor
)
fmt.Println("Ooh, pretty color: ", color)
```

#### type [Compositor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L188) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor "Go to Compositor")

```
type Compositor struct {
	// contains filtered or unexported fields
}
```

Compositor manages the composition of layers. It flattens a layer hierarchy
once and provides efficient drawing and hit testing operations. All computation
related to layers happens in the Compositor.

#### func [NewCompositor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L206) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NewCompositor "Go to NewCompositor")

```
func NewCompositor(layers ...*Layer) *Compositor
```

NewCompositor creates a new Compositor with an internal root layer. Optional
layers can be provided which will be added as children of the root. The layer
hierarchy is flattened and sorted by z-index for efficient rendering and hit testing.

#### func (\*Compositor) [AddLayers](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L218) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.AddLayers "Go to Compositor.AddLayers")

```
func (c *Compositor) AddLayers(layers ...*Layer) *Compositor
```

AddLayers adds layers to the compositor's root and refreshes the internal state.

#### func (\*Compositor) [Bounds](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L273) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.Bounds "Go to Compositor.Bounds")

```
func (c *Compositor) Bounds() image.Rectangle
```

Bounds returns the overall bounds of all layers in the compositor.

#### func (\*Compositor) [Draw](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L278) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.Draw "Go to Compositor.Draw")

```
func (c *Compositor) Draw(scr uv.Screen, area image.Rectangle)
```

Draw draws all layers onto the given [uv.Screen](https://pkg.go.dev/github.com/charmbracelet/ultraviolet#Screen) in z-index order.

#### func (\*Compositor) [GetLayer](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L307) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.GetLayer "Go to Compositor.GetLayer")

```
func (c *Compositor) GetLayer(id string) *Layer
```

GetLayer returns a layer by its ID, or nil if not found.
Layers with empty IDs are not indexed and cannot be retrieved.

#### func (\*Compositor) [Hit](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L289) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.Hit "Go to Compositor.Hit")

```
func (c *Compositor) Hit(x, y int) LayerHit
```

Hit performs a hit test at the given (x, y) coordinates. If a layer is hit,
it returns the ID of the top-most layer at that point. Layers with empty IDs
are ignored. If no layer is hit, it returns an empty [LayerHit](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LayerHit).

#### func (\*Compositor) [Refresh](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L316) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.Refresh "Go to Compositor.Refresh")

```
func (c *Compositor) Refresh()
```

Refresh re-flattens the layer hierarchy. Call this after modifying the layer
tree structure or positions to update the compositor's internal state.

#### func (\*Compositor) [Render](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L323) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Compositor.Render "Go to Compositor.Render")

```
func (c *Compositor) Render() string
```

Render renders the compositor into a styled string. This is a helper
function that creates a temporary canvas, draws the compositor onto it, and
returns the resulting string.

#### type [Layer](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L13) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer "Go to Layer")

```
type Layer struct {
	// contains filtered or unexported fields
}
```

Layer represents a visual layer with content and positioning. It's a pure
data structure that defines the layer hierarchy without any computation.

#### func [NewLayer](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L22) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NewLayer "Go to NewLayer")

```
func NewLayer(content string, layers ...*Layer) *Layer
```

NewLayer creates a new [Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer) with the given content and optional child layers.

#### func (\*Layer) [AddLayers](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L90) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.AddLayers "Go to Layer.AddLayers")

```
func (l *Layer) AddLayers(layers ...*Layer) *Layer
```

AddLayers adds child layers to the Layer.

#### func (\*Layer) [Draw](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L153) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.Draw "Go to Layer.Draw")

```
func (l *Layer) Draw(scr uv.Screen, area uv.Rectangle)
```

Draw draws the content of the layer on the screen at the specified area.

#### func (\*Layer) [GetContent](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L31) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.GetContent "Go to Layer.GetContent")

```
func (l *Layer) GetContent() string
```

GetContent returns the content of the Layer.

#### func (\*Layer) [GetID](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L46) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.GetID "Go to Layer.GetID")

```
func (l *Layer) GetID() string
```

GetID returns the ID of the Layer.

#### func (\*Layer) [GetLayer](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L105) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.GetLayer "Go to Layer.GetLayer")

```
func (l *Layer) GetLayer(id string) *Layer
```

GetLayer returns a descendant layer by its ID, or nil if not found.
Layers with empty IDs are skipped.

#### func (\*Layer) [GetX](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L75) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.GetX "Go to Layer.GetX")

```
func (l *Layer) GetX() int
```

GetX returns the x-coordinate of the Layer relative to its parent.

#### func (\*Layer) [GetY](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L80) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.GetY "Go to Layer.GetY")

```
func (l *Layer) GetY() int
```

GetY returns the y-coordinate of the Layer relative to its parent.

#### func (\*Layer) [GetZ](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L85) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.GetZ "Go to Layer.GetZ")

```
func (l *Layer) GetZ() int
```

GetZ returns the z-index of the Layer relative to its parent.

#### func (\*Layer) [Height](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L41) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.Height "Go to Layer.Height")

```
func (l *Layer) Height() int
```

Height returns the height of the Layer.

#### func (\*Layer) [ID](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L51) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.ID "Go to Layer.ID")

```
func (l *Layer) ID(id string) *Layer
```

ID sets the ID of the Layer.

#### func (\*Layer) [MaxZ](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L121) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.MaxZ "Go to Layer.MaxZ")

```
func (l *Layer) MaxZ() int
```

MaxZ returns the maximum z-index among this layer and all its descendants.

#### func (\*Layer) [Width](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L36) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.Width "Go to Layer.Width")

```
func (l *Layer) Width() int
```

Width returns the width of the Layer.

#### func (\*Layer) [X](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L57) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.X "Go to Layer.X")

```
func (l *Layer) X(x int) *Layer
```

X sets the x-coordinate of the Layer relative to its parent.

#### func (\*Layer) [Y](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L63) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.Y "Go to Layer.Y")

```
func (l *Layer) Y(y int) *Layer
```

Y sets the y-coordinate of the Layer relative to its parent.

#### func (\*Layer) [Z](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L69) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Layer.Z "Go to Layer.Z")

```
func (l *Layer) Z(z int) *Layer
```

Z sets the z-index of the Layer relative to its parent.

#### type [LayerHit](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L159) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LayerHit "Go to LayerHit")

```
type LayerHit struct {
	// contains filtered or unexported fields
}
```

LayerHit represents the result of a hit test on a [Layer](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Layer).

#### func (LayerHit) [Bounds](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L181) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LayerHit.Bounds "Go to LayerHit.Bounds")

```
func (lh LayerHit) Bounds() image.Rectangle
```

Bounds returns the bounds of the LayerHit.

#### func (LayerHit) [Empty](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L166) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LayerHit.Empty "Go to LayerHit.Empty")

```
func (lh LayerHit) Empty() bool
```

Empty returns true if the LayerHit represents no hit.

#### func (LayerHit) [ID](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L171) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LayerHit.ID "Go to LayerHit.ID")

```
func (lh LayerHit) ID() string
```

ID returns the ID of the hit Layer.

#### func (LayerHit) [Layer](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go\#L176) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LayerHit.Layer "Go to LayerHit.Layer")

```
func (lh LayerHit) Layer() *Layer
```

Layer returns the layer that was hit.

#### type [LightDarkFunc](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L174) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LightDarkFunc "Go to LightDarkFunc")

```
type LightDarkFunc func(light, dark color.Color) color.Color
```

LightDarkFunc is a function that returns a color based on whether the
terminal has a light or dark background. You can create one of these with
[LightDark](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LightDark).

Example:

```
lightDark := lipgloss.LightDark(hasDarkBackground)
red, blue := lipgloss.Color("#ff0000"), lipgloss.Color("#0000ff")
myHotColor := lightDark(red, blue)
```

For more info see [LightDark](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#LightDark).

#### func [LightDark](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L205) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#LightDark "Go to LightDark")

```
func LightDark(isDark bool) LightDarkFunc
```

LightDark is a simple helper type that can be used to choose the appropriate
color based on whether the terminal has a light or dark background.

```
lightDark := lipgloss.LightDark(hasDarkBackground)
red, blue := lipgloss.Color("#ff0000"), lipgloss.Color("#0000ff")
myHotColor := lightDark(red, blue)
```

In practice, there are slightly different workflows between Bubble Tea and
Lip Gloss standalone.

In Bubble Tea, listen for tea.BackgroundColorMsg, which automatically
flows through Update on start. This message will be received whenever the
background color changes:

```
case tea.BackgroundColorMsg:
    m.hasDarkBackground = msg.IsDark()
```

Later, when you're rendering use:

```
lightDark := lipgloss.LightDark(m.hasDarkBackground)
red, blue := lipgloss.Color("#ff0000"), lipgloss.Color("#0000ff")
myHotColor := lightDark(red, blue)
```

In standalone Lip Gloss, the workflow is simpler:

```
hasDarkBG := lipgloss.HasDarkBackground(os.Stdin, os.Stdout)
lightDark := lipgloss.LightDark(hasDarkBG)
red, blue := lipgloss.Color("#ff0000"), lipgloss.Color("#0000ff")
myHotColor := lightDark(red, blue)
```

#### type [NoColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L52) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NoColor "Go to NoColor")

```
type NoColor struct{}
```

NoColor is used to specify the absence of color styling. When this is active
foreground colors will be rendered with the terminal's default text color,
and background colors will not be drawn at all.

Example usage:

```
var style = someStyle.Background(lipgloss.NoColor{})
```

#### func (NoColor) [RGBA](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L59) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NoColor.RGBA "Go to NoColor.RGBA")

```
func (n NoColor) RGBA() (r, g, b, a uint32)
```

RGBA returns the RGBA value of this color. Because we have to return
something, despite this color being the absence of color, we're returning
black with 100% opacity.

Red: 0x0, Green: 0x0, Blue: 0x0, Alpha: 0xFFFF.

#### type [Position](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/position.go\#L19) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Position "Go to Position")

```
type Position float64
```

Position represents a position along a horizontal or vertical axis. It's in
situations where an axis is involved, like alignment, joining, placement and
so on.

A value of 0 represents the start (the left or top) and 1 represents the end
(the right or bottom). 0.5 represents the center.

There are constants Top, Bottom, Center, Left and Right in this package that
can be used to aid readability.

```
const (
	Top    Position = 0.0
	Bottom Position = 1.0
	Center Position = 0.5
	Left   Position = 0.0
	Right  Position = 1.0
)
```

Position aliases.

#### type [RGBColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L138) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#RGBColor "Go to RGBColor")

```
type RGBColor struct {
	R uint8
	G uint8
	B uint8
}
```

RGBColor is a color specified by red, green, and blue values.

#### func (RGBColor) [RGBA](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go\#L146) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#RGBColor.RGBA "Go to RGBColor.RGBA")

```
func (c RGBColor) RGBA() (r, g, b, a uint32)
```

RGBA returns the RGBA value of this color. This satisfies the Go Color
interface.

#### type [Range](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/ranges.go\#L45) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Range "Go to Range")

```
type Range struct {
	Start, End int
	Style      Style
}
```

Range is a range of text and associated styling to be used with
[StyleRanges](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#StyleRanges).

#### func [NewRange](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/ranges.go\#L39) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NewRange "Go to NewRange")

```
func NewRange(start, end int, style Style) Range
```

NewRange returns a range and style that can be used with [StyleRanges](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#StyleRanges).

#### type [Style](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L142) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style "Go to Style")

```
type Style struct {
	// contains filtered or unexported fields
}
```

Style contains a set of rules that comprise a style as a whole.

#### func [NewStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L137) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NewStyle "Go to NewStyle")

```
func NewStyle() Style
```

NewStyle returns a new, empty Style. While it's syntactic sugar for the
[Style](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style){} primitive, it's recommended to use this function for creating styles
in case the underlying implementation changes.

#### func (Style) [Align](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L305) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Align "Go to Style.Align")

```
func (s Style) Align(p ...Position) Style
```

Align is a shorthand method for setting horizontal and vertical alignment.

With one argument, the position value is applied to the horizontal alignment.

With two arguments, the value is applied to the horizontal and vertical
alignments, in that order.

#### func (Style) [AlignHorizontal](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L316) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.AlignHorizontal "Go to Style.AlignHorizontal")

```
func (s Style) AlignHorizontal(p Position) Style
```

AlignHorizontal sets a horizontal text alignment rule.

#### func (Style) [AlignVertical](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L322) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.AlignVertical "Go to Style.AlignVertical")

```
func (s Style) AlignVertical(p Position) Style
```

AlignVertical sets a vertical text alignment rule.

#### func (Style) [Background](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L278) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Background "Go to Style.Background")

```
func (s Style) Background(c color.Color) Style
```

Background sets a background color.

#### func (Style) [Blink](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L254) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Blink "Go to Style.Blink")

```
func (s Style) Blink(v bool) Style
```

Blink sets a rule for blinking foreground text.

#### func (Style) [Bold](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L195) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Bold "Go to Style.Bold")

```
func (s Style) Bold(v bool) Style
```

Bold sets a bold formatting rule.

#### func (Style) [Border](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L490) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Border "Go to Style.Border")

```
func (s Style) Border(b Border, sides ...bool) Style
```

Border is shorthand for setting the border style and which sides should
have a border at once. The variadic argument sides works as follows:

With one value, the value is applied to all sides.

With two values, the values are applied to the vertical and horizontal
sides, in that order.

With three values, the values are applied to the top side, the horizontal
sides, and the bottom side, in that order.

With four values, the values are applied clockwise starting from the top
side, followed by the right side, then the bottom, and finally the left.

With more than four arguments the border will be applied to all sides.

Examples:

```
// Applies borders to the top and bottom only
lipgloss.NewStyle().Border(lipgloss.NormalBorder(), true, false)

// Applies rounded borders to the right and bottom only
lipgloss.NewStyle().Border(lipgloss.RoundedBorder(), false, true, true, false)
```

#### func (Style) [BorderBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L675) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderBackground "Go to Style.BorderBackground")

```
func (s Style) BorderBackground(c ...color.Color) Style
```

BorderBackground is a shorthand function for setting all of the
background colors of the borders at once. The arguments work as follows:

With one argument, the argument is applied to all sides.

With two arguments, the arguments are applied to the vertical and horizontal
sides, in that order.

With three arguments, the arguments are applied to the top side, the
horizontal sides, and the bottom side, in that order.

With four arguments, the arguments are applied clockwise starting from the
top side, followed by the right side, then the bottom, and finally the left.

With more than four arguments nothing will be set.

#### func (Style) [BorderBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L541) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderBottom "Go to Style.BorderBottom")

```
func (s Style) BorderBottom(v bool) Style
```

BorderBottom determines whether or not to draw a bottom border.

#### func (Style) [BorderBottomBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L707) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderBottomBackground "Go to Style.BorderBottomBackground")

```
func (s Style) BorderBottomBackground(c color.Color) Style
```

BorderBottomBackground sets the background color of the bottom of the
border.

#### func (Style) [BorderBottomForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L600) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderBottomForeground "Go to Style.BorderBottomForeground")

```
func (s Style) BorderBottomForeground(c color.Color) Style
```

BorderBottomForeground sets the foreground color for the bottom of the
border.

#### func (Style) [BorderForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L567) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderForeground "Go to Style.BorderForeground")

```
func (s Style) BorderForeground(c ...color.Color) Style
```

BorderForeground is a shorthand function for setting all of the
foreground colors of the borders at once. The arguments work as follows:

With one argument, the argument is applied to all sides.

With two arguments, the arguments are applied to the vertical and horizontal
sides, in that order.

With three arguments, the arguments are applied to the top side, the
horizontal sides, and the bottom side, in that order.

With four arguments, the arguments are applied clockwise starting from the
top side, followed by the right side, then the bottom, and finally the left.

With more than four arguments nothing will be set.

#### func (Style) [BorderForegroundBlend](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L628) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderForegroundBlend "Go to Style.BorderForegroundBlend")

```
func (s Style) BorderForegroundBlend(c ...color.Color) Style
```

BorderForegroundBlend sets the foreground colors for the border blend. At least
2 colors are required to use blending, otherwise this will no-op with 0 colors,
and pass to BorderForeground with 1 color. This will override all other border
foreground colors when used.

When providing colors, in most cases (e.g. when all border sides are enabled),
you will want to provide a wrapping-set of colors, so the start and end color
are either the same, or very similar. For example:

```
lipgloss.NewStyle().BorderForegroundBlend(
	lipgloss.Color("#00FA68"),
	lipgloss.Color("#9900FF"),
	lipgloss.Color("#ED5353"),
	lipgloss.Color("#9900FF"),
	lipgloss.Color("#00FA68"),
)
```

#### func (Style) [BorderForegroundBlendOffset](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L655) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderForegroundBlendOffset "Go to Style.BorderForegroundBlendOffset")

```
func (s Style) BorderForegroundBlendOffset(v int) Style
```

BorderForegroundBlendOffset sets the border blend offset cells, starting from
the top left corner. Value can be positive or negative, and does not need to
equal the dimensions of the border region. Direction (when positive) is as
follows ("o" is starting point):

```
  o -------->
  ┌──────────┐
^ │          │ |
| │          │ |
| │          │ |
| │          │ v
  └──────────┘
   <---------
```

#### func (Style) [BorderLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L547) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderLeft "Go to Style.BorderLeft")

```
func (s Style) BorderLeft(v bool) Style
```

BorderLeft determines whether or not to draw a left border.

#### func (Style) [BorderLeftBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L714) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderLeftBackground "Go to Style.BorderLeftBackground")

```
func (s Style) BorderLeftBackground(c color.Color) Style
```

BorderLeftBackground set the background color of the left side of the
border.

#### func (Style) [BorderLeftForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L607) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderLeftForeground "Go to Style.BorderLeftForeground")

```
func (s Style) BorderLeftForeground(c color.Color) Style
```

BorderLeftForeground sets the foreground color for the left side of the
border.

#### func (Style) [BorderRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L535) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderRight "Go to Style.BorderRight")

```
func (s Style) BorderRight(v bool) Style
```

BorderRight determines whether or not to draw a right border.

#### func (Style) [BorderRightBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L700) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderRightBackground "Go to Style.BorderRightBackground")

```
func (s Style) BorderRightBackground(c color.Color) Style
```

BorderRightBackground sets the background color of right side the border.

#### func (Style) [BorderRightForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L593) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderRightForeground "Go to Style.BorderRightForeground")

```
func (s Style) BorderRightForeground(c color.Color) Style
```

BorderRightForeground sets the foreground color for the right side of the
border.

#### func (Style) [BorderStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L523) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderStyle "Go to Style.BorderStyle")

```
func (s Style) BorderStyle(b Border) Style
```

BorderStyle defines the Border on a style. A Border contains a series of
definitions for the sides and corners of a border.

Note that if border visibility has not been set for any sides when setting
the border style, the border will be enabled for all sides during rendering.

You can define border characters as you'd like, though several default
styles are included: NormalBorder(), RoundedBorder(), BlockBorder(),
OuterHalfBlockBorder(), InnerHalfBlockBorder(), ThickBorder(),
and DoubleBorder().

Example:

```
lipgloss.NewStyle().BorderStyle(lipgloss.ThickBorder())
```

#### func (Style) [BorderTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L529) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderTop "Go to Style.BorderTop")

```
func (s Style) BorderTop(v bool) Style
```

BorderTop determines whether or not to draw a top border.

#### func (Style) [BorderTopBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L694) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderTopBackground "Go to Style.BorderTopBackground")

```
func (s Style) BorderTopBackground(c color.Color) Style
```

BorderTopBackground sets the background color of the top of the border.

#### func (Style) [BorderTopForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L586) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.BorderTopForeground "Go to Style.BorderTopForeground")

```
func (s Style) BorderTopForeground(c color.Color) Style
```

BorderTopForeground set the foreground color for the top of the border.

#### func (Style) [ColorWhitespace](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L396) deprecated

```
func (s Style) ColorWhitespace(v bool) Style
```

ColorWhitespace determines whether or not the background color should be
applied to the padding. This is true by default as it's more than likely the
desired and expected behavior, but it can be disabled for certain graphic
effects.

Deprecated: Just use margins and padding.

#### func (Style) [Copy](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L229) deprecated

```
func (s Style) Copy() Style
```

Copy returns a copy of this style, including any underlying string values.

Deprecated: to copy just use assignment (i.e. a := b). All methods also
return a new style.

#### func (Style) [Faint](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L260) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Faint "Go to Style.Faint")

```
func (s Style) Faint(v bool) Style
```

Faint sets a rule for rendering the foreground color in a dimmer shade.

#### func (Style) [Foreground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L272) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Foreground "Go to Style.Foreground")

```
func (s Style) Foreground(c color.Color) Style
```

Foreground sets a foreground color.

```
// Sets the foreground to blue
s := lipgloss.NewStyle().Foreground(lipgloss.Color("#0000ff"))

// Removes the foreground color
s.Foreground(lipgloss.NoColor)
```

#### func (Style) [GetAlign](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L89) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetAlign "Go to Style.GetAlign")

```
func (s Style) GetAlign() Position
```

GetAlign returns the style's implicit horizontal alignment setting.
If no alignment is set Position.Left is returned.

#### func (Style) [GetAlignHorizontal](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L99) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetAlignHorizontal "Go to Style.GetAlignHorizontal")

```
func (s Style) GetAlignHorizontal() Position
```

GetAlignHorizontal returns the style's implicit horizontal alignment setting.
If no alignment is set Position.Left is returned.

#### func (Style) [GetAlignVertical](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L109) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetAlignVertical "Go to Style.GetAlignVertical")

```
func (s Style) GetAlignVertical() Position
```

GetAlignVertical returns the style's implicit vertical alignment setting.
If no alignment is set Position.Top is returned.

#### func (Style) [GetBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L71) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBackground "Go to Style.GetBackground")

```
func (s Style) GetBackground() color.Color
```

GetBackground returns the style's background color. If no value is set
NoColor{} is returned.

#### func (Style) [GetBlink](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L53) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBlink "Go to Style.GetBlink")

```
func (s Style) GetBlink() bool
```

GetBlink returns the style's blink value. If no value is set false is
returned.

#### func (Style) [GetBold](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L11) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBold "Go to Style.GetBold")

```
func (s Style) GetBold() bool
```

GetBold returns the style's bold value. If no value is set false is returned.

#### func (Style) [GetBorder](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L237) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorder "Go to Style.GetBorder")

```
func (s Style) GetBorder() (b Border, top, right, bottom, left bool)
```

GetBorder returns the style's border style (type Border) and value for the
top, right, bottom, and left in that order. If no value is set for the
border style, Border{} is returned. For all other unset values false is
returned.

#### func (Style) [GetBorderBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L265) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderBottom "Go to Style.GetBorderBottom")

```
func (s Style) GetBorderBottom() bool
```

GetBorderBottom returns the style's bottom border setting. If no value is
set false is returned.

#### func (Style) [GetBorderBottomBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L325) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderBottomBackground "Go to Style.GetBorderBottomBackground")

```
func (s Style) GetBorderBottomBackground() color.Color
```

GetBorderBottomBackground returns the style's border bottom background
color. If no value is set NoColor{} is returned.

#### func (Style) [GetBorderBottomForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L289) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderBottomForeground "Go to Style.GetBorderBottomForeground")

```
func (s Style) GetBorderBottomForeground() color.Color
```

GetBorderBottomForeground returns the style's border bottom foreground
color. If no value is set NoColor{} is returned.

#### func (Style) [GetBorderBottomSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L373) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderBottomSize "Go to Style.GetBorderBottomSize")

```
func (s Style) GetBorderBottomSize() int
```

GetBorderBottomSize returns the width of the bottom border. If borders
contain runes of varying widths, the widest rune is returned. If no border
exists on the left edge, 0 is returned.

#### func (Style) [GetBorderForegroundBlend](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L301) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderForegroundBlend "Go to Style.GetBorderForegroundBlend")

```
func (s Style) GetBorderForegroundBlend() []color.Color
```

GetBorderForegroundBlend returns the style's border blend foreground
colors. If no value is set, nil is returned.

#### func (Style) [GetBorderForegroundBlendOffset](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L307) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderForegroundBlendOffset "Go to Style.GetBorderForegroundBlendOffset")

```
func (s Style) GetBorderForegroundBlendOffset() int
```

GetBorderForegroundBlendOffset returns the style's border blend offset. If no
value is set, 0 is returned.

#### func (Style) [GetBorderLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L271) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderLeft "Go to Style.GetBorderLeft")

```
func (s Style) GetBorderLeft() bool
```

GetBorderLeft returns the style's left border setting. If no value is
set false is returned.

#### func (Style) [GetBorderLeftBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L331) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderLeftBackground "Go to Style.GetBorderLeftBackground")

```
func (s Style) GetBorderLeftBackground() color.Color
```

GetBorderLeftBackground returns the style's border left background
color. If no value is set NoColor{} is returned.

#### func (Style) [GetBorderLeftForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L295) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderLeftForeground "Go to Style.GetBorderLeftForeground")

```
func (s Style) GetBorderLeftForeground() color.Color
```

GetBorderLeftForeground returns the style's border left foreground
color. If no value is set NoColor{} is returned.

#### func (Style) [GetBorderLeftSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L360) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderLeftSize "Go to Style.GetBorderLeftSize")

```
func (s Style) GetBorderLeftSize() int
```

GetBorderLeftSize returns the width of the left border. If borders contain
runes of varying widths, the widest rune is returned. If no border exists on
the left edge, 0 is returned.

#### func (Style) [GetBorderRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L259) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderRight "Go to Style.GetBorderRight")

```
func (s Style) GetBorderRight() bool
```

GetBorderRight returns the style's right border setting. If no value is set
false is returned.

#### func (Style) [GetBorderRightBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L319) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderRightBackground "Go to Style.GetBorderRightBackground")

```
func (s Style) GetBorderRightBackground() color.Color
```

GetBorderRightBackground returns the style's border right background color.
If no value is set NoColor{} is returned.

#### func (Style) [GetBorderRightForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L283) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderRightForeground "Go to Style.GetBorderRightForeground")

```
func (s Style) GetBorderRightForeground() color.Color
```

GetBorderRightForeground returns the style's border right foreground color.
If no value is set NoColor{} is returned.

#### func (Style) [GetBorderRightSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L386) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderRightSize "Go to Style.GetBorderRightSize")

```
func (s Style) GetBorderRightSize() int
```

GetBorderRightSize returns the width of the right border. If borders
contain runes of varying widths, the widest rune is returned. If no border
exists on the right edge, 0 is returned.

#### func (Style) [GetBorderStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L247) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderStyle "Go to Style.GetBorderStyle")

```
func (s Style) GetBorderStyle() Border
```

GetBorderStyle returns the style's border style (type Border). If no value
is set Border{} is returned.

#### func (Style) [GetBorderTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L253) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderTop "Go to Style.GetBorderTop")

```
func (s Style) GetBorderTop() bool
```

GetBorderTop returns the style's top border setting. If no value is set
false is returned.

#### func (Style) [GetBorderTopBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L313) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderTopBackground "Go to Style.GetBorderTopBackground")

```
func (s Style) GetBorderTopBackground() color.Color
```

GetBorderTopBackground returns the style's border top background color. If
no value is set NoColor{} is returned.

#### func (Style) [GetBorderTopForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L277) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderTopForeground "Go to Style.GetBorderTopForeground")

```
func (s Style) GetBorderTopForeground() color.Color
```

GetBorderTopForeground returns the style's border top foreground color. If
no value is set NoColor{} is returned.

#### func (Style) [GetBorderTopSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L347) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetBorderTopSize "Go to Style.GetBorderTopSize")

```
func (s Style) GetBorderTopSize() int
```

GetBorderTopSize returns the width of the top border. If borders contain
runes of varying widths, the widest rune is returned. If no border exists on
the top edge, 0 is returned.

#### func (Style) [GetBorderTopWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L340) deprecated

```
func (s Style) GetBorderTopWidth() int
```

GetBorderTopWidth returns the width of the top border. If borders contain
runes of varying widths, the widest rune is returned. If no border exists on
the top edge, 0 is returned.

Deprecated: This function simply calls Style.GetBorderTopSize.

#### func (Style) [GetColorWhitespace](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L174) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetColorWhitespace "Go to Style.GetColorWhitespace")

```
func (s Style) GetColorWhitespace() bool
```

GetColorWhitespace returns the style's whitespace coloring setting. If no
value is set false is returned.

#### func (Style) [GetFaint](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L59) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetFaint "Go to Style.GetFaint")

```
func (s Style) GetFaint() bool
```

GetFaint returns the style's faint value. If no value is set false is
returned.

#### func (Style) [GetForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L65) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetForeground "Go to Style.GetForeground")

```
func (s Style) GetForeground() color.Color
```

GetForeground returns the style's foreground color. If no value is set
NoColor{} is returned.

#### func (Style) [GetFrameSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L464) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetFrameSize "Go to Style.GetFrameSize")

```
func (s Style) GetFrameSize() (x, y int)
```

GetFrameSize returns the sum of the margins, padding and border width for
both the horizontal and vertical margins.

#### func (Style) [GetHeight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L83) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetHeight "Go to Style.GetHeight")

```
func (s Style) GetHeight() int
```

GetHeight returns the style's height setting. If no height is set 0 is
returned.

#### func (Style) [GetHorizontalBorderSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L399) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetHorizontalBorderSize "Go to Style.GetHorizontalBorderSize")

```
func (s Style) GetHorizontalBorderSize() int
```

GetHorizontalBorderSize returns the width of the horizontal borders. If
borders contain runes of varying widths, the widest rune is returned. If no
border exists on the horizontal edges, 0 is returned.

#### func (Style) [GetHorizontalFrameSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L450) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetHorizontalFrameSize "Go to Style.GetHorizontalFrameSize")

```
func (s Style) GetHorizontalFrameSize() int
```

GetHorizontalFrameSize returns the sum of the style's horizontal margins, padding
and border widths.

Provisional: this method may be renamed.

#### func (Style) [GetHorizontalMargins](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L223) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetHorizontalMargins "Go to Style.GetHorizontalMargins")

```
func (s Style) GetHorizontalMargins() int
```

GetHorizontalMargins returns the style's left and right margins. Unset
values are measured as 0.

#### func (Style) [GetHorizontalPadding](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L162) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetHorizontalPadding "Go to Style.GetHorizontalPadding")

```
func (s Style) GetHorizontalPadding() int
```

GetHorizontalPadding returns the style's left and right padding. Unset
values are measured as 0.

#### func (Style) [GetHyperlink](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L476) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetHyperlink "Go to Style.GetHyperlink")

```
func (s Style) GetHyperlink() (link, params string)
```

GetHyperlink returns the hyperlink along with its parameters. If no
hyperlink is set, empty strings are returned.

#### func (Style) [GetInline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L412) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetInline "Go to Style.GetInline")

```
func (s Style) GetInline() bool
```

GetInline returns the style's inline setting. If no value is set false is
returned.

#### func (Style) [GetItalic](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L17) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetItalic "Go to Style.GetItalic")

```
func (s Style) GetItalic() bool
```

GetItalic returns the style's italic value. If no value is set false is
returned.

#### func (Style) [GetMargin](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L180) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMargin "Go to Style.GetMargin")

```
func (s Style) GetMargin() (top, right, bottom, left int)
```

GetMargin returns the style's top, right, bottom, and left margins, in that
order. 0 is returned for unset values.

#### func (Style) [GetMarginBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L201) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMarginBottom "Go to Style.GetMarginBottom")

```
func (s Style) GetMarginBottom() int
```

GetMarginBottom returns the style's bottom margin. If no value is set 0 is
returned.

#### func (Style) [GetMarginChar](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L213) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMarginChar "Go to Style.GetMarginChar")

```
func (s Style) GetMarginChar() rune
```

GetMarginChar returns the style's padding character. If no value is set a
space is returned.

#### func (Style) [GetMarginLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L207) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMarginLeft "Go to Style.GetMarginLeft")

```
func (s Style) GetMarginLeft() int
```

GetMarginLeft returns the style's left margin. If no value is set 0 is
returned.

#### func (Style) [GetMarginRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L195) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMarginRight "Go to Style.GetMarginRight")

```
func (s Style) GetMarginRight() int
```

GetMarginRight returns the style's right margin. If no value is set 0 is
returned.

#### func (Style) [GetMarginTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L189) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMarginTop "Go to Style.GetMarginTop")

```
func (s Style) GetMarginTop() int
```

GetMarginTop returns the style's top margin. If no value is set 0 is
returned.

#### func (Style) [GetMaxHeight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L424) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMaxHeight "Go to Style.GetMaxHeight")

```
func (s Style) GetMaxHeight() int
```

GetMaxHeight returns the style's max height setting. If no value is set 0 is
returned.

#### func (Style) [GetMaxWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L418) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetMaxWidth "Go to Style.GetMaxWidth")

```
func (s Style) GetMaxWidth() int
```

GetMaxWidth returns the style's max width setting. If no value is set 0 is
returned.

#### func (Style) [GetPadding](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L119) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetPadding "Go to Style.GetPadding")

```
func (s Style) GetPadding() (top, right, bottom, left int)
```

GetPadding returns the style's top, right, bottom, and left padding values,
in that order. 0 is returned for unset values.

#### func (Style) [GetPaddingBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L140) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetPaddingBottom "Go to Style.GetPaddingBottom")

```
func (s Style) GetPaddingBottom() int
```

GetPaddingBottom returns the style's bottom padding. If no value is set 0 is
returned.

#### func (Style) [GetPaddingChar](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L152) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetPaddingChar "Go to Style.GetPaddingChar")

```
func (s Style) GetPaddingChar() rune
```

GetPaddingChar returns the style's padding character. If no value is set a
space is returned.

#### func (Style) [GetPaddingLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L146) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetPaddingLeft "Go to Style.GetPaddingLeft")

```
func (s Style) GetPaddingLeft() int
```

GetPaddingLeft returns the style's left padding. If no value is set 0 is
returned.

#### func (Style) [GetPaddingRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L134) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetPaddingRight "Go to Style.GetPaddingRight")

```
func (s Style) GetPaddingRight() int
```

GetPaddingRight returns the style's right padding. If no value is set 0 is
returned.

#### func (Style) [GetPaddingTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L128) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetPaddingTop "Go to Style.GetPaddingTop")

```
func (s Style) GetPaddingTop() int
```

GetPaddingTop returns the style's top padding. If no value is set 0 is
returned.

#### func (Style) [GetReverse](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L47) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetReverse "Go to Style.GetReverse")

```
func (s Style) GetReverse() bool
```

GetReverse returns the style's reverse value. If no value is set false is
returned.

#### func (Style) [GetStrikethrough](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L41) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetStrikethrough "Go to Style.GetStrikethrough")

```
func (s Style) GetStrikethrough() bool
```

GetStrikethrough returns the style's strikethrough value. If no value is set false
is returned.

#### func (Style) [GetStrikethroughSpaces](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L442) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetStrikethroughSpaces "Go to Style.GetStrikethroughSpaces")

```
func (s Style) GetStrikethroughSpaces() bool
```

GetStrikethroughSpaces returns whether or not the style is set to strikethrough
spaces. If not value is set false is returned.

#### func (Style) [GetTabWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L430) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetTabWidth "Go to Style.GetTabWidth")

```
func (s Style) GetTabWidth() int
```

GetTabWidth returns the style's tab width setting. If no value is set 4 is
returned which is the implicit default.

#### func (Style) [GetTransform](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L470) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetTransform "Go to Style.GetTransform")

```
func (s Style) GetTransform() func(string) string
```

GetTransform returns the transform set on the style. If no transform is set
nil is returned.

#### func (Style) [GetUnderline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L23) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetUnderline "Go to Style.GetUnderline")

```
func (s Style) GetUnderline() bool
```

GetUnderline returns the style's underline value. If no value is set false is
returned.

#### func (Style) [GetUnderlineColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L35) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetUnderlineColor "Go to Style.GetUnderlineColor")

```
func (s Style) GetUnderlineColor() color.Color
```

GetUnderlineColor returns the style's underline color. If no value is set
NoColor{} is returned.

#### func (Style) [GetUnderlineSpaces](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L436) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetUnderlineSpaces "Go to Style.GetUnderlineSpaces")

```
func (s Style) GetUnderlineSpaces() bool
```

GetUnderlineSpaces returns whether or not the style is set to underline
spaces. If not value is set false is returned.

#### func (Style) [GetUnderlineStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L29) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetUnderlineStyle "Go to Style.GetUnderlineStyle")

```
func (s Style) GetUnderlineStyle() Underline
```

GetUnderlineStyle returns the style's underline style. If no value is set
UnderlineNone is returned.

#### func (Style) [GetVerticalBorderSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L406) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetVerticalBorderSize "Go to Style.GetVerticalBorderSize")

```
func (s Style) GetVerticalBorderSize() int
```

GetVerticalBorderSize returns the width of the vertical borders. If
borders contain runes of varying widths, the widest rune is returned. If no
border exists on the vertical edges, 0 is returned.

#### func (Style) [GetVerticalFrameSize](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L458) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetVerticalFrameSize "Go to Style.GetVerticalFrameSize")

```
func (s Style) GetVerticalFrameSize() int
```

GetVerticalFrameSize returns the sum of the style's vertical margins, padding
and border widths.

Provisional: this method may be renamed.

#### func (Style) [GetVerticalMargins](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L229) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetVerticalMargins "Go to Style.GetVerticalMargins")

```
func (s Style) GetVerticalMargins() int
```

GetVerticalMargins returns the style's top and bottom margins. Unset values
are measured as 0.

#### func (Style) [GetVerticalPadding](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L168) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetVerticalPadding "Go to Style.GetVerticalPadding")

```
func (s Style) GetVerticalPadding() int
```

GetVerticalPadding returns the style's top and bottom padding. Unset values
are measured as 0.

#### func (Style) [GetWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go\#L77) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.GetWidth "Go to Style.GetWidth")

```
func (s Style) GetWidth() int
```

GetWidth returns the style's width setting. If no width is set 0 is
returned.

#### func (Style) [Height](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L294) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Height "Go to Style.Height")

```
func (s Style) Height(i int) Style
```

Height sets the height of the block before applying margins. If the height of
the text block is less than this value after applying padding (or not), the
block will be set to this height.

#### func (Style) [Hyperlink](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L820) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Hyperlink "Go to Style.Hyperlink")

```
func (s Style) Hyperlink(link string, params ...string) Style
```

Hyperlink sets a hyperlink on a style. This is useful for rendering text that
can be clicked on in a terminal emulator that supports hyperlinks.

Example:

```
s := lipgloss.NewStyle().Hyperlink("https://charm.sh")
s := lipgloss.NewStyle().Hyperlink("https://charm.sh", "id=1")
```

#### func (Style) [Inherit](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L238) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Inherit "Go to Style.Inherit")

```
func (s Style) Inherit(i Style) Style
```

Inherit overlays the style in the argument onto this style by copying each explicitly
set value from the argument style onto this style if it is not already explicitly set.
Existing set values are kept intact and not overwritten.

Margins, padding, and underlying string values are not inherited.

#### func (Style) [Inline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L732) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Inline "Go to Style.Inline")

```
func (s Style) Inline(v bool) Style
```

Inline makes rendering output one line and disables the rendering of
margins, padding and borders. This is useful when you need a style to apply
only to font rendering and don't want it to change any physical dimensions.
It works well with Style.MaxWidth.

Because this in intended to be used at the time of render, this method will
not mutate the style and instead return a copy.

Example:

```
var userInput string = "..."
var userStyle = text.Style{ /* ... */ }
fmt.Println(userStyle.Inline(true).Render(userInput))
```

#### func (Style) [Italic](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L202) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Italic "Go to Style.Italic")

```
func (s Style) Italic(v bool) Style
```

Italic sets an italic formatting rule. In some terminal emulators this will
render with "reverse" coloring if not italic font variant is available.

#### func (Style) [Margin](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L415) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Margin "Go to Style.Margin")

```
func (s Style) Margin(i ...int) Style
```

Margin is a shorthand method for setting margins on all sides at once.

With one argument, the value is applied to all sides.

With two arguments, the value is applied to the vertical and horizontal
sides, in that order.

With three arguments, the value is applied to the top side, the horizontal
sides, and the bottom side, in that order.

With four arguments, the value is applied clockwise starting from the top
side, followed by the right side, then the bottom, and finally the left.

With more than four arguments no margin will be added.

#### func (Style) [MarginBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L455) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MarginBackground "Go to Style.MarginBackground")

```
func (s Style) MarginBackground(c color.Color) Style
```

MarginBackground sets the background color of the margin. Note that this is
also set when inheriting from a style with a background color. In that case
the background color on that style will set the margin color on this style.

#### func (Style) [MarginBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L447) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MarginBottom "Go to Style.MarginBottom")

```
func (s Style) MarginBottom(i int) Style
```

MarginBottom sets the value of the bottom margin.

#### func (Style) [MarginChar](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L462) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MarginChar "Go to Style.MarginChar")

```
func (s Style) MarginChar(r rune) Style
```

MarginChar sets the character used for the margin. This is useful for
rendering blocks with a specific character, such as a space or a dot.

#### func (Style) [MarginLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L429) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MarginLeft "Go to Style.MarginLeft")

```
func (s Style) MarginLeft(i int) Style
```

MarginLeft sets the value of the left margin.

#### func (Style) [MarginRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L435) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MarginRight "Go to Style.MarginRight")

```
func (s Style) MarginRight(i int) Style
```

MarginRight sets the value of the right margin.

#### func (Style) [MarginTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L441) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MarginTop "Go to Style.MarginTop")

```
func (s Style) MarginTop(i int) Style
```

MarginTop sets the value of the top margin.

#### func (Style) [MaxHeight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L762) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MaxHeight "Go to Style.MaxHeight")

```
func (s Style) MaxHeight(n int) Style
```

MaxHeight applies a max height to a given style. This is useful in enforcing
a certain height at render time, particularly with arbitrary strings and
styles.

Because this in intended to be used at the time of render, this method will
not mutate the style and instead returns a copy.

#### func (Style) [MaxWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L750) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.MaxWidth "Go to Style.MaxWidth")

```
func (s Style) MaxWidth(n int) Style
```

MaxWidth applies a max width to a given style. This is useful in enforcing
a certain width at render time, particularly with arbitrary strings and
styles.

Because this in intended to be used at the time of render, this method will
not mutate the style and instead return a copy.

Example:

```
var userInput string = "..."
var userStyle = text.Style{ /* ... */ }
fmt.Println(userStyle.MaxWidth(16).Render(userInput))
```

#### func (Style) [Padding](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L341) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Padding "Go to Style.Padding")

```
func (s Style) Padding(i ...int) Style
```

Padding is a shorthand method for setting padding on all sides at once.

With one argument, the value is applied to all sides.

With two arguments, the value is applied to the vertical and horizontal
sides, in that order.

With three arguments, the value is applied to the top side, the horizontal
sides, and the bottom side, in that order.

With four arguments, the value is applied clockwise starting from the top
side, followed by the right side, then the bottom, and finally the left.

With more than four arguments no padding will be added.

#### func (Style) [PaddingBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L373) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.PaddingBottom "Go to Style.PaddingBottom")

```
func (s Style) PaddingBottom(i int) Style
```

PaddingBottom adds padding to the bottom of the block.

#### func (Style) [PaddingChar](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L385) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.PaddingChar "Go to Style.PaddingChar")

```
func (s Style) PaddingChar(r rune) Style
```

PaddingChar sets the character used for padding. This is useful for
rendering blocks with a specific character, such as a space or a dot.
Example of using [NBSP](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NBSP) as padding to prevent line breaks:

````
```go
s := lipgloss.NewStyle().PaddingChar(lipgloss.NBSP)
```
````

#### func (Style) [PaddingLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L355) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.PaddingLeft "Go to Style.PaddingLeft")

```
func (s Style) PaddingLeft(i int) Style
```

PaddingLeft adds padding on the left.

#### func (Style) [PaddingRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L361) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.PaddingRight "Go to Style.PaddingRight")

```
func (s Style) PaddingRight(i int) Style
```

PaddingRight adds padding on the right.

#### func (Style) [PaddingTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L367) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.PaddingTop "Go to Style.PaddingTop")

```
func (s Style) PaddingTop(i int) Style
```

PaddingTop adds padding to the top of the block.

#### func (Style) [Render](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L268) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Render "Go to Style.Render")

```
func (s Style) Render(strs ...string) string
```

Render applies the defined style formatting to a given string.

#### func (Style) [Reverse](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L248) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Reverse "Go to Style.Reverse")

```
func (s Style) Reverse(v bool) Style
```

Reverse sets a rule for inverting foreground and background colors.

#### func (Style) [SetString](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L208) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.SetString "Go to Style.SetString")

```
func (s Style) SetString(strs ...string) Style
```

SetString sets the underlying string value for this style. To render once
the underlying string is set, use the [Style.String](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.String). This method is
a convenience for cases when having a stringer implementation is handy, such
as when using fmt.Sprintf. You can also simply define a style and render out
strings directly with [Style.Render](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.Render).

#### func (Style) [Strikethrough](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L242) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Strikethrough "Go to Style.Strikethrough")

```
func (s Style) Strikethrough(v bool) Style
```

Strikethrough sets a strikethrough rule. By default, strikes will not be
drawn on whitespace like margins and padding. To change this behavior set
StrikethroughSpaces.

#### func (Style) [StrikethroughSpaces](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L796) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.StrikethroughSpaces "Go to Style.StrikethroughSpaces")

```
func (s Style) StrikethroughSpaces(v bool) Style
```

StrikethroughSpaces determines whether to apply strikethroughs to spaces
between words. By default, this is true. Spaces can also be struck without
underlining the text itself.

#### func (Style) [String](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L221) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.String "Go to Style.String")

```
func (s Style) String() string
```

String implements stringer for a Style, returning the rendered result based
on the rules in this style. An underlying string value must be set with
Style.SetString prior to using this method.

#### func (Style) [TabWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L777) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.TabWidth "Go to Style.TabWidth")

```
func (s Style) TabWidth(n int) Style
```

TabWidth sets the number of spaces that a tab (/t) should be rendered as.
When set to 0, tabs will be removed. To disable the replacement of tabs with
spaces entirely, set this to [NoTabConversion](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#NoTabConversion).

By default, tabs will be replaced with 4 spaces.

#### func (Style) [Transform](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L808) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Transform "Go to Style.Transform")

```
func (s Style) Transform(fn func(string) string) Style
```

Transform applies a given function to a string at render time, allowing for
the string being rendered to be manipuated.

Example:

```
s := NewStyle().Transform(strings.ToUpper)
fmt.Println(s.Render("raow!") // "RAOW!"
```

#### func (Style) [Underline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L210) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Underline "Go to Style.Underline")

```
func (s Style) Underline(v bool) Style
```

Underline sets an underline rule. By default, underlines will not be drawn on
whitespace like margins and padding. To change this behavior set
[Style.UnderlineSpaces](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#Style.UnderlineSpaces).

#### func (Style) [UnderlineColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L234) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnderlineColor "Go to Style.UnderlineColor")

```
func (s Style) UnderlineColor(c color.Color) Style
```

UnderlineColor sets the color of the underline. By default, the underline
will be the same color as the foreground.

Note that not all terminal emulators support colored underlines. If color is
not supported, it might produce unexpected results. This depends on the
terminal emulator being used.

#### func (Style) [UnderlineSpaces](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L788) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnderlineSpaces "Go to Style.UnderlineSpaces")

```
func (s Style) UnderlineSpaces(v bool) Style
```

UnderlineSpaces determines whether to underline spaces between words. By
default, this is true. Spaces can also be underlined without underlining the
text itself.

#### func (Style) [UnderlineStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L223) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnderlineStyle "Go to Style.UnderlineStyle")

```
func (s Style) UnderlineStyle(u Underline) Style
```

UnderlineStyle sets the underline style. This can be used to set the underline
to be a single, double, curly, dotted, or dashed line.

Note that not all terminal emulators support underline styles. If a style is
not supported, it will typically fall back to a single underline but this is
not guaranteed. This depends on the terminal emulator being used.

#### func (Style) [UnsetAlign](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L74) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetAlign "Go to Style.UnsetAlign")

```
func (s Style) UnsetAlign() Style
```

UnsetAlign removes the horizontal and vertical text alignment style rule, if set.

#### func (Style) [UnsetAlignHorizontal](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L81) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetAlignHorizontal "Go to Style.UnsetAlignHorizontal")

```
func (s Style) UnsetAlignHorizontal() Style
```

UnsetAlignHorizontal removes the horizontal text alignment style rule, if set.

#### func (Style) [UnsetAlignVertical](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L87) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetAlignVertical "Go to Style.UnsetAlignVertical")

```
func (s Style) UnsetAlignVertical() Style
```

UnsetAlignVertical removes the vertical text alignment style rule, if set.

#### func (Style) [UnsetBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L56) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBackground "Go to Style.UnsetBackground")

```
func (s Style) UnsetBackground() Style
```

UnsetBackground removes the background style rule, if set.

#### func (Style) [UnsetBlink](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L38) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBlink "Go to Style.UnsetBlink")

```
func (s Style) UnsetBlink() Style
```

UnsetBlink removes the blink style rule, if set.

#### func (Style) [UnsetBold](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L9) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBold "Go to Style.UnsetBold")

```
func (s Style) UnsetBold() Style
```

UnsetBold removes the bold style rule, if set.

#### func (Style) [UnsetBorderBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L262) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderBackground "Go to Style.UnsetBorderBackground")

```
func (s Style) UnsetBorderBackground() Style
```

UnsetBorderBackground removes all border background color styles, if
set.

#### func (Style) [UnsetBorderBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L198) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderBottom "Go to Style.UnsetBorderBottom")

```
func (s Style) UnsetBorderBottom() Style
```

UnsetBorderBottom removes the border bottom style rule, if set.

#### func (Style) [UnsetBorderBottomBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L294) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderBottomBackground "Go to Style.UnsetBorderBottomBackground")

```
func (s Style) UnsetBorderBottomBackground() Style
```

UnsetBorderBottomBackground removes the bottom border background color
rule, if set.

#### func (Style) [UnsetBorderBottomForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L234) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderBottomForeground "Go to Style.UnsetBorderBottomForeground")

```
func (s Style) UnsetBorderBottomForeground() Style
```

UnsetBorderBottomForeground removes the bottom border foreground color
rule, if set.

#### func (Style) [UnsetBorderForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L210) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderForeground "Go to Style.UnsetBorderForeground")

```
func (s Style) UnsetBorderForeground() Style
```

UnsetBorderForeground removes all border foreground color styles, if set.

#### func (Style) [UnsetBorderForegroundBlend](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L248) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderForegroundBlend "Go to Style.UnsetBorderForegroundBlend")

```
func (s Style) UnsetBorderForegroundBlend() Style
```

UnsetBorderForegroundBlend removes the border blend foreground color rules,
if set.

#### func (Style) [UnsetBorderForegroundBlendOffset](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L255) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderForegroundBlendOffset "Go to Style.UnsetBorderForegroundBlendOffset")

```
func (s Style) UnsetBorderForegroundBlendOffset() Style
```

UnsetBorderForegroundBlendOffset removes the border blend offset style rule,
if set.

#### func (Style) [UnsetBorderLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L204) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderLeft "Go to Style.UnsetBorderLeft")

```
func (s Style) UnsetBorderLeft() Style
```

UnsetBorderLeft removes the border left style rule, if set.

#### func (Style) [UnsetBorderLeftBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L300) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderLeftBackground "Go to Style.UnsetBorderLeftBackground")

```
func (s Style) UnsetBorderLeftBackground() Style
```

UnsetBorderLeftBackground removes the left border color rule, if set.

#### func (Style) [UnsetBorderLeftForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L241) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderLeftForeground "Go to Style.UnsetBorderLeftForeground")

```
func (s Style) UnsetBorderLeftForeground() Style
```

UnsetBorderLeftForeground removes the left border foreground color rule,
if set.

#### func (Style) [UnsetBorderRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L192) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderRight "Go to Style.UnsetBorderRight")

```
func (s Style) UnsetBorderRight() Style
```

UnsetBorderRight removes the border right style rule, if set.

#### func (Style) [UnsetBorderRightBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L287) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderRightBackground "Go to Style.UnsetBorderRightBackground")

```
func (s Style) UnsetBorderRightBackground() Style
```

UnsetBorderRightBackground removes the right border background color
rule, if set.

#### func (Style) [UnsetBorderRightForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L227) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderRightForeground "Go to Style.UnsetBorderRightForeground")

```
func (s Style) UnsetBorderRightForeground() Style
```

UnsetBorderRightForeground removes the right border foreground color rule,
if set.

#### func (Style) [UnsetBorderStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L180) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderStyle "Go to Style.UnsetBorderStyle")

```
func (s Style) UnsetBorderStyle() Style
```

UnsetBorderStyle removes the border style rule, if set.

#### func (Style) [UnsetBorderTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L186) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderTop "Go to Style.UnsetBorderTop")

```
func (s Style) UnsetBorderTop() Style
```

UnsetBorderTop removes the border top style rule, if set.

#### func (Style) [UnsetBorderTopBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L280) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderTopBackground "Go to Style.UnsetBorderTopBackground")

```
func (s Style) UnsetBorderTopBackground() Style
```

UnsetBorderTopBackground removes the top border background color rule,
if set.

#### func (Style) [UnsetBorderTopBackgroundColor](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L274) deprecated

```
func (s Style) UnsetBorderTopBackgroundColor() Style
```

UnsetBorderTopBackgroundColor removes the top border background color rule,
if set.

Deprecated: This function simply calls Style.UnsetBorderTopBackground.

#### func (Style) [UnsetBorderTopForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L220) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetBorderTopForeground "Go to Style.UnsetBorderTopForeground")

```
func (s Style) UnsetBorderTopForeground() Style
```

UnsetBorderTopForeground removes the top border foreground color rule,
if set.

#### func (Style) [UnsetColorWhitespace](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L133) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetColorWhitespace "Go to Style.UnsetColorWhitespace")

```
func (s Style) UnsetColorWhitespace() Style
```

UnsetColorWhitespace removes the rule for coloring padding, if set.

#### func (Style) [UnsetFaint](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L44) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetFaint "Go to Style.UnsetFaint")

```
func (s Style) UnsetFaint() Style
```

UnsetFaint removes the faint style rule, if set.

#### func (Style) [UnsetForeground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L50) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetForeground "Go to Style.UnsetForeground")

```
func (s Style) UnsetForeground() Style
```

UnsetForeground removes the foreground style rule, if set.

#### func (Style) [UnsetHeight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L68) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetHeight "Go to Style.UnsetHeight")

```
func (s Style) UnsetHeight() Style
```

UnsetHeight removes the height style rule, if set.

#### func (Style) [UnsetHyperlink](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L348) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetHyperlink "Go to Style.UnsetHyperlink")

```
func (s Style) UnsetHyperlink() Style
```

UnsetHyperlink removes the value set by Hyperlink.

#### func (Style) [UnsetInline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L306) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetInline "Go to Style.UnsetInline")

```
func (s Style) UnsetInline() Style
```

UnsetInline removes the inline style rule, if set.

#### func (Style) [UnsetItalic](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L15) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetItalic "Go to Style.UnsetItalic")

```
func (s Style) UnsetItalic() Style
```

UnsetItalic removes the italic style rule, if set.

#### func (Style) [UnsetMarginBackground](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L174) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMarginBackground "Go to Style.UnsetMarginBackground")

```
func (s Style) UnsetMarginBackground() Style
```

UnsetMarginBackground removes the margin's background color. Note that the
margin's background color can be set from the background color of another
style during inheritance.

#### func (Style) [UnsetMarginBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L166) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMarginBottom "Go to Style.UnsetMarginBottom")

```
func (s Style) UnsetMarginBottom() Style
```

UnsetMarginBottom removes the bottom margin style rule, if set.

#### func (Style) [UnsetMarginLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L148) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMarginLeft "Go to Style.UnsetMarginLeft")

```
func (s Style) UnsetMarginLeft() Style
```

UnsetMarginLeft removes the left margin style rule, if set.

#### func (Style) [UnsetMarginRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L154) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMarginRight "Go to Style.UnsetMarginRight")

```
func (s Style) UnsetMarginRight() Style
```

UnsetMarginRight removes the right margin style rule, if set.

#### func (Style) [UnsetMarginTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L160) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMarginTop "Go to Style.UnsetMarginTop")

```
func (s Style) UnsetMarginTop() Style
```

UnsetMarginTop removes the top margin style rule, if set.

#### func (Style) [UnsetMargins](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L139) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMargins "Go to Style.UnsetMargins")

```
func (s Style) UnsetMargins() Style
```

UnsetMargins removes all margin style rules.

#### func (Style) [UnsetMaxHeight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L318) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMaxHeight "Go to Style.UnsetMaxHeight")

```
func (s Style) UnsetMaxHeight() Style
```

UnsetMaxHeight removes the max height style rule, if set.

#### func (Style) [UnsetMaxWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L312) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetMaxWidth "Go to Style.UnsetMaxWidth")

```
func (s Style) UnsetMaxWidth() Style
```

UnsetMaxWidth removes the max width style rule, if set.

#### func (Style) [UnsetPadding](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L93) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetPadding "Go to Style.UnsetPadding")

```
func (s Style) UnsetPadding() Style
```

UnsetPadding removes all padding style rules.

#### func (Style) [UnsetPaddingBottom](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L127) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetPaddingBottom "Go to Style.UnsetPaddingBottom")

```
func (s Style) UnsetPaddingBottom() Style
```

UnsetPaddingBottom removes the bottom padding style rule, if set.

#### func (Style) [UnsetPaddingChar](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L103) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetPaddingChar "Go to Style.UnsetPaddingChar")

```
func (s Style) UnsetPaddingChar() Style
```

UnsetPaddingChar removes the padding character style rule, if set.

#### func (Style) [UnsetPaddingLeft](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L109) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetPaddingLeft "Go to Style.UnsetPaddingLeft")

```
func (s Style) UnsetPaddingLeft() Style
```

UnsetPaddingLeft removes the left padding style rule, if set.

#### func (Style) [UnsetPaddingRight](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L115) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetPaddingRight "Go to Style.UnsetPaddingRight")

```
func (s Style) UnsetPaddingRight() Style
```

UnsetPaddingRight removes the right padding style rule, if set.

#### func (Style) [UnsetPaddingTop](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L121) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetPaddingTop "Go to Style.UnsetPaddingTop")

```
func (s Style) UnsetPaddingTop() Style
```

UnsetPaddingTop removes the top padding style rule, if set.

#### func (Style) [UnsetReverse](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L32) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetReverse "Go to Style.UnsetReverse")

```
func (s Style) UnsetReverse() Style
```

UnsetReverse removes the reverse style rule, if set.

#### func (Style) [UnsetStrikethrough](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L26) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetStrikethrough "Go to Style.UnsetStrikethrough")

```
func (s Style) UnsetStrikethrough() Style
```

UnsetStrikethrough removes the strikethrough style rule, if set.

#### func (Style) [UnsetStrikethroughSpaces](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L336) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetStrikethroughSpaces "Go to Style.UnsetStrikethroughSpaces")

```
func (s Style) UnsetStrikethroughSpaces() Style
```

UnsetStrikethroughSpaces removes the value set by StrikethroughSpaces.

#### func (Style) [UnsetString](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L356) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetString "Go to Style.UnsetString")

```
func (s Style) UnsetString() Style
```

UnsetString sets the underlying string value to the empty string.

#### func (Style) [UnsetTabWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L324) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetTabWidth "Go to Style.UnsetTabWidth")

```
func (s Style) UnsetTabWidth() Style
```

UnsetTabWidth removes the tab width style rule, if set.

#### func (Style) [UnsetTransform](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L342) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetTransform "Go to Style.UnsetTransform")

```
func (s Style) UnsetTransform() Style
```

UnsetTransform removes the value set by Transform.

#### func (Style) [UnsetUnderline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L21) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetUnderline "Go to Style.UnsetUnderline")

```
func (s Style) UnsetUnderline() Style
```

UnsetUnderline removes the underline style rule, if set.

#### func (Style) [UnsetUnderlineSpaces](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L330) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetUnderlineSpaces "Go to Style.UnsetUnderlineSpaces")

```
func (s Style) UnsetUnderlineSpaces() Style
```

UnsetUnderlineSpaces removes the value set by UnderlineSpaces.

#### func (Style) [UnsetWidth](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go\#L62) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.UnsetWidth "Go to Style.UnsetWidth")

```
func (s Style) UnsetWidth() Style
```

UnsetWidth removes the width style rule, if set.

#### func (Style) [Value](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L214) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Value "Go to Style.Value")

```
func (s Style) Value() string
```

Value returns the raw, unformatted, underlying string value for this style.

#### func (Style) [Width](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go\#L286) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Style.Width "Go to Style.Width")

```
func (s Style) Width(i int) Style
```

Width sets the width of the block before applying margins. This means your
styled content will exactly equal the size set here. Text will wrap based on
Padding and Borders set on the style.

#### type [Underline](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go\#L117) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#Underline "Go to Underline")

```
type Underline = ansi.Underline
```

Underline is the style of the underline.

Caveats:
\- Not all terminals support all underline styles.
\- Some terminals may render unsupported styles as standard underlines.
\- Terminal themes may affect the visibility of different underline styles.

#### type [WhitespaceOption](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/whitespace.go\#L62) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WhitespaceOption "Go to WhitespaceOption")

```
type WhitespaceOption func(*whitespace)
```

WhitespaceOption sets a styling rule for rendering whitespace.

#### func [WithWhitespaceChars](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/whitespace.go\#L72) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WithWhitespaceChars "Go to WithWhitespaceChars")

```
func WithWhitespaceChars(s string) WhitespaceOption
```

WithWhitespaceChars sets the characters to be rendered in the whitespace.

#### func [WithWhitespaceStyle](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/whitespace.go\#L65) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WithWhitespaceStyle "Go to WithWhitespaceStyle")

```
func WithWhitespaceStyle(s Style) WhitespaceOption
```

WithWhitespaceStyle sets the style for the whitespace.

#### type [WrapWriter](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L26) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WrapWriter "Go to WrapWriter")

```
type WrapWriter struct {
	// contains filtered or unexported fields
}
```

WrapWriter is a writer that writes to a buffer and keeps track of the
current pen style and link state for the purpose of wrapping with newlines.

When it encounters a newline, it resets the style and link, writes the
newline, and then reapplies the style and link to the next line.

#### func [NewWrapWriter](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L34) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#NewWrapWriter "Go to NewWrapWriter")

```
func NewWrapWriter(w io.Writer) *WrapWriter
```

NewWrapWriter returns a new [WrapWriter](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6#WrapWriter).

#### func (\*WrapWriter) [Close](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L101) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WrapWriter.Close "Go to WrapWriter.Close")

```
func (w *WrapWriter) Close() error
```

Close closes the writer, resets the style and link if necessary, and releases
its parser. Calling it is performance critical, but forgetting it does not
cause safety issues or leaks.

#### func (\*WrapWriter) [Link](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L60) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WrapWriter.Link "Go to WrapWriter.Link")

```
func (w *WrapWriter) Link() uv.Link
```

Link returns the current pen link.

#### func (\*WrapWriter) [Style](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L55) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WrapWriter.Style "Go to WrapWriter.Style")

```
func (w *WrapWriter) Style() uv.Style
```

Style returns the current pen style.

#### func (\*WrapWriter) [Write](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go\#L65) [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#WrapWriter.Write "Go to WrapWriter.Write")

```
func (w *WrapWriter) Write(p []byte) (int, error)
```

Write writes to the buffer.

## ![](https://pkg.go.dev/static/shared/icon/insert_drive_file_gm_grey_24dp.svg)  Source Files  [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#section-sourcefiles "Go to Source Files")

[View all Source files](https://github.com/charmbracelet/lipgloss/tree/v2.0.6)

- [align.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/align.go "align.go")
- [ansi\_unix.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/ansi_unix.go "ansi_unix.go")
- [blending.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/blending.go "blending.go")
- [borders.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/borders.go "borders.go")
- [canvas.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/canvas.go "canvas.go")
- [color.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/color.go "color.go")
- [get.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/get.go "get.go")
- [join.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/join.go "join.go")
- [layer.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/layer.go "layer.go")
- [lipgloss.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/lipgloss.go "lipgloss.go")
- [position.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/position.go "position.go")
- [query.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/query.go "query.go")
- [ranges.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/ranges.go "ranges.go")
- [runes.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/runes.go "runes.go")
- [set.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/set.go "set.go")
- [size.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/size.go "size.go")
- [style.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/style.go "style.go")
- [terminal.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/terminal.go "terminal.go")
- [unset.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/unset.go "unset.go")
- [whitespace.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/whitespace.go "whitespace.go")
- [wrap.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/wrap.go "wrap.go")
- [writer.go](https://github.com/charmbracelet/lipgloss/blob/v2.0.6/writer.go "writer.go")

## ![](https://pkg.go.dev/static/shared/icon/folder_gm_grey_24dp.svg)  Directories  [¶](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6\#section-directories "Go to Directories")

Show internal
Collapse all

| Path | Synopsis |
| --- | --- |
| [compat](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6/compat)<br>Package compat is a compatibility layer for Lip Gloss that provides a way to deal with the hassle of setting up a writer. | Package compat is a compatibility layer for Lip Gloss that provides a way to deal with the hassle of setting up a writer. |
| [list](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6/list)<br>Package list allows you to build lists, as simple or complicated as you need. | Package list allows you to build lists, as simple or complicated as you need. |
| [table](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6/table)<br>Package table provides a styled table renderer for terminals. | Package table provides a styled table renderer for terminals. |
| [tree](https://pkg.go.dev/charm.land/lipgloss/v2@v2.0.6/tree)<br>Package tree allows you to build trees, as simple or complicated as you need. | Package tree allows you to build trees, as simple or complicated as you need. |

Click to show internal directories.

Click to hide internal directories.

## Jump to

![](https://pkg.go.dev/static/shared/icon/close_gm_grey_24dp.svg)

Close

## Keyboard shortcuts

![](https://pkg.go.dev/static/shared/icon/close_gm_grey_24dp.svg)

|     |     |
| --- | --- |
| **?** | : This menu |
| **/** | : Search site |
| **f** or **F** | : Jump to |
| **y** or **Y** | : Canonical URL |

Close

go.dev uses cookies from Google to deliver and enhance the quality of its services and to
analyze traffic. [Learn more.](https://policies.google.com/technologies/cookies)

Okay