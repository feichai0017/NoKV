package plot

import (
	"image/color"

	goplot "gonum.org/v1/plot"
	"gonum.org/v1/plot/font"
	"gonum.org/v1/plot/vg"
)

// Theme defines a consistent, paper-friendly chart style.
type Theme struct {
	Background color.Color
	Grid       color.Color
	Text       color.Color
	Axis       color.Color
	Palette    []color.Color

	TitleSize  vg.Length
	LabelSize  vg.Length
	TickSize   vg.Length
	LegendSize vg.Length
}

// DefaultTheme returns a colorblind-safe, publication-oriented plotting theme.
func DefaultTheme() Theme {
	return Theme{
		Background: rgba(0xFC, 0xFC, 0xFA, 0xFF),
		Grid:       rgba(0xE3, 0xE7, 0xEC, 0xFF),
		Text:       rgba(0x1F, 0x29, 0x37, 0xFF),
		Axis:       rgba(0x5B, 0x67, 0x75, 0xFF),
		Palette: []color.Color{
			rgba(0x25, 0x63, 0xEB, 0xFF), // strong blue
			rgba(0xD9, 0x77, 0x06, 0xFF), // amber
			rgba(0x05, 0x96, 0x69, 0xFF), // green
			rgba(0xBE, 0x18, 0x5D, 0xFF), // rose
			rgba(0x08, 0x91, 0xB2, 0xFF), // cyan
			rgba(0x7C, 0x3A, 0xED, 0xFF), // violet
			rgba(0x6B, 0x72, 0x80, 0xFF), // neutral gray
		},
		TitleSize:  vg.Points(16),
		LabelSize:  vg.Points(12),
		TickSize:   vg.Points(10),
		LegendSize: vg.Points(10.5),
	}
}

// Apply mutates the plot to use the configured publication theme.
func (t Theme) Apply(p *goplot.Plot) {
	p.BackgroundColor = t.Background

	p.Title.TextStyle.Color = t.Text
	p.Title.TextStyle.Font.Size = t.TitleSize
	p.Title.Padding = vg.Millimeter

	p.X.Label.TextStyle.Color = t.Text
	p.X.Label.TextStyle.Font.Size = t.LabelSize
	p.X.Tick.Color = t.Axis
	p.X.LineStyle.Color = t.Axis
	p.X.LineStyle.Width = vg.Points(0.8)
	p.X.Tick.Label.Color = t.Text
	p.X.Tick.Label.Font.Size = t.TickSize

	p.Y.Label.TextStyle.Color = t.Text
	p.Y.Label.TextStyle.Font.Size = t.LabelSize
	p.Y.Tick.Color = t.Axis
	p.Y.LineStyle.Color = t.Axis
	p.Y.LineStyle.Width = vg.Points(0.8)
	p.Y.Tick.Label.Color = t.Text
	p.Y.Tick.Label.Font.Size = t.TickSize

	p.Legend.TextStyle.Color = t.Text
	p.Legend.TextStyle.Font.Size = t.LegendSize
	p.Legend.Padding = vg.Points(4)
	p.Legend.Top = true
	p.Legend.XOffs = -vg.Points(1)
}

func defaultFont(size vg.Length) font.Font {
	return font.Font{Typeface: "Liberation", Variant: "Sans", Size: size}
}

func rgba(r, g, b, a uint8) color.RGBA {
	return color.RGBA{R: r, G: g, B: b, A: a}
}
