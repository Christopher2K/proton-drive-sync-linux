package main

import (
	"fmt"
	"time"

	"github.com/egoist/mygo/ui"
)

const (
	sidebarWidth = 232.0
	pagePadding  = 28.0
)

var (
	bg           = ui.RGB(0x10, 0x12, 0x18)
	sidebar      = ui.RGB(0x15, 0x18, 0x21)
	card         = ui.RGB(0x1a, 0x1e, 0x28)
	cardHover    = ui.RGB(0x22, 0x27, 0x33)
	inset        = ui.RGB(0x0d, 0x0f, 0x14)
	border       = ui.RGB(0x27, 0x2d, 0x3a)
	borderStrong = ui.RGB(0x36, 0x40, 0x52)
	text         = ui.RGB(0xe7, 0xea, 0xf0)
	muted        = ui.RGB(0x98, 0xa1, 0xb3)
	faint        = ui.RGB(0x6b, 0x74, 0x86)
	accent       = ui.RGB(0x5b, 0x8d, 0xef)
	accentHover  = ui.RGB(0x6f, 0x9b, 0xf5)
	ok           = ui.RGB(0x44, 0xd1, 0x9a)
	okSoft       = ui.RGB(0x11, 0x32, 0x27)
	warn         = ui.RGB(0xf0, 0xb6, 0x4a)
	warnSoft     = ui.RGB(0x33, 0x28, 0x0e)
	danger       = ui.RGB(0xef, 0x73, 0x73)
	dangerSoft   = ui.RGB(0x36, 0x1a, 0x1f)
)

func appTheme() *ui.Theme {
	return &ui.Theme{
		Dark:           true,
		Background:     bg,
		Surface:        card,
		SurfaceHover:   cardHover,
		SurfacePressed: cardHover,
		Border:         border,
		Text:           text,
		TextMuted:      muted,
		Accent:         accent,
		AccentHover:    accentHover,
		AccentPressed:  accent,
		AccentText:     ui.RGB(0xff, 0xff, 0xff),
		Danger:         danger,
		Warning:        warn,
		Success:        ok,
		Selection:      ui.RGBA(0x5b, 0x8d, 0xef, 0.4),
		Focus:          ui.RGBA(0x6f, 0x9b, 0xf5, 0.6),
		Inverse:        text,
		InverseText:    bg,
		Scrollbar:      ui.RGBA(0xff, 0xff, 0xff, 0.35),
		Radius:         8,
		Spacing:        4,
		FontSize:       14,
	}
}

// ---------------------------------------------------------------------------
// UI: primitives
// ---------------------------------------------------------------------------

// appCard mirrors components::card: gap 3, p 4 (16), radius 12.
func appCard(c *ui.Context, children func()) ui.Element {
	t := c.Theme()
	return ui.Box(c).Background(t.Surface).Border(1, t.Border).Radius(12).Padding(16).Children(children)
}

func pageCard(c *ui.Context, children func()) ui.Element {
	return appCard(c, children).FillWidth()
}

// statusDot mirrors components::status_dot: 7px dot, text_sm label.
func statusDot(c *ui.Context, label string, color ui.Color) ui.Element {
	t := c.Theme()
	return ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Children(func() {
		ui.Box(c).Width(7).Height(7).Radius(3.5).Background(color)
		ui.Text(c, label).TextColor(color).FontSize(t.FontSize).Grow(1)
	})
}

// pill mirrors components::pill: px 2 (8), py 0.5 (2), radius full, text_xs.
func pill(c *ui.Context, txt string, fg, bgc ui.Color) ui.Element {
	t := c.Theme()
	return ui.Box(c).Background(bgc).Padding(2, 8).Radius(999).Children(func() {
		ui.Text(c, txt).TextColor(fg).FontSize(t.FontSize * 0.85)
	})
}

// statBox mirrors components::stat: gap 1, p 3 (12), radius 10, inset bg.
func statBox(c *ui.Context, label, value string, color ui.Color) ui.Element {
	t := c.Theme()
	return ui.Box(c).Grow(1).Background(inset).Padding(12).Radius(10).Children(func() {
		ui.Column(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, value).FontSize(t.FontSize * 2).Bold().TextColor(color)
			ui.Text(c, label).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
		})
	})
}

// sectionTitle mirrors components::section_title: text_base semibold + text_xs.
func sectionTitle(c *ui.Context, title, subtitle string) ui.Element {
	t := c.Theme()
	return ui.Column(c).Gap(t.Space(0.5)).Children(func() {
		ui.Text(c, title).FontSize(t.FontSize).Bold()
		if subtitle != "" {
			ui.Text(c, subtitle).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
		}
	})
}

// emptyState mirrors components::empty_state: py 48, gap 2, centered.
func emptyState(c *ui.Context, title, body string) ui.Element {
	t := c.Theme()
	return ui.Column(c).Gap(t.Space(2)).AlignItems(ui.Center).Justify(ui.Center).Padding(48, 0).Children(func() {
		ui.Text(c, title).FontSize(t.FontSize).Bold().TextColor(t.Text)
		ui.Text(c, body).FontSize(t.FontSize).TextColor(t.TextMuted)
	})
}

// banner mirrors components::banner: gap 1, p 3 (12), radius 10.
func banner(c *ui.Context, title, body string, fg, bgc ui.Color) ui.Element {
	t := c.Theme()
	return ui.Box(c).Border(1, fg).Background(bgc).Padding(12).Radius(10).Children(func() {
		ui.Column(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, title).FontSize(t.FontSize).TextColor(fg)
			ui.Text(c, body).FontSize(t.FontSize).TextColor(t.TextMuted)
		})
	})
}

// fieldRow mirrors components::field_row: py 2, bottom border.
func fieldRow(c *ui.Context, label, value string) ui.Element {
	t := c.Theme()
	return ui.Row(c).Gap(t.Space(4)).AlignItems(ui.Center).Padding(t.Space(2), 0).BorderWidth(0, 0, 1, 0).BorderColor(t.Border).Children(func() {
		ui.Text(c, label).FontSize(t.FontSize).TextColor(t.TextMuted)
		ui.Spacer(c)
		ui.Text(c, value).FontSize(t.FontSize).TextColor(t.Text)
	})
}

func chipButton(c *ui.Context, label string, active bool, onClick func()) ui.Element {
	t := c.Theme()
	el := ui.Button(c, label).FontSize(t.FontSize*0.85).Padding(t.Space(1), 12).Radius(6)
	if active {
		el = el.Background(inset).Border(1, accent).TextColor(accent)
	} else {
		el = el.Background(t.Surface).Border(1, t.Border).TextColor(t.TextMuted)
	}
	if el.Clicked() {
		onClick()
	}
	return el
}

func kindLabel(kind string) string {
	switch kind {
	case "Uploaded":
		return "Uploaded"
	case "Downloaded":
		return "Downloaded"
	case "Trashed":
		return "Trashed"
	case "Conflict":
		return "Conflict"
	case "GuardPaused":
		return "Guard paused"
	case "UploadErrored":
		return "Upload error"
	case "DownloadErrored":
		return "Download error"
	case "ConnectionError":
		return "Connection error"
	case "SignInRequired":
		return "Sign-in required"
	case "SidecarUnavailable":
		return "Sidecar unavailable"
	case "Skipped":
		return "Skipped"
	}
	return kind
}

func kindColor(kind string) ui.Color {
	switch kind {
	case "Uploaded":
		return accent
	case "Downloaded":
		return ok
	case "Trashed":
		return muted
	case "Conflict":
		return warn
	case "GuardPaused", "UploadErrored", "DownloadErrored", "SignInRequired", "SidecarUnavailable":
		return danger
	case "ConnectionError", "Skipped":
		return warn
	}
	return muted
}

func formatTime(unix int64) string {
	t := time.Unix(unix, 0).Local()
	return t.Format("15:04:05")
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ---------------------------------------------------------------------------
// UI: root view
// ---------------------------------------------------------------------------
