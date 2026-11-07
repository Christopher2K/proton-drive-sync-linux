package main

import (
	"fmt"
	"strconv"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

// Root view, shell chrome (sidebar, header, status bar) and page routing.

func (a *app) view(c *ui.Context) {
	c.SetTheme(appTheme())
	t := c.Theme()

	a.mu.Lock()
	ph := a.phase
	a.mu.Unlock()

	ui.Column(c).Fill().Background(t.Background).Children(func() {
		switch ph {
		case phaseStarting:
			a.renderSplash(c)
		case phaseNeedsLogin:
			a.renderLogin(c)
		case phaseFailed:
			a.renderFailure(c)
		case phaseRunning:
			a.renderShell(c)
		}
	})

	a.mu.Lock()
	so := a.signOut
	ms := a.modeSwitch
	a.mu.Unlock()

	if so != signOutNone {
		a.renderSignOutModal(c)
	} else if ms != modeSwitchNone {
		a.renderModeSwitchModal(c)
	}
}

func (a *app) renderSplash(c *ui.Context) {
	t := c.Theme()
	// Mirrors render_splash: centered brand block, status dot, and a
	// 420-wide-max disclosure line.
	ui.Column(c).Fill().AlignItems(ui.Center).Justify(ui.Center).Gap(t.Space(4)).Children(func() {
		a.brandBlock(c)
		statusDot(c, "Starting the sync engine…", accent)
		ui.Text(c, disclosure).FontSize(t.FontSize * 0.85).TextColor(faint).MaxWidth(420).TextAlign(ui.Center)
	})
}

func (a *app) renderFailure(c *ui.Context) {
	t := c.Theme()
	a.mu.Lock()
	fatal := a.fatal
	a.mu.Unlock()
	if fatal == "" {
		fatal = "The sync engine stopped unexpectedly."
	}

	ui.Column(c).Fill().AlignItems(ui.Center).Justify(ui.Center).Padding(t.Space(6)).Children(func() {
		// Mirrors render_failure: the card itself is 520 wide, p 6 (24),
		// radius 14, gap 4 (16) — not pageCard, which would stretch
		// full width with different padding.
		ui.Box(c).Width(520).Background(t.Surface).Border(1, t.Border).Radius(14).Padding(24).Children(func() {
			ui.Column(c).Gap(t.Space(4)).Children(func() {
				a.brandBlock(c)
				ui.Text(c, "Sync is stopped").FontSize(t.FontSize * 1.25).Bold().TextColor(danger)
				ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
					ui.Text(c, fatal).FontSize(t.FontSize).TextColor(t.TextMuted)
				})
				ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
					if ui.PrimaryButton(c, "Try again").AlignSelf(ui.Start).Clicked() {
						go a.startEngine()
					}
					if ui.Button(c, "Quit").Background(t.SurfaceHover).TextColor(t.Text).AlignSelf(ui.Start).Clicked() {
						go a.quit()
					}
				})
			})
		})
	})
}

func (a *app) renderLogin(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	login := a.login
	a.mu.Unlock()

	ui.Column(c).Fill().AlignItems(ui.Center).Justify(ui.Center).Padding(t.Space(6)).Children(func() {
		// Mirrors render_login: the card itself is 460 wide, p 6 (24),
		// radius 14, gap 4 (16) — not the shared pageCard, which would
		// stretch full width with different padding.
		ui.Box(c).Width(460).Background(t.Surface).Border(1, t.Border).Radius(14).Padding(24).Children(func() {
			ui.Column(c).Gap(t.Space(4)).Children(func() {
				a.brandBlock(c)
				ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
					ui.Text(c, disclosure).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
				})

				switch login.kind {
				case "idle":
					ui.Text(c, "Sign in with your Proton account to start syncing. Your browser handles the sign-in — this app never sees your password.").
						FontSize(t.FontSize).TextColor(t.TextMuted)
					// Buttons hug their label (GPUI px_3 py_1p5);
					// column children stretch by default here.
					if ui.PrimaryButton(c, "Sign in with Proton").AlignSelf(ui.Start).Clicked() {
						go a.startLogin()
					}
				case "waiting":
					ui.Text(c, "Opening the Proton sign-in page…").FontSize(t.FontSize)
					ui.Text(c, "If nothing happens, check that a browser is available.").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
				case "url":
					ui.Text(c, "Finish signing in in the browser window that just opened.").FontSize(t.FontSize)
					if login.userCode != "" {
						ui.Box(c).Background(inset).Padding(t.Space(3)).Radius(t.Radius).Children(func() {
							ui.Text(c, "Verification code").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
							ui.Text(c, login.userCode).FontSize(t.FontSize * 2).Bold()
						})
					}
					ui.Text(c, login.url).FontSize(t.FontSize * 0.85).TextColor(faint).SingleLine()
					ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
						if ui.Button(c, "Open sign-in page again").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							_ = mygo.Shell.OpenExternal(login.url)
						}
						statusDot(c, "Waiting for sign-in…", accent)
					})
				case "failed":
					ui.Text(c, fmt.Sprintf("Sign-in failed: %s", login.err)).FontSize(t.FontSize).TextColor(danger)
					if ui.PrimaryButton(c, "Try again").AlignSelf(ui.Start).Clicked() {
						go a.startLogin()
					}
				}
			})
		})
	})
}

func (a *app) brandBlock(c *ui.Context) ui.Element {
	t := c.Theme()
	return ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
		ui.Box(c).Width(38).Height(38).Radius(10).Background(accent).Children(func() {
			ui.Box(c).Width(17).Height(17).Radius(8.5).Border(2, ui.RGB(0xff, 0xff, 0xff)).Fill()
		})
		ui.Column(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, appTitle).FontSize(t.FontSize * 1.25).Bold()
			ui.Text(c, appTagline).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
		})
	})
}

func (a *app) renderShell(c *ui.Context) {
	ui.Column(c).Fill().Children(func() {
		ui.Row(c).FillWidth().Grow(1).MinHeight(0).Children(func() {
			a.renderSidebar(c)
			ui.Column(c).Grow(1).MinWidth(0).FillHeight().MinHeight(0).Children(func() {
				a.renderHeader(c)
				a.renderPage(c)
			})
		})
		a.renderStatusBar(c)
	})
}

func (a *app) renderSidebar(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	badgeText := a.badge()
	account := a.accountLabel()
	a.mu.Unlock()

	// Mirrors render_sidebar: w 232, p 4 (16), no gap; children stacked with
	// a flex spacer between nav and the status card, right border.
	ui.Column(c).Width(sidebarWidth).FillHeight().Background(sidebar).Padding(16).
		BorderWidth(0, 1, 0, 0).BorderColor(border).
		Children(func() {
			// Brand: 34px icon, radius 10, text_sm semibold + text_xs faint.
			ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
				ui.Box(c).Width(34).Height(34).Radius(10).Background(accent).AlignItems(ui.Center).Justify(ui.Center).Children(func() {
					ui.Box(c).Width(15).Height(15).Radius(7.5).Border(2, ui.RGB(0xff, 0xff, 0xff)).Fill()
				})
				ui.Column(c).Children(func() {
					ui.Text(c, appTitle).FontSize(t.FontSize)
					ui.Text(c, appTagline).FontSize(t.FontSize * 0.85).TextColor(faint)
				})
			})

			// Nav: mt 6 (24), gap 1 (4).
			ui.Column(c).Gap(t.Space(1)).Margin(t.Space(6), 0, 0, 0).Children(func() {
				for _, p := range []page{pageOverview, pageActivity, pageConflicts, pageSettings} {
					a.navItem(c, p)
				}
			})

			ui.Spacer(c)

			// Status card: gap 3 (12), p 3 (12), radius 10.
			ui.Box(c).Background(card).Border(1, border).Radius(10).Padding(12).Children(func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					dot, _, _ := a.badgeColors()
					ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Children(func() {
						ui.Box(c).Width(7).Height(7).Radius(3.5).Background(dot)
						ui.Text(c, badgeText).TextColor(dot).FontSize(t.FontSize * 0.85)
					})
					ui.Text(c, account).FontSize(t.FontSize).SingleLine()
					pauseColor := accent
					switch a.engineState() {
					case "Paused":
						pauseColor = ok
					case "GuardPaused":
						pauseColor = warn
					}
					if ui.Button(c, a.pauseLabel()).Background(pauseColor).TextColor(ui.RGB(0xff, 0xff, 0xff)).FillWidth().FontSize(t.FontSize).Clicked() {
						go a.togglePause()
					}
				})
			})

			// Footer: mt 3 (12), text_xs faint.
			ui.Row(c).Margin(t.Space(3), 0, 0, 0).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
				ui.Text(c, fmt.Sprintf("v%s", appVersion())).FontSize(t.FontSize * 0.85).TextColor(faint)
				ui.Text(c, "Linux").FontSize(t.FontSize * 0.85).TextColor(faint)
			})
		})
}

func (a *app) navItem(c *ui.Context, p page) ui.Element {
	t := c.Theme()

	a.mu.Lock()
	selected := a.page == p
	var badge int
	if p == pageConflicts {
		badge = a.attentionCount()
	}
	a.mu.Unlock()

	// Mirrors components::nav_item: px 3 (12), py 2 (8), radius 8, text_sm.
	el := ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Padding(t.Space(2), t.Space(3)).Radius(8).
		FontSize(t.FontSize).
		TextColor(func() ui.Color {
			if selected {
				return t.Text
			}
			return t.TextMuted
		}())
	if selected {
		el = el.Background(cardHover)
	}
	if el.Clicked() {
		a.setPage(p)
	}
	return el.Children(func() {
		ui.Text(c, p.title()).FontSize(t.FontSize)
		ui.Spacer(c)
		if badge > 0 {
			pill(c, strconv.Itoa(badge), warn, warnSoft)
		}
	})
}

func (a *app) renderHeader(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	pageTitle := a.page.title()
	pageSubtitle := a.page.subtitle()
	a.mu.Unlock()

	var statusLabel string
	var statusColor ui.Color
	a.mu.Lock()
	switch a.engineState() {
	case "Running":
		statusLabel, statusColor = "Sync is running", ok
	case "Paused":
		statusLabel, statusColor = "Sync is paused", warn
	case "GuardPaused":
		statusLabel, statusColor = "Paused by delete guard", danger
	default:
		statusLabel, statusColor = "Waiting for engine", faint
	}
	a.mu.Unlock()

	// Mirrors render_header: px 28, py 4 (16), bottom border.
	ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Padding(16, pagePadding).
		BorderWidth(0, 0, 1, 0).BorderColor(border).FillWidth().Children(func() {
		ui.Column(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, pageTitle).FontSize(t.FontSize * 1.5).Bold()
			ui.Text(c, pageSubtitle).FontSize(t.FontSize).TextColor(t.TextMuted)
		})
		ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Children(func() {
			statusDot(c, statusLabel, statusColor)
			if ui.Button(c, "Open folder").Background(cardHover).Border(1, borderStrong).TextColor(t.Text).Clicked() {
				go a.openSyncFolder()
			}
		})
	})
}

func (a *app) renderStatusBar(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	root := a.root
	var counts string
	if a.status != nil {
		counts = fmt.Sprintf("%d synced · %d pending · %d deletes queued", a.status.SyncedNodes, a.status.DirtyNodes, a.status.QueuedTombstones)
	} else {
		counts = "Engine starting…"
	}
	a.mu.Unlock()

	// Mirrors render_status_bar: px 28, py 2 (8), top border, text_xs faint.
	ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Padding(t.Space(2), pagePadding).
		BorderWidth(1, 0, 0, 0).BorderColor(border).FillWidth().Children(func() {
		ui.Row(c).Gap(t.Space(2)).AlignItems(ui.Center).Children(func() {
			ui.Text(c, "Sync folder").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
			ui.Text(c, root).FontSize(t.FontSize * 0.85).TextColor(faint).SingleLine()
		})
		ui.Text(c, counts).FontSize(t.FontSize * 0.85).TextColor(faint)
	})
}

func (a *app) renderPage(c *ui.Context) {
	a.mu.Lock()
	p := a.page
	a.mu.Unlock()

	switch p {
	case pageOverview:
		a.renderOverview(c)
	case pageActivity:
		a.renderActivity(c)
	case pageConflicts:
		a.renderConflicts(c)
	case pageSettings:
		a.renderSettings(c)
	}
}

// ---------------------------------------------------------------------------
// UI: Overview
// ---------------------------------------------------------------------------
