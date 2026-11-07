package main

import (
	"fmt"

	"github.com/egoist/mygo/ui"
)

func (a *app) renderSettings(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	account := a.accountLabel()
	currentMode := a.currentMode()
	var cacheCap *uint64
	var cacheUsed uint64
	if a.status != nil {
		cacheCap = a.status.CacheCap
		cacheUsed = a.status.CacheUsed
	}
	switching := a.modeSwitch == modeSwitchInProgress
	root := a.root
	autostart := a.autostartEnabled
	autostartErr := a.autostartError
	mountErr := a.mountError
	a.mu.Unlock()

	// Mirrors settings::render: p 28, gap 4 (16), scrollable, with the
	// "Preferences" heading and cards in the GPUI order. The scroll
	// position resets on page entry (setPage), not here — setting it
	// every frame would fight the user's mouse wheel.
	ui.Scroll(c).FillWidth().Grow(1).MinHeight(0).Padding(pagePadding).TrackScroll(&a.settingsScroll).Children(func() {
		ui.Column(c).FillWidth().Gap(t.Space(4)).Children(func() {
			ui.Text(c, "Preferences").FontSize(t.FontSize).Bold()

			if switching {
				banner(c, "Switching sync mode…", "The sync engine is restarting with your new setting. This notice clears when the new mode is active.", accent, inset)
			}
			if mountErr != "" {
				banner(c, "On-demand view unavailable this session",
					fmt.Sprintf("The placeholder filesystem could not be mounted (%s). Files are being materialized in the sync folder instead; the on-demand view returns next time the app starts.", mountErr),
					warn, warnSoft)
			}

			// Account card: field row + sign-out row.
			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Account", "The Proton account this sync root belongs to")
					fieldRow(c, "Signed in as", account)
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Children(func() {
						ui.Text(c, "Signing out stops sync and deletes the saved session.").
							FontSize(t.FontSize * 0.85).TextColor(t.TextMuted).Grow(1)
						if ui.Button(c, "Sign out…").Background(cardHover).Border(1, borderStrong).TextColor(t.Text).Radius(6).Clicked() {
							a.openSignOut()
						}
					})
				})
			})

			// Sync mode card, with the cache block inside it (as in GPUI).
			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(4)).Children(func() {
					sectionTitle(c, "Sync mode", "How files in the sync folder are stored locally")
					for _, m := range []syncMode{modePlaceholders, modeMirror} {
						title, detail := func() (string, string) {
							if m == modePlaceholders {
								return "On demand (recommended)",
									"Files appear as placeholders at ~0 bytes. Opening one downloads it on the spot; the local cache is capped and cleaned up automatically. Nothing else is stored on disk."
							}
							return "Mirror everything",
								"Every file on Drive is downloaded into the sync folder and kept in full — offline by default, uses the same space as your Drive."
						}()
						selected := currentMode == m
						el := ui.Box(c).FillWidth().Padding(12).Radius(8).Border(1, func() ui.Color {
							if selected {
								return accent
							}
							return t.Border
						}()).Background(func() ui.Color {
							if selected {
								return inset
							}
							return t.Surface
						}())
						if el.Clicked() && !selected {
							a.openModeSwitch(m)
						}
						el.Children(func() {
							ui.Column(c).FillWidth().Gap(t.Space(1)).Children(func() {
								ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
									ui.Text(c, title).FontSize(t.FontSize)
									if selected {
										pill(c, "Active", ok, okSoft)
									}
								})
								ui.Text(c, detail).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
							})
						})
					}
					ui.Text(c, "Switching modes keeps every file and never deletes from Drive.").FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)

					if currentMode == modePlaceholders {
						ui.Column(c).Gap(t.Space(2)).Children(func() {
							ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
								ui.Text(c, "Local cache").FontSize(t.FontSize)
								var capText string
								if cacheCap == nil {
									capText = fmt.Sprintf("%s used / unlimited", formatBytes(cacheUsed))
								} else {
									capText = fmt.Sprintf("%s used / %s max", formatBytes(cacheUsed), formatBytes(*cacheCap))
								}
								ui.Text(c, capText).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
							})
							ui.Row(c).Gap(t.Space(2)).Wrap().Children(func() {
								presets := []struct {
									cap   *uint64
									label string
								}{
									{ptr(uint64(2 << 30)), "2 GB"},
									{ptr(uint64(5 << 30)), "5 GB"},
									{ptr(uint64(10 << 30)), "10 GB"},
									{ptr(uint64(25 << 30)), "25 GB"},
									{nil, "Unlimited"},
								}
								for _, p := range presets {
									active := (cacheCap == nil && p.cap == nil) || (cacheCap != nil && p.cap != nil && *cacheCap == *p.cap)
									chipButton(c, p.label, active, func(cap *uint64) func() {
										return func() { go a.setCacheCap(cap) }
									}(p.cap))
								}
							})
							ui.Text(c, "Files you open are stored locally up to this size; the least-used are cleaned automatically. Pinned files are never cleaned.").
								FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
						})
					}
				})
			})

			// Sync folder card: inset path row with Open folder button.
			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					folderSub := "Everything in this folder is mirrored to Proton Drive"
					if currentMode == modePlaceholders {
						folderSub = "A placeholder view of your Drive; files hydrate when opened"
					}
					sectionTitle(c, "Sync folder", folderSub)
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Padding(12).Radius(8).Background(inset).Children(func() {
						ui.Text(c, root).FontSize(t.FontSize).TextColor(t.Text).Grow(1).SingleLine()
						if ui.Button(c, "Open folder").Background(cardHover).Border(1, borderStrong).TextColor(t.Text).Radius(6).Clicked() {
							go a.openSyncFolder()
						}
					})
				})
			})

			// Sync engine card: status row + wide pause button.
			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Sync engine", "Pause at any time; changes are buffered and applied on resume")
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
						ui.Text(c, "Status").FontSize(t.FontSize).TextColor(t.TextMuted)
						dot, fg, bgc := a.badgeColors()
						pill(c, a.badge(), fg, bgc)
						_ = dot
					})
					pauseColor := accent
					switch a.engineState() {
					case "GuardPaused":
						pauseColor = warn
					case "Paused":
						pauseColor = warn
					}
					if ui.Button(c, a.pauseLabel()).Background(pauseColor).TextColor(ui.RGB(0xff, 0xff, 0xff)).FillWidth().Radius(6).Clicked() {
						go a.togglePause()
					}
				})
			})

			// Startup card.
			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "Startup", "Keep syncing in the background without opening the dashboard")
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Gap(t.Space(4)).Children(func() {
						ui.Column(c).Gap(t.Space(1)).Grow(1).Children(func() {
							ui.Text(c, "Start at login").FontSize(t.FontSize)
							ui.Text(c, "Launches minimized to the tray when you sign in, so files keep syncing. Close the dashboard at any time to keep the app running in the background.").
								FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
						})
						switchEl := ui.Switch(c, &autostart)
						if switchEl.Changed() {
							go a.setAutostart(autostart)
						}
					})
					if autostartErr != "" {
						ui.Text(c, fmt.Sprintf("Could not update the login entry: %s", autostartErr)).FontSize(t.FontSize * 0.85).TextColor(danger)
					}
				})
			})

			// About card: field rows, disclosure inset, Quit text button.
			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					sectionTitle(c, "About", "A third-party client built on Proton's official Drive SDK")
					fieldRow(c, "Application", fmt.Sprintf("%s %s", appTitle, appVersion()))
					fieldRow(c, "Platform", "Linux")
					ui.Box(c).Background(inset).Padding(12).Radius(8).Children(func() {
						ui.Text(c, disclosure).FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
					})
					ui.Row(c).Justify(ui.End).FillWidth().Children(func() {
						if ui.Button(c, "Quit application").Background(bg).TextColor(muted).Radius(6).Clicked() {
							go a.quit()
						}
					})
				})
			})
		})
	})
}

func ptr[T any](v T) *T { return &v }

// ---------------------------------------------------------------------------
// UI: modals
