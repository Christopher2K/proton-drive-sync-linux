package main

import (
	"fmt"

	"github.com/egoist/mygo/ui"
)

func modalOverlay(c *ui.Context, content func()) {
	ui.Box(c).Absolute().Top(0).Left(0).Right(0).Bottom(0).Background(ui.RGBA(0x05, 0x06, 0x0a, 0.65)).
		AlignItems(ui.Center).Justify(ui.Center).Children(content)
}

func (a *app) renderSignOutModal(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	account := a.accountLabel()
	inProgress := a.signOut == signOutInProgress
	a.mu.Unlock()

	modalOverlay(c, func() {
		appCard(c, func() {
			ui.Column(c).Gap(t.Space(4)).Width(430).Children(func() {
				ui.Text(c, fmt.Sprintf("Sign out of %s?", account)).FontSize(t.FontSize * 1.25).Bold()
				ui.Text(c, "Sync will stop and the saved session will be deleted. Files on disk are kept, and you can sign back in at any time.").
					FontSize(t.FontSize).TextColor(t.TextMuted)
				if inProgress {
					statusDot(c, "Signing out…", accent)
				} else {
					ui.Row(c).Gap(t.Space(3)).Children(func() {
						if ui.Button(c, "Sign out").Background(danger).TextColor(ui.RGB(0xff, 0xff, 0xff)).Clicked() {
							go a.confirmSignOut()
						}
						if ui.Button(c, "Cancel").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							a.cancelSignOut()
						}
					})
				}
			})
		})
	})
}

func (a *app) renderModeSwitchModal(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	ms := a.modeSwitch
	to := a.modeSwitchTo
	a.mu.Unlock()

	var title, body string
	switch {
	case ms == modeSwitchInProgress:
		title = "Switching sync mode…"
	case to == modeMirror:
		title = "Download everything to this folder?"
		body = "Switching to mirror materializes every file in your sync folder. Files already on disk are kept; the rest download from Drive once. Your Drive is never deleted."
	case to == modePlaceholders:
		title = "Switch to on-demand placeholders?"
		body = "Files in your sync folder become placeholders at ~0 bytes. Content stays on Drive; opening a file fetches it on demand and the local cache is capped (LRU eviction). Nothing is deleted from Drive."
	}

	modalOverlay(c, func() {
		appCard(c, func() {
			ui.Column(c).Gap(t.Space(4)).Width(460).Children(func() {
				ui.Text(c, title).FontSize(t.FontSize * 1.25).Bold()
				if body != "" {
					ui.Text(c, body).FontSize(t.FontSize).TextColor(t.TextMuted)
				}
				if ms == modeSwitchInProgress {
					statusDot(c, "Switching…", accent)
				} else {
					ui.Row(c).Gap(t.Space(3)).Children(func() {
						if ui.Button(c, "Switch mode").Background(accent).TextColor(ui.RGB(0xff, 0xff, 0xff)).Clicked() {
							go a.confirmModeSwitch()
						}
						if ui.Button(c, "Cancel").Background(t.SurfaceHover).TextColor(t.Text).Clicked() {
							a.cancelModeSwitch()
						}
					})
				}
			})
		})
	})
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------
