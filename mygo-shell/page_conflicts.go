package main

import "github.com/egoist/mygo/ui"

func (a *app) renderConflicts(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	conflicts := append([]string(nil), a.conflicts...)
	errored := append([]erroredItem(nil), a.errored...)
	a.mu.Unlock()

	// Mirrors conflicts::render: p 28, gap 5 (20), scrollable.
	ui.Scroll(c).FillWidth().Grow(1).MinHeight(0).Padding(pagePadding).Children(func() {
		ui.Column(c).FillWidth().Gap(t.Space(5)).Children(func() {
			if len(conflicts) == 0 && len(errored) == 0 {
				pageCard(c, func() {
					emptyState(c, "Everything is in sync", "No conflicts or failed uploads. Keep-both copies and upload errors will show up here.")
				})
			}

			if len(conflicts) > 0 {
				pageCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						sectionTitle(c, "Conflicts", "The remote copy changed while a local edit was pending")
						for _, path := range conflicts {
							// Inset box: p 3 (12), radius 8, gap 1 (4).
							ui.Box(c).Background(inset).Padding(12).Radius(8).Children(func() {
								ui.Column(c).Gap(t.Space(1)).Children(func() {
									ui.Text(c, path).FontSize(t.FontSize).SingleLine()
									ui.Text(c, "Kept both versions — the local copy will be renamed with a conflict suffix and uploaded separately.").
										FontSize(t.FontSize * 0.85).TextColor(t.TextMuted)
								})
							})
						}
					})
				})
			}

			if len(errored) > 0 {
				pageCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						sectionTitle(c, "Failed uploads", "These files stay local until the next successful retry")
						for _, it := range errored {
							msg := it.Error
							if msg == "" {
								msg = "Upload failed without a reported reason."
							}
							ui.Box(c).Background(inset).Padding(12).Radius(8).Children(func() {
								ui.Column(c).Gap(t.Space(1)).Children(func() {
									ui.Text(c, it.Path).FontSize(t.FontSize).SingleLine()
									ui.Text(c, msg).FontSize(t.FontSize * 0.85).TextColor(danger)
								})
							})
						}
					})
				})
			}
		})
	})
}

// ---------------------------------------------------------------------------
// UI: Settings
// ---------------------------------------------------------------------------
