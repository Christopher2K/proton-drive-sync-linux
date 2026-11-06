package main

import (
	"fmt"
	"strconv"

	"github.com/egoist/mygo/ui"
)

func (a *app) renderOverview(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	status := a.status
	dirty := 0
	if status != nil {
		dirty = status.DirtyNodes
	}
	headline := "Waiting for the sync engine"
	if a.phase == phaseRunning {
		switch a.engineState() {
		case "GuardPaused":
			headline = "Syncing paused by the delete guard"
		case "Paused":
			headline = "Sync is paused"
		default:
			if dirty > 0 {
				headline = fmt.Sprintf("Syncing %d pending item%s", dirty, plural(dirty))
			} else {
				headline = "Everything is up to date"
			}
		}
	}
	account := a.accountLabel()
	badgeText := a.badge()
	_, badgeFg, badgeBg := a.badgeColors()
	attention := a.attentionCount()
	recent := a.recentActivity(7)
	a.mu.Unlock()

	// Mirrors overview::render: p 28, gap 5 (20), scrollable.
	ui.Scroll(c).FillWidth().Grow(1).MinHeight(0).Padding(pagePadding).Children(func() {
		ui.Column(c).FillWidth().Gap(t.Space(5)).Children(func() {
			if a.engineState() == "GuardPaused" {
				banner(c, "Too many deletes in a short window",
					"Syncing is paused to protect your Drive. Review the pending deletions, then resume to approve them.",
					danger, dangerSoft)
			}

			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					ui.Row(c).AlignItems(ui.Start).Justify(ui.SpaceBetween).Gap(t.Space(4)).Children(func() {
						ui.Column(c).Gap(t.Space(1)).Children(func() {
							ui.Text(c, headline).FontSize(t.FontSize * 2).Bold()
							ui.Text(c, account).FontSize(t.FontSize).TextColor(t.TextMuted).SingleLine()
						})
						pill(c, badgeText, badgeFg, badgeBg)
					})

					if status != nil {
						ui.Row(c).Gap(t.Space(3)).Children(func() {
							statBox(c, "Synced items", strconv.Itoa(status.SyncedNodes), ok)
							statBox(c, "Pending uploads", strconv.Itoa(status.DirtyNodes), func() ui.Color {
								if status.DirtyNodes > 0 {
									return accent
								}
								return t.Text
							}())
							statBox(c, "Deletes queued", strconv.Itoa(status.QueuedTombstones), func() ui.Color {
								if status.QueuedTombstones > 0 {
									return warn
								}
								return t.Text
							}())
							statBox(c, "Needs attention", strconv.Itoa(status.ConflictedNodes+status.ErroredNodes), func() ui.Color {
								if status.ConflictedNodes+status.ErroredNodes > 0 {
									return danger
								}
								return t.Text
							}())
						})
					} else {
						ui.Text(c, "Collecting the first status snapshot…").FontSize(t.FontSize).TextColor(t.TextMuted)
					}
				})
			})

			if attention > 0 {
				pageCard(c, func() {
					ui.Column(c).Gap(t.Space(3)).Children(func() {
						ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
							sectionTitle(c, "Needs attention", "Conflicts and failed uploads")
							if ui.Button(c, "Review").Background(cardHover).TextColor(muted).Radius(6).Clicked() {
								a.setPage(pageConflicts)
							}
						})
						a.mu.Lock()
						conflicts := a.conflicts
						errored := a.errored
						a.mu.Unlock()
						ui.Text(c, fmt.Sprintf("%d conflict%s · %d failed upload%s",
							len(conflicts), plural(len(conflicts)), len(errored), plural(len(errored)))).
							FontSize(t.FontSize).TextColor(t.TextMuted)
					})
				})
			}

			pageCard(c, func() {
				ui.Column(c).Gap(t.Space(3)).Children(func() {
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Children(func() {
						sectionTitle(c, "Recent activity", "Newest first")
						if ui.Button(c, "View all").Background(cardHover).TextColor(muted).Radius(6).Clicked() {
							a.setPage(pageActivity)
						}
					})
					if len(recent) == 0 {
						emptyState(c, "No activity yet", "Uploads, downloads, deletions, and conflicts will appear here.")
					} else {
						for _, it := range recent {
							a.activityRow(c, it)
						}
					}
				})
			})
		})
	})
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// ---------------------------------------------------------------------------
// UI: Activity
// ---------------------------------------------------------------------------
