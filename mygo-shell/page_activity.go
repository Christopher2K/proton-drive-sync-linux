package main

import (
	"fmt"

	"github.com/egoist/mygo/ui"
)

func (a *app) renderActivity(c *ui.Context) {
	t := c.Theme()

	a.mu.Lock()
	f := a.filter
	a.mu.Unlock()

	rows := a.filteredActivity(f)

	// Mirrors activity::render: p 28, gap 4 (16).
	ui.Box(c).FillWidth().Grow(1).MinHeight(0).Padding(pagePadding).Children(func() {
		ui.Column(c).Grow(1).MinHeight(0).FillWidth().Gap(t.Space(4)).Children(func() {
			// Filter pills: px 3 (12), py 1 (4), radius full, text_xs, gap 2.
			ui.Row(c).Gap(t.Space(2)).Wrap().AlignItems(ui.Center).Children(func() {
				for _, candidate := range allFilters {
					active := candidate == f
					count := a.activityCount(candidate)
					el := ui.Button(c, fmt.Sprintf("%s %d", candidate.label(), count)).
						FontSize(t.FontSize*0.85).Padding(t.Space(1), t.Space(3)).Radius(999)
					if active {
						el = el.Background(accent).TextColor(t.Text)
					} else {
						el = el.Background(card).TextColor(t.TextMuted)
					}
					if el.Clicked() {
						a.setFilter(candidate)
					}
				}
			})

			// List card: radius 12, header row with bottom border, then
			// the table header and the (scrollable) rows.
			ui.Box(c).FillWidth().Grow(1).MinHeight(0).Background(card).Border(1, border).Radius(12).Children(func() {
				ui.Column(c).Grow(1).MinHeight(0).Children(func() {
					ui.Row(c).AlignItems(ui.Center).Justify(ui.SpaceBetween).Padding(t.Space(3), 16).
						BorderWidth(0, 0, 1, 0).BorderColor(border).Children(func() {
						ui.Text(c, fmt.Sprintf("%d %s", len(rows), pluralize(len(rows), "event", "events"))).FontSize(t.FontSize)
						if ui.Button(c, "Settings").Background(cardHover).TextColor(muted).Radius(6).Clicked() {
							a.setPage(pageSettings)
						}
					})
					if len(rows) == 0 {
						emptyState(c, "Nothing here yet", "Sync activity will show up as files move in either direction.")
					} else {
						// Table header: px 4 (16), py 2 (8), text_xs faint.
						ui.Row(c).Gap(t.Space(3)).Padding(t.Space(2), 16).
							BorderWidth(0, 0, 1, 0).BorderColor(border).Children(func() {
							ui.Text(c, "Time").FontSize(t.FontSize * 0.85).TextColor(faint).Width(64)
							ui.Text(c, "Event").FontSize(t.FontSize * 0.85).TextColor(faint).Width(92)
							ui.Text(c, "Path").FontSize(t.FontSize * 0.85).TextColor(faint).Grow(1)
						})
						// Rows: 42 DIP tall each, like the uniform_list rows.
						ui.Scroll(c).Grow(1).MinHeight(0).TrackScroll(&a.activityScroll).Children(func() {
							ui.Column(c).FillWidth().Children(func() {
								for _, it := range rows {
									ui.Box(c).FillWidth().Height(42).Children(func() {
										a.activityRow(c, it)
									})
								}
							})
						})
					}
				})
			})
		})
	})
}

// activityRow mirrors ui::activity_row: py 2 (8), bottom border, pill for
// the event kind.
func (a *app) activityRow(c *ui.Context, it activityItem) ui.Element {
	t := c.Theme()
	return ui.Column(c).Children(func() {
		ui.Row(c).Gap(t.Space(3)).AlignItems(ui.Center).Padding(t.Space(2), 0).
			BorderWidth(0, 0, 1, 0).BorderColor(t.Border).Children(func() {
			ui.Text(c, formatTime(it.AtUnix)).FontSize(t.FontSize * 0.85).TextColor(faint).Width(64)
			pill(c, kindLabel(it.Kind), kindColor(it.Kind), inset)
			ui.Text(c, it.Path).FontSize(t.FontSize).TextColor(t.Text).Grow(1).SingleLine()
		})
	})
}

func pluralize(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// ---------------------------------------------------------------------------
// UI: Conflicts
// ---------------------------------------------------------------------------
