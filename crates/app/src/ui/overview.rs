//! Overview page: headline state, counters, recent activity, attention items.

use gpui::{AnyElement, Context, FontWeight, div, prelude::*, px, rgb};
use pds_core::EngineState;

use crate::components;
use crate::state::{self, Page, Phase};
use crate::theme;
use crate::ui::{AppView, activity_row};

pub fn render(view: &mut AppView, cx: &mut Context<AppView>) -> AnyElement {
    let state = &view.state;
    let status = state.status.clone();
    let dirty = status.as_ref().map(|s| s.dirty_nodes).unwrap_or(0);
    let headline = match (state.phase, state.engine_state()) {
        (Phase::Running, Some(EngineState::GuardPaused)) => {
            "Syncing paused by the delete guard".to_string()
        }
        (Phase::Running, Some(EngineState::Paused)) => "Sync is paused".to_string(),
        (Phase::Running, _) if dirty > 0 => {
            format!("Syncing {dirty} pending {}", if dirty == 1 { "item" } else { "items" })
        }
        (Phase::Running, _) => "Everything is up to date".to_string(),
        _ => "Waiting for the sync engine".to_string(),
    };

    let (badge_text, badge_color, badge_bg) = match state.engine_state() {
        Some(EngineState::Running) if state.phase == Phase::Running => {
            (state::badge(state), theme::OK, theme::OK_SOFT)
        }
        Some(EngineState::Paused) => (state::badge(state), theme::WARN, theme::WARN_SOFT),
        Some(EngineState::GuardPaused) => (state::badge(state), theme::DANGER, theme::DANGER_SOFT),
        Some(EngineState::Running) => (state::badge(state), theme::FAINT, theme::INSET),
        None => (state::badge(state), theme::FAINT, theme::INSET),
    };

    let mut content = div()
        .id("overview-scroll")
        .flex()
        .flex_col()
        .gap_5()
        .flex_1()
        .min_h(px(0.0))
        .overflow_y_scroll()
        .p(px(theme::PAGE_PADDING));

    if state.engine_state() == Some(EngineState::GuardPaused) {
        content = content.child(components::banner(
            "Too many deletes in a short window",
            "Syncing is paused to protect your Drive. Review the pending deletions, then resume \
             to approve them.",
            theme::DANGER,
            theme::DANGER_SOFT,
        ));
    }

    let mut status_card = components::card()
        .child(
            div()
                .flex()
                .items_start()
                .justify_between()
                .gap_4()
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_2xl()
                                .font_weight(FontWeight::SEMIBOLD)
                                .child(headline),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(rgb(theme::MUTED))
                                .child(state.account_label()),
                        ),
                )
                .child(components::pill(badge_text, badge_color, badge_bg)),
        );

    if let Some(status) = status {
        status_card = status_card.child(
            div()
                .flex()
                .gap_3()
                .child(components::stat(
                    "Synced items",
                    status.synced_nodes.to_string(),
                    theme::OK,
                ))
                .child(components::stat(
                    "Pending uploads",
                    status.dirty_nodes.to_string(),
                    if status.dirty_nodes > 0 {
                        theme::ACCENT
                    } else {
                        theme::TEXT
                    },
                ))
                .child(components::stat(
                    "Deletes queued",
                    status.queued_tombstones.to_string(),
                    if status.queued_tombstones > 0 {
                        theme::WARN
                    } else {
                        theme::TEXT
                    },
                ))
                .child(components::stat(
                    "Needs attention",
                    (status.conflicted_nodes + status.errored_nodes).to_string(),
                    if status.conflicted_nodes + status.errored_nodes > 0 {
                        theme::DANGER
                    } else {
                        theme::TEXT
                    },
                )),
        );
    } else {
        status_card = status_card.child(
            div()
                .text_sm()
                .text_color(rgb(theme::MUTED))
                .child("Collecting the first status snapshot…"),
        );
    }

    content = content.child(status_card);

    if state.attention_count() > 0 {
        content = content.child(
            components::card()
                .child(
                    div()
                        .flex()
                        .items_center()
                        .justify_between()
                        .child(components::section_title(
                            "Needs attention",
                            Some("Conflicts and failed uploads"),
                        ))
                        .child(components::text_button(
                            "overview-conflicts",
                            "Review",
                            cx,
                            |this, _, _, cx| this.set_page(Page::Conflicts, cx),
                        )),
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(theme::MUTED))
                        .child(format!(
                            "{} conflict{} · {} failed upload{}",
                            state.conflicts.len(),
                            plural(state.conflicts.len()),
                            state.errored.len(),
                            plural(state.errored.len()),
                        )),
                ),
        );
    }

    let recent: Vec<_> = state.recent_activity(7).to_vec();
    let mut activity_card = components::card().child(
        div()
            .flex()
            .items_center()
            .justify_between()
            .child(components::section_title(
                "Recent activity",
                Some("Newest first"),
            ))
            .child(components::text_button(
                "overview-activity",
                "View all",
                cx,
                |this, _, _, cx| this.set_page(Page::Activity, cx),
            )),
    );
    if recent.is_empty() {
        activity_card = activity_card.child(components::empty_state(
            "No activity yet",
            "Uploads, downloads, deletions, and conflicts will appear here.",
        ));
    } else {
        for item in &recent {
            activity_card = activity_card.child(activity_row(item));
        }
    }
    content = content.child(activity_card);

    content.into_any_element()
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}
