//! Settings page: account, sync mode, local folder, engine controls, about.

use gpui::{AnyElement, Context, FontWeight, div, prelude::*, px, rgb};
use pds_core::SyncMode;

use crate::components;
use crate::state::{self, APP_TITLE, DISCLOSURE, ModeSwitchState, Phase};
use crate::theme;
use crate::ui::AppView;

pub fn render(view: &mut AppView, cx: &mut Context<AppView>) -> AnyElement {
    let state = &view.state;
    let account = state.account_label();
    let root = state.root.display().to_string();
    let status = state.status.clone();
    let current_mode = status.as_ref().map(|s| s.mode);
    let badge_color = match state.engine_state() {
        Some(pds_core::EngineState::Running) if state.phase == Phase::Running => theme::OK,
        Some(pds_core::EngineState::Paused) => theme::WARN,
        Some(pds_core::EngineState::GuardPaused) => theme::DANGER,
        _ => theme::FAINT,
    };

    let account_card = components::card()
        .child(components::section_title(
            "Account",
            Some("The Proton account this sync root belongs to"),
        ))
        .child(components::field_row("Signed in as", account))
        .child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap_4()
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(theme::MUTED))
                        .child("Signing out stops sync and deletes the saved session."),
                )
                .child(components::secondary_button(
                    "settings-sign-out",
                    "Sign out…",
                    cx,
                    |this, _, _, cx| this.request_sign_out(cx),
                )),
        );

    // Sync mode (F5): placeholders (default) vs full mirror.
    let mut mode_card = components::card().child(components::section_title(
        "Sync mode",
        Some("How files in the sync folder are stored locally"),
    ));
    for mode in [SyncMode::Placeholders, SyncMode::Mirror] {
        let (title, detail) = match mode {
            SyncMode::Placeholders => (
                "On demand (recommended)",
                "Files appear as placeholders at ~0 bytes. Opening one downloads it on the \
                 spot; the local cache is capped and cleaned up automatically. Nothing else \
                 is stored on disk.",
            ),
            SyncMode::Mirror => (
                "Mirror everything",
                "Every file on Drive is downloaded into the sync folder and kept in full — \
                 offline by default, uses the same space as your Drive.",
            ),
        };
        let selected = current_mode == Some(mode);
        let mut row = div()
            .id(("sync-mode", mode as usize))
            .flex()
            .flex_col()
            .gap_1()
            .p_3()
            .rounded(px(8.0))
            .border_1()
            .cursor_pointer()
            .on_click(cx.listener(move |this, _, _, cx| {
                if this.state.status.as_ref().map(|s| s.mode) != Some(mode) {
                    this.request_mode_switch(mode, cx);
                }
            }));
        row = if selected {
            row.border_color(rgb(theme::ACCENT)).bg(rgb(theme::INSET))
        } else {
            row.border_color(rgb(theme::BORDER))
                .hover(|s| s.bg(rgb(theme::CARD_HOVER)))
        };
        row = row
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child(title),
                    )
                    .child(if selected {
                        components::pill("Active", theme::OK, theme::OK_SOFT).into_any_element()
                    } else {
                        div().into_any_element()
                    }),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(theme::MUTED))
                    .child(detail),
            );
        mode_card = mode_card.child(row);
    }
    mode_card = mode_card.child(
        div()
            .text_xs()
            .text_color(rgb(theme::MUTED))
            .child("Switching modes keeps every file and never deletes from Drive."),
    );
    let mode_card = mode_card;

    let folder_card = components::card()
        .child(components::section_title(
            "Sync folder",
            Some(
                if current_mode == Some(SyncMode::Placeholders) {
                    "A placeholder view of your Drive; files hydrate when opened"
                } else {
                    "Everything in this folder is mirrored to Proton Drive"
                },
            ),
        ))
        .child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap_4()
                .p_3()
                .rounded(px(8.0))
                .bg(rgb(theme::INSET))
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .truncate()
                        .text_sm()
                        .child(root),
                )
                .child(components::secondary_button(
                    "settings-open-folder",
                    "Open folder",
                    cx,
                    |this, _, _, _| this.open_sync_folder(),
                )),
        );

    let mut engine_card = components::card()
        .child(components::section_title(
            "Sync engine",
            Some("Pause at any time; changes are buffered and applied on resume"),
        ))
        .child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(theme::MUTED))
                        .child("Status"),
                )
                .child(components::pill(
                    state::badge(state),
                    badge_color,
                    theme::INSET,
                )),
        )
        .child(components::wide_button(
            "settings-pause",
            state::pause_label(state),
            match state.engine_state() {
                Some(pds_core::EngineState::Running) => theme::ACCENT,
                Some(pds_core::EngineState::GuardPaused) => theme::WARN,
                _ => theme::OK,
            },
            cx,
            |this, _, _, cx| this.toggle_pause(cx),
        ));

    if let Some(status) = status {
        engine_card = engine_card
            .child(components::field_row(
                "Pending uploads",
                status.dirty_nodes.to_string(),
            ))
            .child(components::field_row(
                "Deletes queued",
                status.queued_tombstones.to_string(),
            ))
            .child(components::field_row(
                "Synced items",
                status.synced_nodes.to_string(),
            ));
    }

    let about_card = components::card()
        .child(components::section_title(
            "About",
            Some("A third-party client built on Proton's official Drive SDK"),
        ))
        .child(components::field_row(
            "Application",
            format!("{APP_TITLE} {}", env!("CARGO_PKG_VERSION")),
        ))
        .child(components::field_row("Platform", "Linux"))
        .child(
            div()
                .p_3()
                .rounded(px(8.0))
                .bg(rgb(theme::INSET))
                .text_xs()
                .text_color(rgb(theme::MUTED))
                .child(DISCLOSURE),
        )
        .child(
            div()
                .flex()
                .items_center()
                .justify_end()
                .child(components::text_button(
                    "settings-quit",
                    "Quit application",
                    cx,
                    |this, _, _, cx| this.request_quit(cx),
                )),
        );

    let mut has_notices = false;
    let mut notices = div().flex().flex_col().gap_4();
    if state.layout_migrated {
        has_notices = true;
        notices = notices.child(components::banner(
            "Settings storage moved",
            "This update moved the sync engine's internal database out of your sync folder. \
             Your existing sync state was carried over automatically.",
            theme::OK,
            theme::OK_SOFT,
        ));
    }
    if let Some(error) = &state.mount_error {
        has_notices = true;
        notices = notices.child(components::banner(
            "On-demand view unavailable this session",
            format!("The placeholder filesystem could not be mounted ({error}). Files are \
                     being materialized in the sync folder instead; the on-demand view \
                     returns next time the app starts."),
            theme::WARN,
            theme::WARN_SOFT,
        ));
    }
    if matches!(state.mode_switch, ModeSwitchState::InProgress(_)) {
        has_notices = true;
        notices = notices.child(components::banner(
            "Switching sync mode…",
            "The sync engine is restarting with your new setting. This notice clears when \
             the new mode is active.",
            theme::ACCENT,
            theme::INSET,
        ));
    }

    let mut page = div()
        .id("settings-scroll")
        .flex()
        .flex_col()
        .gap_4()
        .flex_1()
        .min_h(px(0.0))
        .overflow_y_scroll()
        .p(px(theme::PAGE_PADDING))
        .child(
            div()
                .text_base()
                .font_weight(FontWeight::SEMIBOLD)
                .child("Preferences"),
        );
    if has_notices {
        page = page.child(notices);
    }
    page.child(account_card)
        .child(mode_card)
        .child(folder_card)
        .child(engine_card)
        .child(about_card)
        .into_any_element()
}
