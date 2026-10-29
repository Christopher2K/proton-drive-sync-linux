//! Settings page: account, local folder, engine controls, about.

use gpui::{AnyElement, Context, FontWeight, div, prelude::*, px, rgb};

use crate::components;
use crate::state::{self, APP_TITLE, DISCLOSURE, Phase};
use crate::theme;
use crate::ui::AppView;

pub fn render(view: &mut AppView, cx: &mut Context<AppView>) -> AnyElement {
    let state = &view.state;
    let account = state.account_label();
    let root = state.root.display().to_string();
    let status = state.status.clone();
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

    let folder_card = components::card()
        .child(components::section_title(
            "Sync folder",
            Some("Everything in this folder is mirrored to Proton Drive"),
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

    div()
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
        )
        .child(account_card)
        .child(folder_card)
        .child(engine_card)
        .child(about_card)
        .into_any_element()
}
