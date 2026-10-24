//! Status window: header (account, engine state, counts, big pause/resume
//! toggle), virtualized activity feed, conflicts/errors inbox, sync-root
//! footer (SPEC.md §6, gpui.md §2.2 dashboard shape).

use gpui::{
    Context, Div, Entity, ScrollHandle, Stateful, Subscription, Window, div, prelude::*, px, rgb,
    uniform_list,
};
use pds_core::EngineState;

use crate::app_model::AppModel;
use crate::model::{self, ModelState, Phase, SignOutState, UiCommand, format_time, kind_label};
use crate::widgets::{self, ACCENT, BG, BORDER, DANGER, DIM, OK, PANEL, TEXT, WARN};

pub struct StatusView {
    model: Entity<AppModel>,
    inbox_scroll: ScrollHandle,
    _sub: Subscription,
}

impl StatusView {
    pub fn new(model: Entity<AppModel>, cx: &mut Context<Self>) -> Self {
        let sub = cx.observe(&model, |_this, _model, cx| cx.notify());
        Self {
            model,
            inbox_scroll: ScrollHandle::new(),
            _sub: sub,
        }
    }

    fn toggle_pause(&mut self, cx: &mut Context<Self>) {
        let cmd = model::toggle_command(&self.model.read(cx).state);
        self.model.update(cx, |m, _| m.send(cmd));
    }

    fn header(&self, state: &ModelState, cx: &mut Context<Self>) -> Div {
        let badge_text = model::badge(state);
        let badge_color = match state.engine_state() {
            Some(EngineState::Running) if state.phase == Phase::Running => OK,
            Some(EngineState::GuardPaused) => DANGER,
            Some(EngineState::Paused) => WARN,
            _ => DIM,
        };
        let email = state
            .email
            .clone()
            .unwrap_or_else(|| "unknown account".into());

        let mut counts = div().flex().gap_3().text_sm().text_color(rgb(DIM));
        if let Some(st) = &state.status {
            counts = counts
                .child(format!("{} synced", st.synced_nodes))
                .child(format!("{} pending", st.dirty_nodes))
                .child(format!("{} deletes queued", st.queued_tombstones))
                .child(format!("{} conflicts", st.conflicted_nodes))
                .child(format!("{} errors", st.errored_nodes));
        } else {
            counts = counts.child("waiting for engine status…");
        }

        let paused = state.engine_state() != Some(EngineState::Running);
        let toggle_color = if paused { OK } else { ACCENT };

        let mut header = div()
            .flex()
            .flex_col()
            .gap_3()
            .px_4()
            .py_3()
            .border_b_1()
            .border_color(rgb(BORDER))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(div().text_base().child(email))
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .rounded_md()
                            .bg(rgb(PANEL))
                            .text_sm()
                            .text_color(rgb(badge_color))
                            .child(badge_text),
                    ),
            )
            .child(counts);

        if state.engine_state() == Some(EngineState::GuardPaused) {
            header = header.child(
                div()
                    .p_3()
                    .rounded_md()
                    .border_1()
                    .border_color(rgb(DANGER))
                    .text_sm()
                    .child(
                        "Too many deletes in a short window — syncing paused to protect your \
                         Drive. Review the deletions, then resume to approve them.",
                    ),
            );
        }

        if state.phase == Phase::Failed {
            header = header.child(
                div()
                    .p_3()
                    .rounded_md()
                    .border_1()
                    .border_color(rgb(DANGER))
                    .text_sm()
                    .text_color(rgb(DANGER))
                    .child(
                        state
                            .fatal
                            .clone()
                            .unwrap_or_else(|| "sync engine stopped".into()),
                    ),
            );
        } else {
            header = header.child(widgets::wide_button(
                "pause-toggle",
                model::pause_label(state),
                toggle_color,
                cx,
                |this, _, _, cx| this.toggle_pause(cx),
            ));
        }

        header
    }

    fn inbox(&self, state: &ModelState) -> Stateful<Div> {
        let mut panel = div()
            .id("inbox")
            .w(px(260.0))
            .h_full()
            .flex()
            .flex_col()
            .gap_2()
            .p_3()
            .border_l_1()
            .border_color(rgb(BORDER))
            .overflow_y_scroll()
            .track_scroll(&self.inbox_scroll);

        panel = panel.child(div().text_sm().text_color(rgb(DIM)).child("Conflicts"));
        if state.conflicts.is_empty() {
            panel = panel.child(
                div()
                    .text_sm()
                    .text_color(rgb(DIM))
                    .child("No conflicts — everything is in sync."),
            );
        } else {
            for path in &state.conflicts {
                panel = panel.child(
                    div()
                        .p_2()
                        .rounded_md()
                        .bg(rgb(PANEL))
                        .text_sm()
                        .child(path.clone())
                        .child(
                            div()
                                .text_color(rgb(DIM))
                                .child("remote changed during upload; heals on next remote event"),
                        ),
                );
            }
        }

        panel = panel.child(
            div()
                .mt_2()
                .text_sm()
                .text_color(rgb(DIM))
                .child("Failed uploads"),
        );
        if state.errored.is_empty() {
            panel = panel.child(div().text_sm().text_color(rgb(DIM)).child("None."));
        } else {
            for (path, error) in &state.errored {
                panel = panel.child(
                    div()
                        .p_2()
                        .rounded_md()
                        .bg(rgb(PANEL))
                        .text_sm()
                        .child(path.clone())
                        .child(div().text_color(rgb(DANGER)).child(error.clone())),
                );
            }
        }

        panel
    }

    /// Centered sign-out confirmation (Confirming) / progress (InProgress)
    /// panel, replacing the activity+inbox body while active.
    fn confirm_panel(&self, state: &ModelState, cx: &mut Context<Self>) -> Div {
        let email = state.email.clone().unwrap_or_else(|| "this account".into());
        let mut panel = div()
            .flex()
            .flex_col()
            .gap_3()
            .p_4()
            .rounded_md()
            .border_1()
            .border_color(rgb(BORDER))
            .bg(rgb(PANEL))
            .child(div().text_base().child(format!("Sign out of {email}?")))
            .child(div().text_sm().text_color(rgb(DIM)).child(
                "Sync will stop and the saved session will be deleted. Files on disk are kept.",
            ));
        if state.sign_out == SignOutState::InProgress {
            panel = panel.child(widgets::dim("Signing out…"));
        } else {
            panel = panel.child(
                div()
                    .flex()
                    .gap_2()
                    .child(widgets::danger_button(
                        "confirm-sign-out",
                        "Sign Out",
                        cx,
                        |this, _, _, cx| {
                            this.model.update(cx, |m, cx| {
                                m.state.confirm_sign_out();
                                m.send(UiCommand::SignOut);
                                cx.notify();
                            });
                        },
                    ))
                    .child(widgets::ghost_button(
                        "cancel-sign-out",
                        "Cancel",
                        cx,
                        |this, _, _, cx| {
                            this.model.update(cx, |m, cx| {
                                m.state.cancel_sign_out();
                                cx.notify();
                            });
                        },
                    )),
            );
        }
        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .child(panel)
    }
}

impl Render for StatusView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.model.read(cx).state.clone();

        let activity_count = state.activity.len();
        let activity = uniform_list(
            "activity",
            activity_count,
            cx.processor(|this, range: std::ops::Range<usize>, _window, cx| {
                let state = this.model.read(cx).state.clone();
                range
                    .map(|ix| {
                        let item = &state.activity[ix];
                        let mut row = div()
                            .id(ix)
                            .flex()
                            .gap_2()
                            .px_3()
                            .py_1()
                            .text_sm()
                            .whitespace_nowrap()
                            .child(div().text_color(rgb(DIM)).child(format_time(item.at_unix)))
                            .child(kind_label(item.kind))
                            .child(item.path.clone());
                        if let Some(detail) = &item.detail {
                            row = row.child(div().text_color(rgb(DIM)).child(detail.clone()));
                        }
                        row
                    })
                    .collect::<Vec<_>>()
            }),
        )
        .flex_1();

        let mut activity_col = div().flex_1().flex().flex_col().h_full().child(
            div()
                .px_3()
                .py_2()
                .text_sm()
                .text_color(rgb(DIM))
                .border_b_1()
                .border_color(rgb(BORDER))
                .child("Activity"),
        );
        activity_col =
            if activity_count == 0 {
                activity_col.child(
                    div().p_3().text_sm().text_color(rgb(DIM)).child(
                        "Nothing yet — uploads, downloads, trashes and conflicts show up here.",
                    ),
                )
            } else {
                activity_col.child(activity)
            };

        let body = if state.sign_out == SignOutState::None {
            div()
                .flex_1()
                .flex()
                .flex_row()
                .min_h(px(0.0))
                .child(activity_col)
                .child(self.inbox(&state))
        } else {
            self.confirm_panel(&state, cx)
        };

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(rgb(BG))
            .text_color(rgb(TEXT))
            .text_sm()
            .child(self.header(&state, cx))
            .child(body)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_3()
                    .px_4()
                    .py_2()
                    .border_t_1()
                    .border_color(rgb(BORDER))
                    .text_color(rgb(DIM))
                    .child(
                        div()
                            .whitespace_nowrap()
                            .child(format!("Sync folder: {}", state.root.display())),
                    )
                    .child(widgets::text_button(
                        "sign-out",
                        "Sign out…",
                        cx,
                        |this, _, _, cx| {
                            this.model.update(cx, |m, cx| {
                                m.state.begin_sign_out();
                                cx.notify();
                            });
                        },
                    )),
            )
    }
}
