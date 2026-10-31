//! The single application window: login/onboarding, the main shell, and the
//! sign-out confirmation modal.

mod activity;
mod conflicts;
mod overview;
mod settings;

use std::path::PathBuf;
use std::time::Duration;

use gpui::{
    Context, Div, FocusHandle, FontWeight, KeyBinding, Task, Window, div, prelude::*,
    px, rgb, rgba,
};
use pds_core::EngineState;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

use crate::actions::{Quit, ShowActivity, ShowConflicts, ShowOverview, ShowSettings, TogglePause};
use crate::components::{self, pill, status_dot};
use crate::engine::{self, EngineService, UiCommand};
use crate::platform;
use crate::state::{
    self, APP_TAGLINE, APP_TITLE, ActivityFilter, AppState, AppUpdate, DISCLOSURE, LoginState,
    ModeSwitchState, Page, Phase, SignOutState, SyncMode, badge, pause_label,
};
use pds_core::Db;
use crate::theme;

/// The root view. It owns the view state and bridges engine updates into the
/// GPUI main thread.
pub struct AppView {
    pub(crate) state: AppState,
    pub(crate) filter: ActivityFilter,
    sidecar_cmd: Option<String>,
    service: Option<EngineService>,
    updates_tx: UnboundedSender<AppUpdate>,
    focus_handle: FocusHandle,
    _pump: Task<()>,
}
impl AppView {
    pub fn new(
        root: PathBuf,
        sidecar_cmd: Option<String>,
        service: EngineService,
        updates_tx: UnboundedSender<AppUpdate>,
        mut updates_rx: UnboundedReceiver<AppUpdate>,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();
        let pump = cx.spawn(|this: gpui::WeakEntity<Self>, cx: &mut gpui::AsyncApp| {
            let cx = cx.clone();
            async move {
                while let Some(update) = updates_rx.recv().await {
                    let quit = cx
                        .update(|cx| {
                            this.update(cx, |view, cx| view.apply_update(update, cx))
                                .unwrap_or(false)
                        })
                        .unwrap_or(false);
                    if quit {
                        let _ = cx.update(|cx| cx.quit());
                        break;
                    }
                }
            }
        });

        Self {
            state: AppState::new(root),
            filter: ActivityFilter::All,
            sidecar_cmd,
            service: Some(service),
            updates_tx,
            focus_handle,
            _pump: pump,
        }
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Apply one engine update on the GPUI thread. Returns `true` when the
    /// app should quit (the engine finished a requested shutdown).
    fn apply_update(&mut self, update: AppUpdate, cx: &mut Context<Self>) -> bool {
        let before = self.state.phase;
        let was_mode_restart = matches!(self.state.mode_switch, ModeSwitchState::InProgress(_));
        if let AppUpdate::LoginUrl { url, .. } = &update {
            platform::open_url(url);
        }
        state::apply(&mut self.state, &update);

        if before != self.state.phase {
            info!(?before, phase = ?self.state.phase, "phase transition");
        }

        // A mode-switch shutdown: the engine thread has fully exited, so it
        // is safe to start a fresh one into the new mode.
        if was_mode_restart && matches!(update, AppUpdate::Exited { .. }) && !self.state.quitting {
            info!("restarting the engine into the new sync mode");
            self.start_engine(cx);
            return false;
        }

        let quit = self.state.quitting && matches!(update, AppUpdate::Exited { .. });
        cx.notify();
        quit
    }

    // -- commands ---------------------------------------------------------

    fn send(&self, cmd: UiCommand) {
        if let Some(service) = &self.service {
            service.send(cmd);
        }
    }

    fn start_login(&mut self, cx: &mut Context<Self>) {
        self.send(UiCommand::StartLogin);
        cx.notify();
    }

    pub(crate) fn toggle_pause(&mut self, cx: &mut Context<Self>) {
        let cmd = match self.state.engine_state() {
            Some(EngineState::Running) => UiCommand::Pause,
            Some(EngineState::Paused) | Some(EngineState::GuardPaused) => UiCommand::Resume,
            None => return,
        };
        self.send(cmd);
        cx.notify();
    }

    pub(crate) fn request_sign_out(&mut self, cx: &mut Context<Self>) {
        self.state.open_sign_out();
        cx.notify();
    }

    fn confirm_sign_out(&mut self, cx: &mut Context<Self>) {
        self.state.confirm_sign_out();
        self.send(UiCommand::SignOut);
        cx.notify();
    }

    fn cancel_sign_out(&mut self, cx: &mut Context<Self>) {
        self.state.cancel_sign_out();
        cx.notify();
    }

    /// Open the confirm modal for switching the sync mode.
    pub(crate) fn request_mode_switch(&mut self, to: SyncMode, cx: &mut Context<Self>) {
        self.state.open_mode_switch(to);
        cx.notify();
    }

    /// Confirmed: persist the new mode, then ask the engine thread to shut
    /// down. `apply_update` starts a fresh engine into the new mode when the
    /// `Exited` arrives — never two engines on one root at once.
    fn confirm_mode_switch(&mut self, cx: &mut Context<Self>) {
        let to = match self.state.mode_switch {
            ModeSwitchState::Confirming(to) => to,
            _ => return,
        };
        self.state.mode_switch = ModeSwitchState::InProgress(to);
        // Persist immediately (own SQLite connection; the engine's handle is
        // separate). The next engine construction reads this value.
        let db_path = pds_core::engine_state_dir(&self.state.root).join("state.db");
        if let Ok(db) = Db::open(&db_path)
            && let Err(e) = pds_core::set_sync_mode(&db, to)
        {
            warn!("persisting sync mode failed: {e}");
        }
        if let Some(service) = &self.service {
            service.send(UiCommand::Shutdown);
        }
        cx.notify();
    }

    fn cancel_mode_switch(&mut self, cx: &mut Context<Self>) {
        self.state.mode_switch = ModeSwitchState::None;
        cx.notify();
    }

    /// Settings changed the backing-cache cap: persist it (own SQLite
    /// connection) and let the running engine apply it live — no restart.
    pub(crate) fn set_cache_cap(&mut self, cap: Option<u64>, cx: &mut Context<Self>) {
        let db_path = pds_core::engine_state_dir(&self.state.root).join("state.db");
        if let Ok(db) = Db::open(&db_path)
            && let Err(e) = pds_core::set_cache_cap(&db, cap)
        {
            warn!("persisting cache cap failed: {e}");
        }
        if let Some(service) = &self.service {
            service.send(UiCommand::SetCacheCap(cap));
        }
        cx.notify();
    }

    /// Start a fresh background engine thread. Call only when the previous
    /// thread has fully exited (Failed-phase retry, or the post-shutdown
    /// mode switch) — never while one is running on the same root.
    fn start_engine(&mut self, cx: &mut Context<Self>) {
        self.state.phase = Phase::Starting;
        self.state.fatal = None;
        self.state.login = LoginState::Idle;
        self.state.mount_error = None;
        self.service = Some(engine::start(
            self.state.root.clone(),
            self.sidecar_cmd.clone(),
            self.updates_tx.clone(),
        ));
        cx.notify();
    }

    pub(crate) fn set_page(&mut self, page: Page, cx: &mut Context<Self>) {
        self.state.page = page;
        cx.notify();
    }

    fn set_filter(&mut self, filter: ActivityFilter, cx: &mut Context<Self>) {
        self.filter = filter;
        cx.notify();
    }

    pub(crate) fn open_sync_folder(&self) {
        platform::open_folder(&self.state.root);
    }

    /// Retry after a fatal startup/runtime error: start a fresh engine
    /// thread with the same configuration (the failed one has exited).
    fn retry(&mut self, cx: &mut Context<Self>) {
        self.start_engine(cx);
    }

    /// Quit gracefully. The engine gets a chance to stop transfers and close
    /// the sidecar; a fallback timer guarantees the process exits.
    pub(crate) fn request_quit(&mut self, cx: &mut Context<Self>) {
        if self.state.quitting {
            cx.quit();
            return;
        }
        self.state.quitting = true;
        if self.service.is_some() {
            self.send(UiCommand::Shutdown);
            cx.spawn(async move |_this, cx| {
                cx.background_executor().timer(Duration::from_secs(5)).await;
                let _ = cx.update(|cx| cx.quit());
            })
            .detach();
        } else {
            cx.quit();
            return;
        }
        cx.notify();
    }

    // -- rendering --------------------------------------------------------

    fn render_sidebar(&self, cx: &mut Context<Self>) -> Div {
        let mut nav = div().flex().flex_col().gap_1().mt_6();
        for page in Page::ALL {
            let selected = self.state.page == page;
            let badge = if page == Page::Conflicts {
                Some(self.state.attention_count())
            } else {
                None
            };
            nav = nav.child(components::nav_item(
                ("nav", page as usize),
                page.title(),
                selected,
                badge,
                cx,
                move |this, _, _, cx| this.set_page(page, cx),
            ));
        }

        let state = &self.state;
        let engine_color = match state.engine_state() {
            Some(EngineState::Running) if state.phase == Phase::Running => theme::OK,
            Some(EngineState::GuardPaused) => theme::DANGER,
            Some(EngineState::Paused) => theme::WARN,
            _ => theme::FAINT,
        };
        let pause_color = match state.engine_state() {
            Some(EngineState::Running) => theme::ACCENT,
            Some(EngineState::Paused) => theme::OK,
            Some(EngineState::GuardPaused) => theme::WARN,
            None => theme::BORDER_STRONG,
        };

        div()
            .flex()
            .flex_col()
            .w(px(theme::SIDEBAR_WIDTH))
            .h_full()
            .p_4()
            .bg(rgb(theme::SIDEBAR))
            .border_r_1()
            .border_color(rgb(theme::BORDER))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .size(px(34.0))
                            .rounded(px(10.0))
                            .bg(rgb(theme::ACCENT))
                            .flex()
                            .items_center()
                            .justify_center()
                            .child(
                                div()
                                    .size(px(15.0))
                                    .rounded_full()
                                    .border_2()
                                    .border_color(rgb(0xffffff)),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child(APP_TITLE),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(theme::FAINT))
                                    .child(APP_TAGLINE),
                            ),
                    ),
            )
            .child(nav)
            .child(div().flex_1())
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .p_3()
                    .rounded(px(10.0))
                    .bg(rgb(theme::CARD))
                    .border_1()
                    .border_color(rgb(theme::BORDER))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                div()
                                    .size(px(7.0))
                                    .rounded_full()
                                    .bg(rgb(engine_color)),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(engine_color))
                                    .child(badge(state)),
                            ),
                    )
                    .child(
                        div()
                            .text_sm()
                            .truncate()
                            .child(state.account_label()),
                    )
                    .child(components::wide_button(
                        "sidebar-pause",
                        pause_label(state),
                        pause_color,
                        cx,
                        |this, _, _, cx| this.toggle_pause(cx),
                    )),
            )
            .child(
                div()
                    .mt_3()
                    .flex()
                    .items_center()
                    .justify_between()
                    .text_xs()
                    .text_color(rgb(theme::FAINT))
                    .child(format!("v{}", env!("CARGO_PKG_VERSION")))
                    .child("Linux"),
            )
    }

    fn render_header(&self, cx: &mut Context<Self>) -> Div {
        let state = &self.state;
        let (label, color) = match state.engine_state() {
            Some(EngineState::Running) => ("Sync is running", theme::OK),
            Some(EngineState::Paused) => ("Sync is paused", theme::WARN),
            Some(EngineState::GuardPaused) => ("Paused by delete guard", theme::DANGER),
            None => ("Waiting for engine", theme::FAINT),
        };

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap_4()
            .px(px(theme::PAGE_PADDING))
            .py_4()
            .border_b_1()
            .border_color(rgb(theme::BORDER))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .text_xl()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(state.page.title()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(theme::MUTED))
                            .child(state.page.subtitle()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(status_dot(label, color))
                    .child(components::secondary_button(
                        "open-folder",
                        "Open folder",
                        cx,
                        |this, _, _, _| this.open_sync_folder(),
                    )),
            )
    }

    fn render_status_bar(&self) -> Div {
        let state = &self.state;
        let counts = state
            .status
            .as_ref()
            .map(|s| {
                format!(
                    "{} synced · {} pending · {} deletes queued",
                    s.synced_nodes, s.dirty_nodes, s.queued_tombstones
                )
            })
            .unwrap_or_else(|| "Engine starting…".into());

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap_4()
            .px(px(theme::PAGE_PADDING))
            .py_2()
            .border_t_1()
            .border_color(rgb(theme::BORDER))
            .text_xs()
            .text_color(rgb(theme::FAINT))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .min_w_0()
                    .child(
                        div()
                            .text_color(rgb(theme::MUTED))
                            .child("Sync folder"),
                    )
                    .child(
                        div()
                            .truncate()
                            .child(state.root.display().to_string()),
                    ),
            )
            .child(div().flex_shrink_0().child(counts))
    }

    fn render_shell(&mut self, cx: &mut Context<Self>) -> Div {
        let page: gpui::AnyElement = match self.state.page {
            Page::Overview => overview::render(self, cx),
            Page::Activity => activity::render(self, cx),
            Page::Conflicts => conflicts::render(self, cx),
            Page::Settings => settings::render(self, cx),
        };

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(
                div()
                    .flex()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(self.render_sidebar(cx))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_w_0()
                            .min_h(px(0.0))
                            .child(self.render_header(cx))
                            .child(page),
                    ),
            )
            .child(self.render_status_bar())
    }

    fn render_login(&mut self, cx: &mut Context<Self>) -> Div {
        let mut card = div()
            .flex()
            .flex_col()
            .gap_4()
            .w(px(460.0))
            .p_6()
            .rounded(px(14.0))
            .bg(rgb(theme::CARD))
            .border_1()
            .border_color(rgb(theme::BORDER))
            .child(self.brand_block())
            .child(
                div()
                    .p_3()
                    .rounded(px(8.0))
                    .bg(rgb(theme::INSET))
                    .text_xs()
                    .text_color(rgb(theme::MUTED))
                    .child(DISCLOSURE),
            );

        match &self.state.login {
            LoginState::Idle => {
                card = card
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(theme::MUTED))
                            .child(
                                "Sign in with your Proton account to start syncing. Your browser \
                                 handles the sign-in — this app never sees your password.",
                            ),
                    )
                    .child(components::primary_button(
                        "sign-in",
                        "Sign in with Proton",
                        cx,
                        |this, _, _, cx| this.start_login(cx),
                    ));
            }
            LoginState::Waiting => {
                card = card
                    .child(div().text_sm().child("Opening the Proton sign-in page…"))
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(theme::MUTED))
                            .child("If nothing happens, check that a browser is available."),
                    );
            }
            LoginState::Url { url, user_code } => {
                card = card.child(
                    div()
                        .text_sm()
                        .child("Finish signing in in the browser window that just opened."),
                );
                if !user_code.is_empty() {
                    card = card.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .p_3()
                            .rounded(px(8.0))
                            .bg(rgb(theme::INSET))
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(theme::MUTED))
                                    .child("Verification code"),
                            )
                            .child(
                                div()
                                    .text_2xl()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child(user_code.clone()),
                            ),
                    );
                }
                let url_text = url.clone();
                let open_url = url.clone();
                card = card
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(theme::FAINT))
                            .truncate()
                            .child(url_text),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(components::secondary_button(
                                "reauth-open",
                                "Open sign-in page again",
                                cx,
                                move |_, _, _, _| platform::open_url(&open_url),
                            ))
                            .child(status_dot(
                                "Waiting for sign-in…",
                                theme::ACCENT,
                            )),
                    );
            }
            LoginState::Failed(error) => {
                let error = error.clone();
                card = card
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(theme::DANGER))
                            .child(format!("Sign-in failed: {error}")),
                    )
                    .child(components::primary_button(
                        "retry-login",
                        "Try again",
                        cx,
                        |this, _, _, cx| this.start_login(cx),
                    ));
            }
        }

        div()
            .flex()
            .items_center()
            .justify_center()
            .size_full()
            .p_6()
            .child(card)
    }

    fn render_splash(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap_4()
            .size_full()
            .child(self.brand_block())
            .child(status_dot("Starting the sync engine…", theme::ACCENT))
            .child(
                div()
                    .max_w(px(420.0))
                    .text_xs()
                    .text_color(rgb(theme::FAINT))
                    .child(DISCLOSURE),
            )
    }

    fn render_failure(&mut self, cx: &mut Context<Self>) -> Div {
        let error = self
            .state
            .fatal
            .clone()
            .unwrap_or_else(|| "The sync engine stopped unexpectedly.".into());

        div()
            .flex()
            .items_center()
            .justify_center()
            .size_full()
            .p_6()
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .w(px(520.0))
                    .p_6()
                    .rounded(px(14.0))
                    .bg(rgb(theme::CARD))
                    .border_1()
                    .border_color(rgb(theme::BORDER))
                    .child(self.brand_block())
                    .child(
                        div()
                            .text_base()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(theme::DANGER))
                            .child("Sync is stopped"),
                    )
                    .child(
                        div()
                            .p_3()
                            .rounded(px(8.0))
                            .bg(rgb(theme::INSET))
                            .text_xs()
                            .text_color(rgb(theme::MUTED))
                            .child(error),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(components::primary_button(
                                "retry-engine",
                                "Try again",
                                cx,
                                |this, _, _, cx| this.retry(cx),
                            ))
                            .child(components::text_button(
                                "quit-failed",
                                "Quit",
                                cx,
                                |this, _, _, cx| this.request_quit(cx),
                            )),
                    ),
            )
    }

    fn brand_block(&self) -> Div {
        div()
            .flex()
            .items_center()
            .gap_3()
            .child(
                div()
                    .size(px(38.0))
                    .rounded(px(11.0))
                    .bg(rgb(theme::ACCENT))
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .size(px(17.0))
                            .rounded_full()
                            .border_2()
                            .border_color(rgb(0xffffff)),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(APP_TITLE),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(theme::MUTED))
                            .child(APP_TAGLINE),
                    ),
            )
    }

    /// Confirm dialog for switching the sync mode (Settings).
    fn render_mode_switch_modal(&self, cx: &mut Context<Self>) -> Div {
        let (title, body) = match self.state.mode_switch {
            ModeSwitchState::Confirming(SyncMode::Mirror) => (
                "Download everything to this folder?",
                "Switching to mirror materializes every file in your sync folder. Files already \
                 on disk are kept; the rest download from Drive once. Your Drive is never \
                 deleted.",
            ),
            ModeSwitchState::Confirming(SyncMode::Placeholders) => (
                "Switch to on-demand placeholders?",
                "Files in your sync folder become placeholders at ~0 bytes. Content stays on \
                 Drive; opening a file fetches it on demand and the local cache is capped (LRU \
                 eviction). Nothing is deleted from Drive.",
            ),
            ModeSwitchState::None => ("", ""),
            ModeSwitchState::InProgress(_) => ("Switching sync mode…", ""),
        };

        let mut panel = div()
            .flex()
            .flex_col()
            .gap_4()
            .w(px(460.0))
            .p_6()
            .rounded(px(14.0))
            .bg(rgb(theme::CARD))
            .border_1()
            .border_color(rgb(theme::BORDER))
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(title),
            );
        if !body.is_empty() {
            panel = panel.child(
                div()
                    .text_sm()
                    .text_color(rgb(theme::MUTED))
                    .child(body),
            );
        }

        if let ModeSwitchState::InProgress(_) = self.state.mode_switch {
            panel = panel.child(status_dot("Switching…", theme::ACCENT));
        } else {
            panel = panel.child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(components::primary_button(
                        "confirm-mode-switch",
                        "Switch mode",
                        cx,
                        |this, _, _, cx| this.confirm_mode_switch(cx),
                    ))
                    .child(components::secondary_button(
                        "cancel-mode-switch",
                        "Cancel",
                        cx,
                        |this, _, _, cx| this.cancel_mode_switch(cx),
                    )),
            );
        }

        div()
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(rgba(0x05060aa8))
            .child(panel)
    }

    fn render_sign_out_modal(&self, cx: &mut Context<Self>) -> Div {
        let email = self.state.account_label();
        let mut panel = div()
            .flex()
            .flex_col()
            .gap_4()
            .w(px(430.0))
            .p_6()
            .rounded(px(14.0))
            .bg(rgb(theme::CARD))
            .border_1()
            .border_color(rgb(theme::BORDER))
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(format!("Sign out of {email}?")),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(theme::MUTED))
                    .child(
                        "Sync will stop and the saved session will be deleted. Files on disk are \
                         kept, and you can sign back in at any time.",
                    ),
            );

        if self.state.sign_out == SignOutState::InProgress {
            panel = panel.child(status_dot("Signing out…", theme::ACCENT));
        } else {
            panel = panel.child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(components::danger_button(
                        "confirm-sign-out",
                        "Sign out",
                        cx,
                        |this, _, _, cx| this.confirm_sign_out(cx),
                    ))
                    .child(components::secondary_button(
                        "cancel-sign-out",
                        "Cancel",
                        cx,
                        |this, _, _, cx| this.cancel_sign_out(cx),
                    )),
            );
        }

        div()
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(rgba(0x05060aa8))
            .child(panel)
    }
}

impl Render for AppView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let body = match self.state.phase {
            Phase::NeedsLogin => self.render_login(cx),
            Phase::Failed => self.render_failure(cx),
            Phase::Starting => self.render_splash(),
            Phase::Running => self.render_shell(cx),
        };

        let mut root = div()
            .size_full()
            .relative()
            .bg(rgb(theme::BG))
            .text_color(rgb(theme::TEXT))
            .text_sm()
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &Quit, _, cx| this.request_quit(cx)))
            .on_action(cx.listener(|this, _: &TogglePause, _, cx| this.toggle_pause(cx)))
            .on_action(cx.listener(|this, _: &ShowOverview, _, cx| this.set_page(Page::Overview, cx)))
            .on_action(cx.listener(|this, _: &ShowActivity, _, cx| this.set_page(Page::Activity, cx)))
            .on_action(cx.listener(|this, _: &ShowConflicts, _, cx| this.set_page(Page::Conflicts, cx)))
            .on_action(cx.listener(|this, _: &ShowSettings, _, cx| this.set_page(Page::Settings, cx)))
            .child(body);

        if self.state.sign_out != SignOutState::None {
            root = root.child(self.render_sign_out_modal(cx));
        }
        if self.state.mode_switch != ModeSwitchState::None {
            root = root.child(self.render_mode_switch_modal(cx));
        }
        root
    }
}

/// Keyboard shortcuts registered at startup.
pub fn key_bindings() -> Vec<KeyBinding> {
    vec![
        KeyBinding::new("ctrl-q", Quit, None),
        KeyBinding::new("ctrl-p", TogglePause, None),
        KeyBinding::new("ctrl-1", ShowOverview, None),
        KeyBinding::new("ctrl-2", ShowActivity, None),
        KeyBinding::new("ctrl-3", ShowConflicts, None),
        KeyBinding::new("ctrl-4", ShowSettings, None),
    ]
}

/// Small helper used by page modules for consistent activity kind colours.
pub(crate) fn kind_color(kind: pds_core::ActivityKind) -> u32 {
    use pds_core::ActivityKind;
    match kind {
        ActivityKind::Uploaded => theme::ACCENT,
        ActivityKind::Downloaded => theme::OK,
        ActivityKind::Trashed => theme::MUTED,
        ActivityKind::Conflict => theme::WARN,
        ActivityKind::GuardPaused | ActivityKind::UploadErrored => theme::DANGER,
    }
}

/// One activity row shared by the overview preview and the activity page.
pub(crate) fn activity_row(item: &pds_core::ActivityItem) -> Div {
    let color = kind_color(item.kind);
    let mut row = div()
        .flex()
        .items_center()
        .gap_3()
        .py_2()
        .border_b_1()
        .border_color(rgb(theme::BORDER))
        .child(
            div()
                .w(px(64.0))
                .flex_shrink_0()
                .text_xs()
                .text_color(rgb(theme::FAINT))
                .child(state::format_time(item.at_unix)),
        )
        .child(
            div()
                .w(px(92.0))
                .flex_shrink_0()
                .child(pill(state::kind_label(item.kind), color, theme::INSET)),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .text_sm()
                .child(item.path.clone()),
        );

    if let Some(detail) = &item.detail {
        row = row.child(
            div()
                .max_w(px(320.0))
                .truncate()
                .text_xs()
                .text_color(rgb(theme::MUTED))
                .child(detail.clone()),
        );
    }
    row
}
