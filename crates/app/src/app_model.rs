//! The `AppModel` entity: owns the [`ModelState`], pumps [`AppUpdate`]s
//! from the engine runtime into it (gpui.md §3.2 bridge pattern), and
//! manages the login/status windows. Views observe this entity and
//! re-render on `cx.notify()`.

use std::time::Duration;

use gpui::{
    App, AppContext, Bounds, Context, Entity, Task, TitlebarOptions, WindowBounds, WindowHandle,
    WindowOptions, px, size,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, warn};

use crate::login::LoginView;
use crate::model::{self, APP_TITLE, AppUpdate, ModelState, Phase, UiCommand};
use crate::status::StatusView;
use crate::{ModelHolder, TrayHolder, TrayState};

pub struct AppModel {
    pub state: ModelState,
    ui_tx: UnboundedSender<UiCommand>,
    login_window: Option<WindowHandle<LoginView>>,
    status_window: Option<WindowHandle<StatusView>>,
    _pump: Task<()>,
}

impl AppModel {
    pub fn new(
        state: ModelState,
        ui_tx: UnboundedSender<UiCommand>,
        mut updates_rx: UnboundedReceiver<AppUpdate>,
        cx: &mut Context<Self>,
    ) -> Self {
        let pump = cx.spawn(|this: gpui::WeakEntity<Self>, cx: &mut gpui::AsyncApp| {
            // Clone the context handle (Weak refs — 'static) into the
            // future; the `&mut AsyncApp` itself must not be held across
            // await points (spawned futures are 'static).
            let cx = cx.clone();
            async move {
                while let Some(update) = updates_rx.recv().await {
                    // Channel recv is executor-agnostic (safe on gpui's smol
                    // executor); hop to the main thread to mutate the entity.
                    // AsyncApp::update fails during app shutdown and
                    // WeakEntity::update once the entity is gone — both mean
                    // "stop pumping".
                    let ok = cx
                        .update(|cx| this.update(cx, |model, cx| model.apply(update, cx)).is_ok())
                        .unwrap_or(false);
                    if !ok {
                        break;
                    }
                }
            }
        });
        Self {
            state,
            ui_tx,
            login_window: None,
            status_window: None,
            _pump: pump,
        }
    }

    /// Send a command to the engine runtime (pause/resume/login/shutdown).
    pub fn send(&self, cmd: UiCommand) {
        let _ = self.ui_tx.send(cmd);
    }

    /// This entity's own handle (stored as a global at startup).
    fn this(cx: &App) -> Entity<Self> {
        cx.global::<ModelHolder>().0.clone()
    }

    /// Reducer + side effects (browser open, tray refresh, window swaps).
    fn apply(&mut self, update: AppUpdate, cx: &mut Context<Self>) {
        let before = self.state.phase;
        model::apply(&mut self.state, &update);

        match &update {
            // Sign-in page → the user's browser (fork login, protocol.md).
            AppUpdate::LoginUrl { url, .. } => open_in_browser(url),
            AppUpdate::Status(_) | AppUpdate::EngineStarted { .. } => refresh_tray(cx, &self.state),
            AppUpdate::Exited { .. } if self.state.quitting => {
                // Engine thread finished the requested shutdown; quit now.
                cx.quit();
                return;
            }
            _ => {}
        }

        let after = self.state.phase;
        if after != before {
            // Window management READS AppModel (opening a window renders the
            // new view immediately, and both views do `model.read(cx)` in
            // render) — doing it here, inside `update`, is a re-entrant
            // entity access and panics (entity_map.rs: "already being
            // updated"). Defer it past the end of this update.
            cx.defer(move |cx| match after {
                Phase::NeedsLogin => open_login_window(cx),
                Phase::Running => {
                    close_login_window(cx);
                    open_status_window(cx);
                }
                Phase::Failed => open_status_window(cx), // shows the error
                Phase::Starting => {}
            });
        }
        cx.notify();
    }

    /// Quit menu item / tray Quit: ask the engine to shut down; the pump
    /// quits the app on `Exited`. A fallback timer force-quits after 3 s in
    /// case the engine wedges mid-transfer.
    pub fn request_quit(&mut self, cx: &mut App) {
        if self.state.quitting {
            cx.quit();
            return;
        }
        self.state.quitting = true;
        self.send(UiCommand::Shutdown);
        cx.spawn(async move |cx| {
            cx.background_executor().timer(Duration::from_secs(3)).await;
            let _ = cx.update(|cx| cx.quit());
        })
        .detach();
    }
}

// ---------------------------------------------------------------------------
// Window management — free functions taking `&mut App`, NEVER called from
// inside `Entity<AppModel>::update`: `cx.open_window` renders the new view
// immediately, and both views read AppModel in `render`, so an open nested
// in a model update panics with re-entrant entity access. Pattern: read /
// clone what's needed first, open the window, then store the handle with a
// tiny update that performs no window work.
// ---------------------------------------------------------------------------

/// Tray "Open Status" / dock reopen: surface the window matching the
/// current phase, activating an existing one if possible.
pub fn open_front_window(cx: &mut App) {
    let model = AppModel::this(cx);
    cx.activate(true);
    match model.read(cx).state.phase {
        Phase::NeedsLogin => open_login_window(cx),
        _ => open_status_window(cx),
    }
}

fn open_login_window(cx: &mut App) {
    let model = AppModel::this(cx);
    let existing = model.read(cx).login_window;
    if let Some(w) = existing
        && w.update(cx, |_, window, _| window.activate_window())
            .is_ok()
    {
        return;
    }
    let options = window_options(px(480.0), px(420.0), cx);
    match cx.open_window(options, |_, cx| {
        cx.new(|cx| LoginView::new(model.clone(), cx))
    }) {
        Ok(handle) => model.update(cx, |m, _| m.login_window = Some(handle)),
        Err(e) => error!("open login window: {e:#}"),
    }
}

fn open_status_window(cx: &mut App) {
    let model = AppModel::this(cx);
    let existing = model.read(cx).status_window;
    if let Some(w) = existing
        && w.update(cx, |_, window, _| window.activate_window())
            .is_ok()
    {
        return;
    }
    let options = window_options(px(780.0), px(560.0), cx);
    match cx.open_window(options, |_, cx| {
        cx.new(|cx| StatusView::new(model.clone(), cx))
    }) {
        Ok(handle) => model.update(cx, |m, _| m.status_window = Some(handle)),
        Err(e) => error!("open status window: {e:#}"),
    }
}

fn close_login_window(cx: &mut App) {
    let model = AppModel::this(cx);
    let w = model.update(cx, |m, _| m.login_window.take());
    if let Some(w) = w {
        let _ = w.update(cx, |_, window, _| window.remove_window());
    }
}

fn window_options(width: gpui::Pixels, height: gpui::Pixels, cx: &App) -> WindowOptions {
    let bounds = Bounds::centered(None, size(width, height), cx);
    WindowOptions {
        window_bounds: Some(WindowBounds::Windowed(bounds)),
        titlebar: Some(TitlebarOptions {
            title: Some(APP_TITLE.into()),
            appears_transparent: false,
            traffic_light_position: None,
        }),
        ..Default::default()
    }
}

/// Push the pause/resume label state into the tray global and rebuild the
/// tray menu (gpui-tray re-runs the menu builder; there is no item-level
/// label mutation API).
fn refresh_tray(cx: &mut App, state: &ModelState) {
    cx.set_global(TrayState {
        engine_state: state.engine_state(),
    });
    if let Some(tray) = cx.try_global::<TrayHolder>() {
        let tray = tray.0.clone();
        if let Err(e) = tray.refresh_menu(cx) {
            warn!("tray refresh: {e}");
        }
    }
}

/// Open a URL in the system browser (no webbrowser crate — `open` /
/// `xdg-open` are enough for the fork-login page).
pub fn open_in_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let program = "open";
    #[cfg(target_os = "linux")]
    let program = "xdg-open";
    if let Err(e) = std::process::Command::new(program).arg(url).spawn() {
        warn!(url, "open in browser: {e}");
    }
}
