//! `proton-drive-sync` — windowless menu-bar shell (M2, SPEC.md §6).
//!
//! Shape per docs/research/gpui.md §5: a tray icon owns the app's lifetime;
//! windows open on demand and closing them never quits the app. The M1 sync
//! engine runs in-process on its own tokio runtime (runtime.rs); the UI
//! bridges to it through channels only (gpui.md §3.2).

mod app_model;
mod login;
mod model;
mod runtime;
mod status;
mod widgets;

use gpui::{App, AppContext, Application, Entity, Global, Menu, MenuItem, actions};
use gpui_tray::{Icon, Tray};
use pds_core::EngineState;
use tokio::sync::mpsc;
use tracing::{error, info};

use app_model::AppModel;
use model::{APP_TITLE, ModelState, UiCommand};

actions!(pds_app, [OpenStatus, TogglePause, SignOut, QuitApp]);

/// What the tray menu builder needs to render the pause/resume label.
#[derive(Clone, Copy, Default)]
struct TrayState {
    engine_state: Option<EngineState>,
}

impl Global for TrayState {}

struct ModelHolder(Entity<AppModel>);
impl Global for ModelHolder {}

struct TrayHolder(Tray);
impl Global for TrayHolder {}

fn tray_menu(cx: &mut App) -> Vec<MenuItem> {
    let label = match cx.try_global::<TrayState>() {
        Some(state) if state.engine_state == Some(EngineState::Running) => "Pause Syncing",
        _ => "Resume Syncing",
    };
    vec![
        MenuItem::action("Open Status", OpenStatus),
        MenuItem::action(label, TogglePause),
        MenuItem::separator(),
        MenuItem::action("Sign Out…", SignOut),
        MenuItem::separator(),
        MenuItem::action(format!("Quit {APP_TITLE}"), QuitApp),
    ]
}

fn build_tray(cx: &mut App) -> gpui_tray::Result<Tray> {
    Tray::builder()
        .icon(tray_icon()?)
        .tooltip(APP_TITLE)
        .menu(tray_menu)
        .build(cx)
}

/// A neutral "sync ring" drawn in code — no bundled assets, no Proton
/// branding. Template-rendered on macOS (adapts to light/dark menu bars).
fn tray_icon() -> gpui_tray::Result<Icon> {
    const SIZE: u32 = 36;
    let mut rgba = vec![0u8; (SIZE * SIZE * 4) as usize];
    for y in 0..SIZE {
        for x in 0..SIZE {
            let dx = x as f32 - 17.5;
            let dy = y as f32 - 17.5;
            let r = (dx * dx + dy * dy).sqrt();
            // Ring with two gaps, evoking circular arrows.
            let angle = dy.atan2(dx);
            let gap = (angle - 0.6).abs() < 0.35 || (angle + 0.6).abs() < 0.35;
            if (9.0..=15.0).contains(&r) && !gap {
                let o = ((y * SIZE + x) * 4) as usize;
                rgba[o..o + 4].copy_from_slice(&[255, 255, 255, 255]);
            }
        }
    }
    let builder = Icon::from_rgba(rgba, SIZE, SIZE);
    #[cfg(target_os = "macos")]
    let builder = builder.scale_factor(2.0).template(true);
    builder.build()
}

fn on_open_status(_: &OpenStatus, cx: &mut App) {
    app_model::open_front_window(cx);
}

fn on_toggle_pause(_: &TogglePause, cx: &mut App) {
    if let Some(holder) = cx.try_global::<ModelHolder>() {
        let model = holder.0.clone();
        model.update(cx, |m, _| {
            let cmd = model::toggle_command(&m.state);
            m.send(cmd);
        });
    }
}

/// Tray "Sign Out…": open/focus the status window with the confirm panel
/// showing (sign-out is destructive — the panel, not the menu, confirms).
fn on_sign_out(_: &SignOut, cx: &mut App) {
    let confirming = cx
        .try_global::<ModelHolder>()
        .map(|holder| holder.0.clone())
        .map(|model| {
            model.update(cx, |m, cx| {
                m.state.begin_sign_out();
                cx.notify();
                m.state.sign_out == model::SignOutState::Confirming
            })
        })
        .unwrap_or(false);
    if confirming {
        app_model::open_status_window(cx);
    } else {
        app_model::open_front_window(cx);
    }
}

fn on_quit(_: &QuitApp, cx: &mut App) {
    if let Some(holder) = cx.try_global::<ModelHolder>() {
        let model = holder.0.clone();
        model.update(cx, |m, cx| m.request_quit(cx));
    } else {
        cx.quit();
    }
}

/// Dock-click reopen (only reachable unbundled; the bundle sets
/// LSUIElement). Reopens the window matching the current phase.
fn on_reopen(cx: &mut App) {
    if cx.windows().is_empty() {
        app_model::open_front_window(cx);
    }
}

/// `PDS_HEADLESS_PROOF=1`: boot the app headless (no windows, no engine),
/// prove the event loop + tray construction work, then quit. Used by CI-ish
/// checks on machines without a GUI session.
fn run_headless_proof() {
    Application::headless().run(|cx: &mut App| {
        cx.set_global(TrayState::default());
        let (ui_tx, _ui_rx) = mpsc::unbounded_channel();
        let (_updates_tx, updates_rx) = mpsc::unbounded_channel();
        let root = model::sync_root();
        let model = cx.new(|cx| AppModel::new(ModelState::new(root), ui_tx, updates_rx, cx));
        cx.set_global(ModelHolder(model));
        match build_tray(cx) {
            Ok(tray) => {
                cx.set_global(TrayHolder(tray));
                println!("headless proof: tray built");
            }
            Err(e) => println!("headless proof: tray unavailable headless ({e})"),
        }
        cx.spawn(async move |cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_millis(300))
                .await;
            let _ = cx.update(|cx| {
                println!("headless proof: event loop OK");
                cx.quit();
            });
        })
        .detach();
    });
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pds_core=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    if std::env::var_os("PDS_HEADLESS_PROOF").is_some() {
        run_headless_proof();
        return;
    }

    let root = model::sync_root();
    let sidecar_cmd = std::env::var("PDS_SIDECAR_CMD").ok();
    info!(root = %root.display(), "starting {APP_TITLE}");

    let app = Application::new();
    app.on_reopen(on_reopen);
    app.run(move |cx: &mut App| {
        let (ui_tx, ui_rx) = mpsc::unbounded_channel::<UiCommand>();
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();

        cx.set_global(TrayState::default());
        let model =
            cx.new(|cx| AppModel::new(ModelState::new(root.clone()), ui_tx, updates_rx, cx));
        cx.set_global(ModelHolder(model.clone()));

        match build_tray(cx) {
            Ok(tray) => cx.set_global(TrayHolder(tray)),
            Err(e) => error!("tray unavailable: {e} (windows still work)"),
        }

        // App menu (visible while one of our windows is focused): Quit.
        cx.set_menus(vec![Menu {
            name: APP_TITLE.into(),
            items: vec![MenuItem::action(format!("Quit {APP_TITLE}"), QuitApp)],
        }]);

        cx.on_action(on_open_status);
        cx.on_action(on_toggle_pause);
        cx.on_action(on_sign_out);
        cx.on_action(on_quit);

        // Never quit when the last window closes (menu-bar app): simply no
        // on_window_closed handler. macOS does not auto-quit either.

        runtime::start_engine_runtime(root, sidecar_cmd, updates_tx, ui_rx);

        // Dev hook: `PDS_DEBUG_SIGNOUT=1` confirms sign-out as soon as the
        // engine is running — exercises the whole flow (graceful engine
        // stop → auth.logout → login view) without GUI automation.
        if std::env::var_os("PDS_DEBUG_SIGNOUT").is_some() {
            cx.spawn(async move |cx| {
                for _ in 0..60 {
                    cx.background_executor()
                        .timer(std::time::Duration::from_millis(500))
                        .await;
                    let triggered = cx
                        .update(|cx| {
                            model.update(cx, |m, cx| {
                                m.state.begin_sign_out();
                                if m.state.sign_out == model::SignOutState::Confirming {
                                    m.state.confirm_sign_out();
                                    m.send(UiCommand::SignOut);
                                    cx.notify();
                                    true
                                } else {
                                    false
                                }
                            })
                        })
                        .unwrap_or(false);
                    if triggered {
                        break;
                    }
                }
            })
            .detach();
        }
    });
}
