//! `proton-drive-sync` — native Linux desktop client (GPUI).
//!
//! The process is the sync service: it starts at login (XDG autostart), owns
//! the engine + FUSE mount, and keeps a StatusNotifierItem tray icon alive.
//! A login start stays in the tray only; the dashboard window is created on
//! first use. Closing it unmaps the window and keeps the process (and sync)
//! running; Quit lives in the tray and in the app's own controls.

mod actions;
mod autostart;
mod components;
mod engine;
mod instance;
mod platform;
mod state;
mod theme;
mod tray;
mod ui;

use std::time::Duration;

use gpui::{App, AppContext};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use instance::Launch;
use state::APP_TITLE;
use ui::AppView;

enum CliAction {
    Run { background: bool },
    Exit,
    Headless,
}

fn main() {
    let action = handle_cli();
    let background = match action {
        CliAction::Run { background } => background,
        CliAction::Exit => return,
        CliAction::Headless => {
            init_logging();
            run_headless_proof();
            return;
        }
    };

    init_logging();

    // One engine per user. A second launch (menu click while the autostarted
    // instance is in the tray) asks the primary to show its dashboard.
    let Launch::Primary {
        events: show_events,
        _guard: instance_guard,
    } = instance::acquire()
    else {
        return;
    };

    // First-run start-at-login setup (installed builds enable it, checkout
    // builds stay opt-in). Failures are non-fatal.
    autostart::init_on_launch();

    // `--minimized` is authoritative: a login start never opens a window on
    // its own. Probing the tray here is only for logging — the panel (and its
    // StatusNotifierWatcher) may still be starting, so the probe must not
    // change the decision. The fallback way back to the dashboard is starting
    // the app again, which asks this instance to show it.
    let tray_host = tray::host_available();
    if background && !tray_host {
        info!(
            "no StatusNotifier tray host found yet; staying windowless \
             (start the app again to open the dashboard)"
        );
    }

    let root = platform::sync_root();
    let sidecar_cmd = std::env::var("PDS_SIDECAR_CMD").ok();
    info!(root = %root.display(), background, "starting {APP_TITLE}");

    let (updates_tx, updates_rx) = mpsc::unbounded_channel();
    let service = engine::start(root.clone(), sidecar_cmd.clone(), updates_tx.clone());

    gpui_platform::application().run(move |cx: &mut App| {
        cx.bind_keys(ui::key_bindings());

        // The view outlives any single window: it owns the state and pumps
        // engine updates into it (and into the tray status) even while the
        // dashboard has never been opened, e.g. when started at login.
        let view = cx.new(|cx| {
            AppView::new(
                root,
                sidecar_cmd,
                service,
                updates_tx,
                updates_rx,
                cx,
            )
        });
        cx.set_global(tray::Dashboard::new(view.clone()));

        match tray::build(cx) {
            Ok(tray) => {
                // `gpui-tray` builds even without a watcher (it keeps
                // retrying registration), so the host probe — not the build
                // result — decides whether closing hides the window.
                cx.set_global(tray::TrayHolder(tray));
                cx.set_global(tray::TrayAvailable(tray_host));
            }
            Err(e) => {
                // Keep close = quit so the window can never become
                // unreachable behind a tray that does not exist.
                cx.set_global(tray::TrayAvailable(false));
                warn!("tray icon unavailable: {e} (closing the window quits)");
            }
        }

        cx.on_action(tray::on_open_dashboard);
        cx.on_action(tray::on_open_settings);
        cx.on_action(tray::on_open_sync_folder);
        cx.on_action(tray::on_toggle_pause);
        cx.on_action(tray::on_quit);

        // Started at login: stay in the tray only, whatever the tray probe
        // and build results were. An autostart entry must never open a
        // window on its own; if no tray icon is usable, starting the app
        // again opens the dashboard (single instance). Otherwise the window
        // is created on first use.
        if background {
            if cx.try_global::<tray::TrayHolder>().is_some() && tray_host {
                info!("started in the background; the dashboard is available from the tray");
            } else {
                info!(
                    "started in the background without a tray icon; \
                     start the app again to open the dashboard"
                );
            }
        } else if let Err(e) = tray::show_dashboard(cx, None) {
            error!("could not open the application window: {e:#}");
            cx.quit();
            return;
        }

        // Later launches ask this instance to surface the dashboard.
        let mut show_events = show_events;
        cx.spawn(async move |cx| {
            let _guard = instance_guard;
            while show_events.recv().await.is_some() {
                let shown = cx.update(|cx| tray::show_dashboard(cx, None).is_ok());
                if !shown {
                    break;
                }
            }
        })
        .detach();
    });
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pds_core=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();
}

/// Handle `--help` / `--version` / `--headless` without starting the GUI.
fn handle_cli() -> CliAction {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => CliAction::Run { background: false },
        Some("--version" | "-V") => {
            println!("{APP_TITLE} {}", env!("CARGO_PKG_VERSION"));
            CliAction::Exit
        }
        Some("--help" | "-h") => {
            println!(
                "{APP_TITLE} {}\n\n\
                 Unofficial Proton Drive sync client for Linux.\n\n\
                 Usage: proton-drive-sync [OPTIONS]\n\n\
                 Options:\n  \
                   -h, --help       Show this help\n  \
                   -V, --version    Show the version\n  \
                       --minimized  Start in the tray without opening a window\n  \
                       --headless   Start without a window (smoke test)\n\n\
                 Environment:\n  \
                   PDS_SYNC_ROOT     Sync folder (default: ~/ProtonDrive)\n  \
                   PDS_SIDECAR_CMD   Sidecar command override\n  \
                   RUST_LOG          Log filter (default: info,pds_core=debug)",
                env!("CARGO_PKG_VERSION")
            );
            CliAction::Exit
        }
        Some("--minimized") => CliAction::Run { background: true },
        Some("--headless") => CliAction::Headless,
        Some(other) => {
            eprintln!("unknown argument `{other}` (try --help)");
            CliAction::Exit
        }
    }
}

/// Boot the GPUI event loop without a window. Used by packaging/CI checks on
/// machines without a display; proves the platform backend links and runs.
fn run_headless_proof() {
    gpui_platform::headless().run(|cx: &mut App| {
        cx.spawn(async move |cx| {
            cx.background_executor()
                .timer(Duration::from_millis(250))
                .await;
            cx.update(|cx| {
                println!("headless proof: event loop OK");
                cx.quit();
            });
        })
        .detach();
    });
}
