//! `proton-drive-sync` — native Linux desktop client (GPUI).
//!
//! The process is the sync service: it starts at login (XDG autostart), owns
//! the engine + FUSE mount, and keeps a StatusNotifierItem tray icon alive.
//! Closing the dashboard window minimizes it instead of stopping sync; Quit
//! lives in the tray and in the app's own controls.

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

use gpui::{
    App, AppContext, Application, Bounds, TitlebarOptions, WindowBounds, WindowOptions, px, size,
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use instance::Launch;
use state::APP_TITLE;
use ui::AppView;

enum CliAction {
    Run { minimized: bool },
    Exit,
    Headless,
}

fn main() {
    let action = handle_cli();
    let minimized = match action {
        CliAction::Run { minimized } => minimized,
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

    // Never hide the dashboard at login on a desktop that cannot show the
    // tray icon: there would be no way back to the window.
    let tray_host = tray::host_available();
    let minimized = minimized && tray_host;
    if !tray_host {
        info!("no StatusNotifier tray host found; the dashboard opens at login");
    }

    let root = platform::sync_root();
    let sidecar_cmd = std::env::var("PDS_SIDECAR_CMD").ok();
    info!(root = %root.display(), minimized, "starting {APP_TITLE}");

    let (updates_tx, updates_rx) = mpsc::unbounded_channel();
    let service = engine::start(root.clone(), sidecar_cmd.clone(), updates_tx.clone());

    Application::new().run(move |cx: &mut App| {
        cx.bind_keys(ui::key_bindings());
        cx.set_global(tray::TrayAvailable(tray_host));

        // The view outlives any single activation of the window: the window
        // is created once and minimized (not destroyed) when closed.
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

        let bounds = Bounds::centered(None, size(px(1080.0), px(720.0)), cx);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            titlebar: Some(TitlebarOptions {
                title: Some(APP_TITLE.into()),
                appears_transparent: false,
                traffic_light_position: None,
            }),
            app_id: Some("proton-drive-sync".into()),
            window_min_size: Some(size(px(900.0), px(620.0))),
            ..Default::default()
        };

        let window_view = view.clone();
        let opened = cx.open_window(options, move |window, cx| {
            window.focus(window_view.read(cx).focus_handle());

            // Closing the dashboard puts the app back in the tray; only the
            // tray's Quit (or the in-app quit buttons) actually stops sync.
            let weak = window_view.downgrade();
            window.on_window_should_close(cx, move |window, cx| {
                let quitting = weak
                    .read_with(cx, |view, _| view.state.quitting)
                    .unwrap_or(true);
                if quitting {
                    return false;
                }
                let tray_available = cx
                    .try_global::<tray::TrayAvailable>()
                    .is_some_and(|flag| flag.0);
                if tray_available {
                    window.minimize_window();
                } else {
                    // No tray to restore from: closing means quit.
                    let _ = weak.update(cx, |view, cx| view.request_quit(cx));
                }
                false
            });

            window_view
        });

        let window = match opened {
            Ok(window) => window,
            Err(e) => {
                error!("could not open the application window: {e:#}");
                cx.quit();
                return;
            }
        };
        cx.set_global(tray::WindowHolder(window));

        if minimized {
            // Give the compositor a beat to map the window before asking it
            // to minimize; a same-frame request can be dropped.
            cx.spawn(async move |cx| {
                cx.background_executor()
                    .timer(Duration::from_millis(400))
                    .await;
                let _ = cx.update(|cx| {
                    let _ = window.update(cx, |_, window, _| window.minimize_window());
                });
            })
            .detach();
        }

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

        // Later launches ask this instance to surface the dashboard.
        let mut show_events = show_events;
        cx.spawn(async move |cx| {
            let _guard = instance_guard;
            while show_events.recv().await.is_some() {
                if cx.update(|cx| tray::show_dashboard(cx, None)).is_err() {
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
        None => CliAction::Run { minimized: false },
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
                       --minimized  Start minimized to the tray (used at login)\n  \
                       --headless   Start without a window (smoke test)\n\n\
                 Environment:\n  \
                   PDS_SYNC_ROOT     Sync folder (default: ~/ProtonDrive)\n  \
                   PDS_SIDECAR_CMD   Sidecar command override\n  \
                   RUST_LOG          Log filter (default: info,pds_core=debug)",
                env!("CARGO_PKG_VERSION")
            );
            CliAction::Exit
        }
        Some("--minimized") => CliAction::Run { minimized: true },
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
    Application::headless().run(|cx: &mut App| {
        cx.spawn(async move |cx| {
            cx.background_executor()
                .timer(Duration::from_millis(250))
                .await;
            let _ = cx.update(|cx| {
                println!("headless proof: event loop OK");
                cx.quit();
            });
        })
        .detach();
    });
}
