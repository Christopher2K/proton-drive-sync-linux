//! `proton-drive-sync` — native Linux desktop client (GPUI).

mod actions;
mod components;
mod engine;
mod platform;
mod state;
mod theme;
mod ui;

use std::time::Duration;

use gpui::{
    App, AppContext, Application, Bounds, TitlebarOptions, WindowBounds, WindowOptions, px, size,
};
use tokio::sync::mpsc;
use tracing::{error, info};

use state::APP_TITLE;
use ui::AppView;

enum CliAction {
    Run,
    Exit,
    Headless,
}

fn main() {
    let action = handle_cli();
    if matches!(action, CliAction::Exit) {
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pds_core=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    if matches!(action, CliAction::Headless)
        || std::env::var_os("PDS_HEADLESS_PROOF").is_some()
    {
        run_headless_proof();
        return;
    }

    let root = platform::sync_root();
    let sidecar_cmd = std::env::var("PDS_SIDECAR_CMD").ok();
    info!(root = %root.display(), "starting {APP_TITLE}");

    let (updates_tx, updates_rx) = mpsc::unbounded_channel();
    let service = engine::start(root.clone(), sidecar_cmd.clone(), updates_tx.clone());

    Application::new().run(move |cx: &mut App| {
        cx.bind_keys(ui::key_bindings());

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

        let root = root.clone();
        let sidecar_cmd = sidecar_cmd.clone();
        let service = service.clone();
        let updates_tx = updates_tx.clone();

        let opened = cx.open_window(options, move |window, cx| {
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

            window.focus(view.read(cx).focus_handle());

            // Closing the window is a graceful quit: veto the close, ask the
            // engine to shut down, and let the app exit when it reports back
            // (with a fallback timer in `request_quit`).
            let weak = view.downgrade();
            window.on_window_should_close(cx, move |_, cx| {
                let _ = weak.update(cx, |view, cx| view.request_quit(cx));
                false
            });

            view
        });

        if let Err(e) = opened {
            error!("could not open the application window: {e:#}");
            cx.quit();
        }
    });
}

/// Handle `--help` / `--version` / `--headless` without starting the GUI.
/// Returns `true` when the process should exit.
fn handle_cli() -> CliAction {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => CliAction::Run,
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
                       --headless   Start without a window (smoke test)\n\n\
                 Environment:\n  \
                   PDS_SYNC_ROOT     Sync folder (default: ~/ProtonDrive)\n  \
                   PDS_SIDECAR_CMD   Sidecar command override\n  \
                   RUST_LOG          Log filter (default: info,pds_core=debug)",
                env!("CARGO_PKG_VERSION")
            );
            CliAction::Exit
        }
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
