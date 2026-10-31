//! StatusNotifierItem tray icon: the always-visible face of the background
//! sync service.
//!
//! The icon lives for the whole process lifetime (the dashboard window is
//! only hidden/minimized, never the source of truth), shows the engine state
//! as a coloured dot, and offers the common actions without opening a
//! window. The tray is best-effort: on a desktop without a StatusNotifier
//! host (GNOME needs an extension) the icon silently never appears — building
//! still succeeds — so `host_available()` probes the bus first and callers
//! degrade gracefully.

use gpui::{App, Global, MenuItem, SharedString, WindowHandle};
use gpui_tray::{Icon, Tray};
use pds_core::EngineState;
use tracing::warn;

use crate::actions::{OpenDashboard, OpenSettings, OpenSyncFolder, Quit, TogglePause};
use crate::state::{APP_TITLE, AppState, Page, Phase};
use crate::ui::AppView;

/// The window the tray re-opens. It is created once at startup; closing it
/// minimizes it instead of quitting, so this handle stays valid for the
/// process lifetime.
pub struct WindowHolder(pub WindowHandle<AppView>);
impl Global for WindowHolder {}

pub struct TrayHolder(pub Tray);
impl Global for TrayHolder {}

/// Whether a tray icon is actually usable. Closing the dashboard only hides
/// it when this is true; otherwise closing quits (there would be no way back
/// to the window).
#[derive(Clone, Copy)]
pub struct TrayAvailable(pub bool);
impl Global for TrayAvailable {}

/// Tray-visible summary of the app state (icons and menu labels).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum StatusKind {
    #[default]
    Starting,
    Running,
    Paused,
    GuardPaused,
    NeedsLogin,
    Failed,
}

impl StatusKind {
    pub fn from_state(state: &AppState) -> Self {
        match state.phase {
            Phase::Starting => StatusKind::Starting,
            Phase::NeedsLogin => StatusKind::NeedsLogin,
            Phase::Failed => StatusKind::Failed,
            Phase::Running => match state.engine_state() {
                Some(EngineState::Paused) => StatusKind::Paused,
                Some(EngineState::GuardPaused) => StatusKind::GuardPaused,
                Some(EngineState::Running) | None => StatusKind::Running,
            },
        }
    }

    fn label(self) -> &'static str {
        match self {
            StatusKind::Starting => "Starting…",
            StatusKind::Running => "Syncing",
            StatusKind::Paused => "Paused",
            StatusKind::GuardPaused => "Paused — delete review needed",
            StatusKind::NeedsLogin => "Sign-in required",
            StatusKind::Failed => "Sync stopped",
        }
    }

    fn color(self) -> u32 {
        match self {
            StatusKind::Running => 0x3fb950,   // green
            StatusKind::Paused => 0xd29922,    // amber
            StatusKind::GuardPaused => 0xf85149, // red
            StatusKind::Failed => 0xf85149,    // red
            StatusKind::Starting => 0x58a6ff,  // blue
            StatusKind::NeedsLogin => 0x8b949e, // grey
        }
    }
}

/// Global read by the dynamic menu builder.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TrayStatus {
    pub kind: StatusKind,
    pub email: Option<String>,
}

impl Global for TrayStatus {}

/// Whether the session bus currently has a `StatusNotifierWatcher` — i.e. a
/// panel that can actually display our icon. Used to decide between starting
/// minimized (tray available) and opening the dashboard at login (no tray:
/// GNOME without the AppIndicator extension, bare compositors).
pub fn host_available() -> bool {
    let check = std::panic::catch_unwind(|| {
        zbus::block_on(async {
            let connection = zbus::Connection::session().await?;
            let reply = connection
                .call_method(
                    Some("org.freedesktop.DBus"),
                    "/org/freedesktop/DBus",
                    Some("org.freedesktop.DBus"),
                    "NameHasOwner",
                    &("org.kde.StatusNotifierWatcher",),
                )
                .await?;
            let owned: bool = reply.body().deserialize()?;
            Ok::<bool, zbus::Error>(owned)
        })
    });
    matches!(check, Ok(Ok(true)))
}

/// Create the tray icon. Returns an error when the desktop has no
/// StatusNotifier host or no session bus; callers log and continue.
pub fn build(cx: &mut App) -> gpui_tray::Result<Tray> {
    let status = current_status(cx);
    cx.set_global(status.clone());

    Tray::builder()
        .icon(status_icon(status.kind))
        .title(APP_TITLE)
        .tooltip(tooltip(&status))
        .menu(menu)
        .on_activate_with(|_token, cx| show_dashboard(cx, None))
        .build(cx)
}

/// Refresh the tray after an engine update. Cheap no-op while the visible
/// status did not change.
pub fn refresh(cx: &mut App, state: &AppState) {
    let next = TrayStatus {
        kind: StatusKind::from_state(state),
        email: state.email.clone(),
    };
    let changed = cx.try_global::<TrayStatus>() != Some(&next);
    cx.set_global(next.clone());
    if !changed {
        return;
    }

    let Some(holder) = cx.try_global::<TrayHolder>() else {
        return;
    };
    let tray = holder.0.clone();
    if let Err(e) = tray.set_icon(status_icon(next.kind), cx) {
        warn!("tray: icon update failed: {e}");
    }
    if let Err(e) = tray.set_tooltip(Some(tooltip(&next)), cx) {
        warn!("tray: tooltip update failed: {e}");
    }
    if let Err(e) = tray.refresh_menu(cx) {
        warn!("tray: menu refresh failed: {e}");
    }
}

/// Show and focus the dashboard. `page` selects a section first.
pub fn show_dashboard(cx: &mut App, page: Option<Page>) {
    let Some(holder) = cx.try_global::<WindowHolder>() else {
        return;
    };
    let handle = holder.0;
    let result = handle.update(cx, |view, window, cx| {
        if let Some(page) = page {
            view.set_page(page, cx);
        }
        window.activate_window();
    });
    if result.is_err() {
        // The window should never be destroyed (close is vetoed), but a
        // compositor/driver failure must not leave the tray dead.
        warn!("tray: dashboard window is unavailable");
    }
}

fn current_status(cx: &App) -> TrayStatus {
    cx.try_global::<WindowHolder>()
        .and_then(|holder| holder.0.read(cx).ok())
        .map(|view| TrayStatus {
            kind: StatusKind::from_state(&view.state),
            email: view.state.email.clone(),
        })
        .unwrap_or_default()
}

fn tooltip(status: &TrayStatus) -> SharedString {
    match &status.email {
        Some(email) => format!("{APP_TITLE} — {} ({email})", status.kind.label()).into(),
        None => format!("{APP_TITLE} — {}", status.kind.label()).into(),
    }
}

fn menu(cx: &mut App) -> Vec<MenuItem> {
    let status = cx.try_global::<TrayStatus>().cloned().unwrap_or_default();
    let mut items = vec![MenuItem::action("Open Dashboard", OpenDashboard)];
    match status.kind {
        StatusKind::Running => items.push(MenuItem::action("Pause syncing", TogglePause)),
        StatusKind::Paused => items.push(MenuItem::action("Resume syncing", TogglePause)),
        StatusKind::GuardPaused => items.push(MenuItem::action(
            "Review paused deletes…",
            OpenDashboard,
        )),
        _ => {}
    }
    items.extend([
        MenuItem::action("Open Sync Folder", OpenSyncFolder),
        MenuItem::action("Preferences…", OpenSettings),
        MenuItem::separator(),
        MenuItem::action(format!("Quit {APP_TITLE}"), Quit),
    ]);
    items
}

// -- actions -------------------------------------------------------------

pub fn on_open_dashboard(_: &OpenDashboard, cx: &mut App) {
    show_dashboard(cx, None);
}

pub fn on_open_settings(_: &OpenSettings, cx: &mut App) {
    show_dashboard(cx, Some(Page::Settings));
}

pub fn on_open_sync_folder(_: &OpenSyncFolder, cx: &mut App) {
    let root = cx
        .try_global::<WindowHolder>()
        .and_then(|holder| holder.0.read(cx).ok())
        .map(|view| view.state.root.clone());
    if let Some(root) = root {
        crate::platform::open_folder(&root);
    }
}

pub fn on_toggle_pause(_: &TogglePause, cx: &mut App) {
    toggle_pause(cx);
}

pub fn on_quit(_: &Quit, cx: &mut App) {
    quit(cx);
}

/// Tray fallbacks for actions whose normal handler lives in the window root.
/// They only run when no window is active (the root handles the action
/// otherwise), so a click never toggles twice.
pub fn toggle_pause(cx: &mut App) {
    if let Some(holder) = cx.try_global::<WindowHolder>() {
        let handle = holder.0;
        let _ = handle.update(cx, |view, _, cx| view.toggle_pause(cx));
    }
}

pub fn quit(cx: &mut App) {
    if let Some(holder) = cx.try_global::<WindowHolder>() {
        let handle = holder.0;
        let _ = handle.update(cx, |view, _, cx| view.request_quit(cx));
    } else {
        cx.quit();
    }
}

// -- icon ----------------------------------------------------------------

const ICON_SIZE: u32 = 32;

/// A neutral sync ring with a status dot in the corner, drawn in code so the
/// tray needs no icon-theme assets.
fn status_icon(kind: StatusKind) -> Icon {
    let mut rgba = vec![0u8; (ICON_SIZE * ICON_SIZE * 4) as usize];
    let dot = kind.color();
    let center = (ICON_SIZE as f32 - 1.0) / 2.0;

    for y in 0..ICON_SIZE {
        for x in 0..ICON_SIZE {
            let fx = x as f32;
            let fy = y as f32;
            let dx = fx - center;
            let dy = fy - center;
            let r = (dx * dx + dy * dy).sqrt();
            let angle = dy.atan2(dx);

            // Ring with two gaps, evoking circular sync arrows.
            let gap = (angle - 0.55).abs() < 0.42 || (angle + 2.35).abs() < 0.42;
            let ring_cov = coverage(r, 10.6, 13.6) * if gap { 0.0 } else { 1.0 };
            if ring_cov > 0.0 {
                blend(&mut rgba, x, y, 0xe6edf3, ring_cov);
            }

            // Status dot (dark rim keeps it readable on light panels).
            let ddx = fx - 23.5;
            let ddy = fy - 23.5;
            let dr = (ddx * ddx + ddy * ddy).sqrt();
            let rim = coverage(dr, 0.0, 7.6);
            if rim > 0.0 {
                blend(&mut rgba, x, y, 0x0d1117, rim);
            }
            let dot_cov = coverage(dr, 0.0, 5.4);
            if dot_cov > 0.0 {
                blend(&mut rgba, x, y, dot, dot_cov);
            }
        }
    }

    Icon::from_rgba(rgba, ICON_SIZE, ICON_SIZE)
        .build()
        .expect("tray icon bitmap is valid")
}

/// Antialiased coverage for a filled disc of `radius`, fading over one pixel.
fn coverage(distance: f32, inner: f32, radius: f32) -> f32 {
    if distance <= inner {
        return 1.0;
    }
    (radius + 0.5 - distance).clamp(0.0, 1.0)
}

/// Source-over blend of an opaque RGB colour at `coverage` alpha.
fn blend(rgba: &mut [u8], x: u32, y: u32, rgb: u32, coverage: f32) {
    let offset = ((y * ICON_SIZE + x) * 4) as usize;
    let alpha = coverage.clamp(0.0, 1.0);
    let src = [
        ((rgb >> 16) & 0xff) as f32,
        ((rgb >> 8) & 0xff) as f32,
        (rgb & 0xff) as f32,
    ];
    let dst_alpha = rgba[offset + 3] as f32 / 255.0;
    let out_alpha = alpha + dst_alpha * (1.0 - alpha);
    if out_alpha <= 0.0 {
        return;
    }
    for channel in 0..3 {
        let dst = rgba[offset + channel] as f32;
        let out = (src[channel] * alpha + dst * dst_alpha * (1.0 - alpha)) / out_alpha;
        rgba[offset + channel] = out.round().clamp(0.0, 255.0) as u8;
    }
    rgba[offset + 3] = (out_alpha * 255.0).round().clamp(0.0, 255.0) as u8;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn running() -> AppState {
        let mut state = AppState::new(PathBuf::from("/tmp/pds"));
        state.phase = Phase::Running;
        state.status = Some(pds_core::EngineStatus {
            state: EngineState::Running,
            mode: pds_core::SyncMode::Placeholders,
            synced_nodes: 3,
            dirty_nodes: 0,
            queued_tombstones: 0,
            errored_nodes: 0,
            conflicted_nodes: 0,
            cache_cap: None,
        });
        state
    }

    #[test]
    fn status_kind_maps_engine_states() {
        let mut state = running();
        assert_eq!(StatusKind::from_state(&state), StatusKind::Running);

        state.status.as_mut().unwrap().state = EngineState::Paused;
        assert_eq!(StatusKind::from_state(&state), StatusKind::Paused);

        state.status.as_mut().unwrap().state = EngineState::GuardPaused;
        assert_eq!(StatusKind::from_state(&state), StatusKind::GuardPaused);

        state.phase = Phase::NeedsLogin;
        assert_eq!(StatusKind::from_state(&state), StatusKind::NeedsLogin);

        state.phase = Phase::Failed;
        assert_eq!(StatusKind::from_state(&state), StatusKind::Failed);
    }

    #[test]
    fn icons_build_for_every_state() {
        for kind in [
            StatusKind::Starting,
            StatusKind::Running,
            StatusKind::Paused,
            StatusKind::GuardPaused,
            StatusKind::NeedsLogin,
            StatusKind::Failed,
        ] {
            status_icon(kind);
        }
    }

    #[test]
    fn tooltip_includes_state_and_account() {
        let status = TrayStatus {
            kind: StatusKind::Paused,
            email: Some("me@example.com".into()),
        };
        assert_eq!(
            tooltip(&status).to_string(),
            "Proton Drive Sync — Paused (me@example.com)"
        );
        let anonymous = TrayStatus {
            kind: StatusKind::Running,
            email: None,
        };
        assert_eq!(tooltip(&anonymous).to_string(), "Proton Drive Sync — Syncing");
    }

    #[test]
    fn status_colors_are_distinct() {
        assert_ne!(StatusKind::Running.color(), StatusKind::Paused.color());
        assert_ne!(StatusKind::Paused.color(), StatusKind::GuardPaused.color());
    }
}
