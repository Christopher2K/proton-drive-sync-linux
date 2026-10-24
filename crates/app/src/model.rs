//! Pure view-model for the menu-bar shell: the state the UI renders, the
//! updates the engine runtime sends, the commands the UI sends back, and
//! the reducer applying updates to state. No gpui imports — this module is
//! deliberately unit-testable.

use std::path::PathBuf;

use pds_core::{ActivityItem, ActivityKind, EngineState, EngineStatus};

/// Mandated third-party disclosure (SPEC.md §5 / AGENT.md hard rules).
pub const DISCLOSURE: &str =
    "This is a third-party application not officially supported by Proton.";

/// Window title / tray tooltip. Clearly unofficial, no Proton branding.
pub const APP_TITLE: &str = "Proton Drive Sync (unofficial)";

/// Cap on the in-memory activity feed (newest first).
pub const ACTIVITY_CAP: usize = 500;

/// High-level app phase, driving which window the shell shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Phase {
    /// Probing the sidecar / starting the engine.
    #[default]
    Starting,
    /// Sidecar reachable but no session — show the login window.
    NeedsLogin,
    /// Engine running (or paused) — show the status window.
    Running,
    /// Unrecoverable startup/runtime failure; see `ModelState::fatal`.
    Failed,
}

/// Sign-out progress (SPEC: destructive → confirm first). Local UI intent
/// until `InProgress`, when the runtime performs shutdown + `auth.logout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SignOutState {
    #[default]
    None,
    /// The confirm panel is showing in the status window.
    Confirming,
    /// Confirmed; engine shutdown + `auth.logout` in flight.
    InProgress,
}

/// Fork-login progress, driven by sidecar traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginState {
    /// Not attempted yet.
    Idle,
    /// `auth.loginFork` in flight, sign-in URL not yet received.
    Waiting,
    /// The sidecar sent `auth.signInUrl`; finish in the browser.
    Url { url: String, user_code: String },
    /// `auth.loginFork` returned an error; the user may retry.
    Failed(String),
}

/// Everything the windows render. Owned by the `AppModel` entity; mutated
/// only through [`apply`].
#[derive(Debug, Clone)]
pub struct ModelState {
    pub phase: Phase,
    /// Error text when `phase == Phase::Failed`.
    pub fatal: Option<String>,
    pub email: Option<String>,
    pub login: LoginState,
    pub status: Option<EngineStatus>,
    /// Newest-first activity feed from the engine.
    pub activity: Vec<ActivityItem>,
    /// Paths parked as remote-conflicted, awaiting reconciliation (F7).
    pub conflicts: Vec<String>,
    /// (path, last error) of terminally failed uploads.
    pub errored: Vec<(String, String)>,
    pub root: PathBuf,
    /// Set once Quit was requested; the runtime shuts the engine down.
    pub quitting: bool,
    /// Sign-out confirm/in-progress state.
    pub sign_out: SignOutState,
}

impl ModelState {
    pub fn new(root: PathBuf) -> Self {
        Self {
            phase: Phase::Starting,
            fatal: None,
            email: None,
            login: LoginState::Idle,
            status: None,
            activity: Vec::new(),
            conflicts: Vec::new(),
            errored: Vec::new(),
            root,
            quitting: false,
            sign_out: SignOutState::None,
        }
    }

    /// Engine run-state, if a status snapshot has arrived.
    pub fn engine_state(&self) -> Option<EngineState> {
        self.status.as_ref().map(|s| s.state)
    }

    /// Open the sign-out confirm panel (only meaningful while running).
    pub fn begin_sign_out(&mut self) {
        if self.phase == Phase::Running {
            self.sign_out = SignOutState::Confirming;
        }
    }

    /// Dismiss the confirm panel without signing out.
    pub fn cancel_sign_out(&mut self) {
        if self.sign_out == SignOutState::Confirming {
            self.sign_out = SignOutState::None;
        }
    }

    /// Confirm: the caller follows up with `UiCommand::SignOut`.
    pub fn confirm_sign_out(&mut self) {
        if self.sign_out == SignOutState::Confirming {
            self.sign_out = SignOutState::InProgress;
        }
    }
}

/// Engine-runtime → UI updates (tokio side sends, `AppModel` pump applies).
#[derive(Debug, Clone)]
pub enum AppUpdate {
    /// `auth.status` says no session; the login window is needed.
    AuthRequired,
    /// A session already exists; the engine is starting.
    AuthOk { email: String },
    /// The UI asked to sign in; `auth.loginFork` is in flight.
    LoginStarted,
    /// `auth.signInUrl` notification: display + open in the browser.
    LoginUrl { url: String, user_code: String },
    /// `auth.loginFork` failed; the login view offers a retry.
    LoginFailed { error: String },
    /// Engine constructed and running.
    EngineStarted { email: String },
    /// Periodic engine status snapshot.
    Status(EngineStatus),
    /// One activity-feed entry from the engine.
    Activity(ActivityItem),
    /// Periodic DB scan: remote-conflicted paths + errored uploads.
    Inbox {
        conflicts: Vec<String>,
        errored: Vec<(String, String)>,
    },
    /// Unrecoverable runtime failure (sidecar spawn, auth probe, …).
    Failed { error: String },
    /// Sign-out completed: engine stopped, `auth.logout` done (or the
    /// session was already gone — 1001 is treated as success).
    SignedOut,
    /// Engine task ended (after a requested shutdown, or unexpectedly).
    Exited { reason: String },
}

/// UI → engine-runtime commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiCommand {
    StartLogin,
    Pause,
    Resume,
    /// Confirmed sign-out: stop the engine, `auth.logout`, respawn the
    /// sidecar into the login flow.
    SignOut,
    Shutdown,
}

/// The state reducer. Pure: no IO, no gpui — unit tests drive this.
pub fn apply(state: &mut ModelState, update: &AppUpdate) {
    match update {
        AppUpdate::AuthRequired => {
            state.phase = Phase::NeedsLogin;
            state.login = LoginState::Idle;
        }
        AppUpdate::AuthOk { email } => {
            state.email = Some(email.clone());
        }
        AppUpdate::LoginStarted => {
            state.login = LoginState::Waiting;
        }
        AppUpdate::LoginUrl { url, user_code } => {
            state.login = LoginState::Url {
                url: url.clone(),
                user_code: user_code.clone(),
            };
        }
        AppUpdate::LoginFailed { error } => {
            state.login = LoginState::Failed(error.clone());
        }
        AppUpdate::EngineStarted { email } => {
            state.email = Some(email.clone());
            state.phase = Phase::Running;
        }
        AppUpdate::Status(status) => {
            state.status = Some(status.clone());
        }
        AppUpdate::Activity(item) => {
            push_activity(&mut state.activity, item.clone(), ACTIVITY_CAP);
        }
        AppUpdate::Inbox { conflicts, errored } => {
            state.conflicts = conflicts.clone();
            state.errored = errored.clone();
        }
        AppUpdate::Failed { error } => {
            state.phase = Phase::Failed;
            state.fatal = Some(error.clone());
        }
        AppUpdate::SignedOut => {
            // Back to a pristine signed-out state; the login window opens
            // so a different account can sign in immediately.
            state.phase = Phase::NeedsLogin;
            state.fatal = None;
            state.email = None;
            state.login = LoginState::Idle;
            state.status = None;
            state.activity.clear();
            state.conflicts.clear();
            state.errored.clear();
            state.sign_out = SignOutState::None;
        }
        AppUpdate::Exited { reason } => {
            // An exit during a requested quit is normal; anything else is
            // surfaced as a failure so the shell doesn't look alive-but-dead.
            if !state.quitting {
                state.phase = Phase::Failed;
                state.fatal = Some(format!("sync engine stopped: {reason}"));
            }
        }
    }
}

/// Prepend an entry, keeping at most `cap` entries (newest first).
pub fn push_activity(list: &mut Vec<ActivityItem>, item: ActivityItem, cap: usize) {
    list.insert(0, item);
    list.truncate(cap);
}

/// Tray menu / big-toggle label: pausing is the action while running.
pub fn pause_label(state: &ModelState) -> &'static str {
    match state.engine_state() {
        Some(EngineState::Running) => "Pause Syncing",
        _ => "Resume Syncing",
    }
}

/// The command the big toggle / tray item sends: the inverse of the
/// current run-state.
pub fn toggle_command(state: &ModelState) -> UiCommand {
    match state.engine_state() {
        Some(EngineState::Running) => UiCommand::Pause,
        _ => UiCommand::Resume,
    }
}

/// Short badge text for the header.
pub fn badge(state: &ModelState) -> &'static str {
    match state.phase {
        Phase::Starting => "starting",
        Phase::NeedsLogin => "sign-in required",
        Phase::Failed => "stopped",
        Phase::Running => match state.engine_state() {
            Some(EngineState::Running) | None => "running",
            Some(EngineState::Paused) => "paused",
            Some(EngineState::GuardPaused) => "paused — mass-delete guard",
        },
    }
}

/// Feed label for one activity entry.
pub fn kind_label(kind: ActivityKind) -> &'static str {
    match kind {
        ActivityKind::Uploaded => "uploaded",
        ActivityKind::Downloaded => "downloaded",
        ActivityKind::Trashed => "trashed",
        ActivityKind::Conflict => "conflict",
        ActivityKind::GuardPaused => "guard paused",
        ActivityKind::UploadErrored => "upload error",
    }
}

/// `HH:MM:SS` local time for an activity timestamp (unix seconds).
pub fn format_time(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|t| {
            t.with_timezone(&chrono::Local)
                .format("%H:%M:%S")
                .to_string()
        })
        .unwrap_or_default()
}

/// The sync root: `$PDS_SYNC_ROOT` or `~/ProtonDrive`.
pub fn sync_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("PDS_SYNC_ROOT") {
        return PathBuf::from(dir);
    }
    let home = std::env::var_os("HOME").expect("HOME not set");
    PathBuf::from(home).join("ProtonDrive")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(state: EngineState) -> EngineStatus {
        EngineStatus {
            state,
            synced_nodes: 3,
            dirty_nodes: 1,
            queued_tombstones: 2,
            errored_nodes: 0,
            conflicted_nodes: 0,
        }
    }

    fn item(kind: ActivityKind, path: &str) -> ActivityItem {
        ActivityItem {
            at_unix: 1_700_000_000,
            kind,
            path: path.into(),
            detail: None,
        }
    }

    #[test]
    fn auth_flow_drives_phases() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        assert_eq!(s.phase, Phase::Starting);

        apply(&mut s, &AppUpdate::AuthRequired);
        assert_eq!(s.phase, Phase::NeedsLogin);
        assert_eq!(s.login, LoginState::Idle);

        apply(&mut s, &AppUpdate::LoginStarted);
        assert_eq!(s.login, LoginState::Waiting);

        apply(
            &mut s,
            &AppUpdate::LoginUrl {
                url: "https://example.test/login".into(),
                user_code: "ABCD".into(),
            },
        );
        assert!(matches!(s.login, LoginState::Url { .. }));

        apply(
            &mut s,
            &AppUpdate::EngineStarted {
                email: "a@b.c".into(),
            },
        );
        assert_eq!(s.phase, Phase::Running);
        assert_eq!(s.email.as_deref(), Some("a@b.c"));
    }

    #[test]
    fn login_failure_allows_retry() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        apply(&mut s, &AppUpdate::AuthRequired);
        apply(&mut s, &AppUpdate::LoginStarted);
        apply(
            &mut s,
            &AppUpdate::LoginFailed {
                error: "denied".into(),
            },
        );
        assert_eq!(s.login, LoginState::Failed("denied".into()));
        // Retry goes back to waiting.
        apply(&mut s, &AppUpdate::LoginStarted);
        assert_eq!(s.login, LoginState::Waiting);
    }

    #[test]
    fn activity_feed_is_newest_first_and_capped() {
        let mut feed = Vec::new();
        for i in 0..(ACTIVITY_CAP + 10) {
            push_activity(
                &mut feed,
                item(ActivityKind::Uploaded, &format!("f{i}")),
                ACTIVITY_CAP,
            );
        }
        assert_eq!(feed.len(), ACTIVITY_CAP);
        assert_eq!(feed[0].path, format!("f{}", ACTIVITY_CAP + 9));
        assert_eq!(feed[ACTIVITY_CAP - 1].path, "f10");
    }

    #[test]
    fn pause_label_and_toggle_flip_with_engine_state() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        // No status yet: offer Resume (harmless no-op on a running engine).
        assert_eq!(pause_label(&s), "Resume Syncing");

        apply(&mut s, &AppUpdate::Status(status(EngineState::Running)));
        assert_eq!(pause_label(&s), "Pause Syncing");
        assert_eq!(toggle_command(&s), UiCommand::Pause);

        apply(&mut s, &AppUpdate::Status(status(EngineState::Paused)));
        assert_eq!(pause_label(&s), "Resume Syncing");
        assert_eq!(toggle_command(&s), UiCommand::Resume);
    }

    #[test]
    fn badge_reflects_guard_pause() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        apply(
            &mut s,
            &AppUpdate::EngineStarted {
                email: "a@b.c".into(),
            },
        );
        apply(&mut s, &AppUpdate::Status(status(EngineState::GuardPaused)));
        assert_eq!(badge(&s), "paused — mass-delete guard");
    }

    #[test]
    fn unexpected_exit_is_fatal_but_requested_quit_is_not() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        apply(
            &mut s,
            &AppUpdate::Exited {
                reason: "sidecar died".into(),
            },
        );
        assert_eq!(s.phase, Phase::Failed);
        assert!(s.fatal.as_deref().unwrap().contains("sidecar died"));

        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        s.quitting = true;
        apply(
            &mut s,
            &AppUpdate::Exited {
                reason: "shutdown".into(),
            },
        );
        assert_ne!(s.phase, Phase::Failed);
    }

    #[test]
    fn sign_out_flow_returns_to_login() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        // Signed in and running, with some session state accumulated.
        apply(
            &mut s,
            &AppUpdate::EngineStarted {
                email: "a@b.c".into(),
            },
        );
        apply(&mut s, &AppUpdate::Status(status(EngineState::Running)));
        apply(
            &mut s,
            &AppUpdate::Activity(item(ActivityKind::Uploaded, "f")),
        );
        apply(
            &mut s,
            &AppUpdate::Inbox {
                conflicts: vec!["x".into()],
                errored: vec![("y".into(), "boom".into())],
            },
        );

        // signed-in -> confirming
        s.begin_sign_out();
        assert_eq!(s.sign_out, SignOutState::Confirming);
        // cancel path
        s.cancel_sign_out();
        assert_eq!(s.sign_out, SignOutState::None);
        // confirming -> confirmed (in progress) -> signed out
        s.begin_sign_out();
        s.confirm_sign_out();
        assert_eq!(s.sign_out, SignOutState::InProgress);
        assert_eq!(s.phase, Phase::Running); // engine still up during logout

        apply(&mut s, &AppUpdate::SignedOut);
        assert_eq!(s.phase, Phase::NeedsLogin);
        assert_eq!(s.login, LoginState::Idle);
        assert_eq!(s.sign_out, SignOutState::None);
        assert!(s.email.is_none());
        assert!(s.status.is_none());
        assert!(s.activity.is_empty());
        assert!(s.conflicts.is_empty());
        assert!(s.errored.is_empty());
    }

    #[test]
    fn begin_sign_out_requires_running_phase() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        s.begin_sign_out(); // phase == Starting: nothing to sign out of
        assert_eq!(s.sign_out, SignOutState::None);
        s.confirm_sign_out(); // no-op without Confirming
        assert_eq!(s.sign_out, SignOutState::None);
    }

    #[test]
    fn inbox_replaces_conflicts_and_errors() {
        let mut s = ModelState::new(PathBuf::from("/tmp/root"));
        apply(
            &mut s,
            &AppUpdate::Inbox {
                conflicts: vec!["a.txt".into()],
                errored: vec![("b.bin".into(), "boom".into())],
            },
        );
        assert_eq!(s.conflicts, ["a.txt"]);
        assert_eq!(s.errored.len(), 1);
        apply(
            &mut s,
            &AppUpdate::Inbox {
                conflicts: vec![],
                errored: vec![],
            },
        );
        assert!(s.conflicts.is_empty());
        assert!(s.errored.is_empty());
    }

    #[test]
    fn time_format_is_hms() {
        let t = format_time(1_700_000_000);
        assert_eq!(t.len(), 8);
        assert_eq!(t.chars().filter(|c| *c == ':').count(), 2);
    }
}
