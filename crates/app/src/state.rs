//! Pure application state: the values the UI renders, the updates the engine
//! sends, and the reducer that applies them. No GPUI or tokio imports, so the
//! whole view model is unit-testable.

use std::path::PathBuf;

use pds_core::{ActivityItem, ActivityKind, EngineState, EngineStatus};

pub use pds_core::SyncMode;

/// Mandated third-party disclosure (SPEC.md §5 / AGENT.md hard rules).
pub const DISCLOSURE: &str =
    "This is a third-party application not officially supported by Proton.";

/// Window title and in-app product name. Clearly unofficial, no Proton branding.
pub const APP_TITLE: &str = "Proton Drive Sync";
pub const APP_TAGLINE: &str = "Unofficial Linux client";

/// Cap on the in-memory activity feed (newest first).
pub const ACTIVITY_CAP: usize = 500;

/// Top-level navigation destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Page {
    #[default]
    Overview,
    Activity,
    Conflicts,
    Settings,
}

impl Page {
    pub const ALL: [Page; 4] = [Page::Overview, Page::Activity, Page::Conflicts, Page::Settings];

    pub fn title(self) -> &'static str {
        match self {
            Page::Overview => "Overview",
            Page::Activity => "Activity",
            Page::Conflicts => "Conflicts",
            Page::Settings => "Settings",
        }
    }

    pub fn subtitle(self) -> &'static str {
        match self {
            Page::Overview => "Your sync status at a glance",
            Page::Activity => "Recent uploads, downloads, and remote changes",
            Page::Conflicts => "Items that need your attention",
            Page::Settings => "Account, folders, and application controls",
        }
    }
}

/// Activity feed filter pills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActivityFilter {
    #[default]
    All,
    Uploads,
    Downloads,
    Deletes,
    Conflicts,
    Problems,
}

impl ActivityFilter {
    pub const ALL: [ActivityFilter; 6] = [
        ActivityFilter::All,
        ActivityFilter::Uploads,
        ActivityFilter::Downloads,
        ActivityFilter::Deletes,
        ActivityFilter::Conflicts,
        ActivityFilter::Problems,
    ];

    pub fn label(self) -> &'static str {
        match self {
            ActivityFilter::All => "All",
            ActivityFilter::Uploads => "Uploads",
            ActivityFilter::Downloads => "Downloads",
            ActivityFilter::Deletes => "Deletes",
            ActivityFilter::Conflicts => "Conflicts",
            ActivityFilter::Problems => "Problems",
        }
    }

    pub fn matches(self, kind: ActivityKind) -> bool {
        match self {
            ActivityFilter::All => true,
            ActivityFilter::Uploads => kind == ActivityKind::Uploaded,
            ActivityFilter::Downloads => kind == ActivityKind::Downloaded,
            ActivityFilter::Deletes => kind == ActivityKind::Trashed,
            ActivityFilter::Conflicts => kind == ActivityKind::Conflict,
            ActivityFilter::Problems => {
                matches!(kind, ActivityKind::UploadErrored | ActivityKind::GuardPaused)
            }
        }
    }
}

/// High-level app phase. Login, onboarding, running, and failure are distinct
/// screens within the single application window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Phase {
    /// Probing the sidecar / starting the engine.
    #[default]
    Starting,
    /// Sidecar reachable but no session — show the sign-in screen.
    NeedsLogin,
    /// Engine running (or paused) — show the main shell.
    Running,
    /// Unrecoverable startup/runtime failure; see [`AppState::fatal`].
    Failed,
}

/// Sign-out progress (destructive → confirm first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SignOutState {
    #[default]
    None,
    /// The confirmation modal is showing.
    Confirming,
    /// Confirmed; engine shutdown + `auth.logout` in flight.
    InProgress,
}

/// Pending sync-mode switch (destructive-ish → confirm first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ModeSwitchState {
    #[default]
    None,
    /// The confirmation modal is showing for `to`.
    Confirming(SyncMode),
    /// Confirmed; the engine restarts into the new mode.
    InProgress(SyncMode),
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

/// Everything the UI renders. Mutated only through [`apply`].
#[derive(Debug, Clone)]
pub struct AppState {
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
    /// Currently visible page (ignored while signing in).
    pub page: Page,
    /// One-time notice: the engine adopted the legacy in-root state DB.
    pub layout_migrated: bool,
    /// This session's mount failure (running materialized instead).
    pub mount_error: Option<String>,
    /// Pending sync-mode switch (confirm modal / restart in flight).
    pub mode_switch: ModeSwitchState,
}

impl AppState {
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
            page: Page::Overview,
            layout_migrated: false,
            mount_error: None,
            mode_switch: ModeSwitchState::None,
        }
    }

    /// Engine run-state, if a status snapshot has arrived.
    pub fn engine_state(&self) -> Option<EngineState> {
        self.status.as_ref().map(|s| s.state)
    }

    /// True when the engine is not actively syncing: no status yet, user
    /// paused, or the delete guard paused it. `GuardPaused` counts as paused
    /// (deletions are held), matching the UI's toggle mapping.
    #[cfg(test)]
    pub fn is_paused(&self) -> bool {
        self.engine_state() != Some(EngineState::Running)
    }

    pub fn account_label(&self) -> String {
        self.email
            .clone()
            .unwrap_or_else(|| "Not signed in".into())
    }

    pub fn open_sign_out(&mut self) {
        if self.phase == Phase::Running {
            self.sign_out = SignOutState::Confirming;
        }
    }

    pub fn cancel_sign_out(&mut self) {
        if self.sign_out == SignOutState::Confirming {
            self.sign_out = SignOutState::None;
        }
    }

    pub fn confirm_sign_out(&mut self) {
        if self.sign_out == SignOutState::Confirming {
            self.sign_out = SignOutState::InProgress;
        }
    }

    /// Open the sync-mode switch confirm modal for `to`.
    pub fn open_mode_switch(&mut self, to: SyncMode) {
        if self.phase == Phase::Running && self.mode_switch == ModeSwitchState::None {
            self.mode_switch = ModeSwitchState::Confirming(to);
        }
    }

    /// Number of entries a filter would show.
    pub fn activity_count(&self, filter: ActivityFilter) -> usize {
        self.activity
            .iter()
            .filter(|item| filter.matches(item.kind))
            .count()
    }

    /// Newest-first entries matching `filter` (cloned for list rendering).
    pub fn filtered_activity(&self, filter: ActivityFilter) -> Vec<ActivityItem> {
        self.activity
            .iter()
            .filter(|item| filter.matches(item.kind))
            .cloned()
            .collect()
    }

    /// Newest `limit` entries of any kind.
    pub fn recent_activity(&self, limit: usize) -> &[ActivityItem] {
        &self.activity[..self.activity.len().min(limit)]
    }

    /// Badge count shown next to the Conflicts nav item.
    pub fn attention_count(&self) -> usize {
        self.conflicts.len() + self.errored.len()
    }
}

/// Engine-runtime → UI updates.
#[derive(Debug, Clone)]
pub enum AppUpdate {
    /// `auth.status` says no session; the sign-in screen is needed.
    AuthRequired,
    /// A session already exists; the engine is starting.
    AuthOk { email: String },
    /// The UI asked to sign in; `auth.loginFork` is in flight.
    LoginStarted,
    /// `auth.signInUrl` notification: display + open in the browser.
    LoginUrl { url: String, user_code: String },
    /// `auth.loginFork` failed; the sign-in screen offers a retry.
    LoginFailed { error: String },
    /// Engine constructed and running.
    EngineStarted,
    /// The engine adopted the pre-0.3 in-root state DB (one-time layout
    /// migration); purely informational.
    LayoutMigrated,
    /// The FUSE placeholder view could not be mounted; this session runs
    /// materialized instead (shown as a Settings notice).
    MountUnavailable { error: String },
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
    /// Sign-out completed: engine stopped, `auth.logout` done.
    SignedOut,
    /// Engine task ended (after a requested shutdown, or unexpectedly).
    Exited { reason: String },
}

/// The state reducer. Pure: no IO, no gpui — unit tests drive this.
pub fn apply(state: &mut AppState, update: &AppUpdate) {
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
        AppUpdate::EngineStarted => {
            state.phase = Phase::Running;
            state.fatal = None;
            state.page = Page::Overview;
        }
        AppUpdate::LayoutMigrated => {
            state.layout_migrated = true;
        }
        AppUpdate::MountUnavailable { error } => {
            state.mount_error = Some(error.clone());
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
            state.sign_out = SignOutState::None;
        }
        AppUpdate::SignedOut => {
            // Back to a pristine signed-out state so another account can sign
            // in immediately. Files on disk and the state DB are kept.
            state.phase = Phase::NeedsLogin;
            state.fatal = None;
            state.email = None;
            state.login = LoginState::Idle;
            state.status = None;
            state.activity.clear();
            state.conflicts.clear();
            state.errored.clear();
            state.sign_out = SignOutState::None;
            state.page = Page::Overview;
            state.mount_error = None;
        }
        AppUpdate::Exited { reason } => {
            if !state.quitting {
                if matches!(state.mode_switch, ModeSwitchState::InProgress(_)) {
                    // Expected stop mid mode-switch: the UI restarts the
                    // engine thread into the new mode (no failure).
                    state.mode_switch = ModeSwitchState::None;
                    state.phase = Phase::Starting;
                } else {
                    state.phase = Phase::Failed;
                    state.fatal = Some(format!("sync engine stopped: {reason}"));
                }
            }
        }
    }
}

/// Prepend an entry, keeping at most `cap` entries (newest first).
pub fn push_activity(list: &mut Vec<ActivityItem>, item: ActivityItem, cap: usize) {
    list.insert(0, item);
    list.truncate(cap);
}

/// Toggle label: pausing is the action while running.
pub fn pause_label(state: &AppState) -> &'static str {
    match state.engine_state() {
        Some(EngineState::Running) => "Pause syncing",
        Some(EngineState::Paused) => "Resume syncing",
        Some(EngineState::GuardPaused) => "Review and resume",
        None => "Starting…",
    }
}

/// Short badge text for the header and sidebar.
pub fn badge(state: &AppState) -> &'static str {
    match state.phase {
        Phase::Starting => "Starting",
        Phase::NeedsLogin => "Sign-in required",
        Phase::Failed => "Stopped",
        Phase::Running => match state.engine_state() {
            Some(EngineState::Running) | None => "Syncing",
            Some(EngineState::Paused) => "Paused",
            Some(EngineState::GuardPaused) => "Delete guard",
        },
    }
}

/// Feed label for one activity entry.
pub fn kind_label(kind: ActivityKind) -> &'static str {
    match kind {
        ActivityKind::Uploaded => "Uploaded",
        ActivityKind::Downloaded => "Downloaded",
        ActivityKind::Trashed => "Trashed",
        ActivityKind::Conflict => "Conflict",
        ActivityKind::GuardPaused => "Guard paused",
        ActivityKind::UploadErrored => "Upload error",
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
            mode: SyncMode::Placeholders,
            cache_cap: Some(10 * 1024 * 1024 * 1024),
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
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
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
            &AppUpdate::AuthOk {
                email: "a@b.c".into(),
            },
        );
        assert_eq!(s.email.as_deref(), Some("a@b.c"));

        apply(&mut s, &AppUpdate::EngineStarted);
        assert_eq!(s.phase, Phase::Running);
    }

    #[test]
    fn login_failure_allows_retry() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        apply(&mut s, &AppUpdate::AuthRequired);
        apply(&mut s, &AppUpdate::LoginStarted);
        apply(
            &mut s,
            &AppUpdate::LoginFailed {
                error: "denied".into(),
            },
        );
        assert_eq!(s.login, LoginState::Failed("denied".into()));
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
    fn activity_filters_narrow_the_feed() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        for (kind, path) in [
            (ActivityKind::Uploaded, "up"),
            (ActivityKind::Downloaded, "down"),
            (ActivityKind::Trashed, "gone"),
            (ActivityKind::Conflict, "clash"),
            (ActivityKind::UploadErrored, "bad"),
        ] {
            apply(&mut s, &AppUpdate::Activity(item(kind, path)));
        }

        assert_eq!(s.activity_count(ActivityFilter::All), 5);
        assert_eq!(s.activity_count(ActivityFilter::Uploads), 1);
        assert_eq!(s.activity_count(ActivityFilter::Downloads), 1);
        assert_eq!(s.activity_count(ActivityFilter::Deletes), 1);
        assert_eq!(s.activity_count(ActivityFilter::Conflicts), 1);
        assert_eq!(s.activity_count(ActivityFilter::Problems), 1);
        assert_eq!(s.filtered_activity(ActivityFilter::Conflicts)[0].path, "clash");
    }

    #[test]
    fn pause_label_flips_with_engine_state() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        assert_eq!(pause_label(&s), "Starting…");

        apply(&mut s, &AppUpdate::Status(status(EngineState::Running)));
        assert_eq!(pause_label(&s), "Pause syncing");
        assert!(!s.is_paused());

        apply(&mut s, &AppUpdate::Status(status(EngineState::Paused)));
        assert_eq!(pause_label(&s), "Resume syncing");
        assert!(s.is_paused());

        apply(&mut s, &AppUpdate::Status(status(EngineState::GuardPaused)));
        assert_eq!(pause_label(&s), "Review and resume");
        assert!(s.is_paused());
    }

    #[test]
    fn badge_reflects_guard_pause() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        apply(&mut s, &AppUpdate::EngineStarted);
        apply(&mut s, &AppUpdate::Status(status(EngineState::GuardPaused)));
        assert_eq!(badge(&s), "Delete guard");
    }

    #[test]
    fn unexpected_exit_is_fatal_but_requested_quit_is_not() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        apply(
            &mut s,
            &AppUpdate::Exited {
                reason: "sidecar died".into(),
            },
        );
        assert_eq!(s.phase, Phase::Failed);
        assert!(s.fatal.as_deref().unwrap().contains("sidecar died"));

        let mut s = AppState::new(PathBuf::from("/tmp/root"));
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
    fn mode_switch_exit_is_an_expected_restart_not_a_failure() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        apply(&mut s, &AppUpdate::EngineStarted);
        s.open_mode_switch(SyncMode::Mirror);
        assert!(matches!(s.mode_switch, ModeSwitchState::Confirming(SyncMode::Mirror)));
        s.mode_switch = ModeSwitchState::InProgress(SyncMode::Mirror);

        apply(
            &mut s,
            &AppUpdate::Exited {
                reason: "shutdown".into(),
            },
        );
        assert_eq!(s.phase, Phase::Starting);
        assert_eq!(s.mode_switch, ModeSwitchState::None);
        assert!(s.fatal.is_none());
    }

    #[test]
    fn sign_out_flow_returns_to_login() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        apply(&mut s, &AppUpdate::EngineStarted);
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

        s.open_sign_out();
        assert_eq!(s.sign_out, SignOutState::Confirming);
        s.cancel_sign_out();
        assert_eq!(s.sign_out, SignOutState::None);

        s.open_sign_out();
        s.confirm_sign_out();
        assert_eq!(s.sign_out, SignOutState::InProgress);
        assert_eq!(s.phase, Phase::Running);

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
    fn open_sign_out_requires_running_phase() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        s.open_sign_out();
        assert_eq!(s.sign_out, SignOutState::None);
        s.confirm_sign_out();
        assert_eq!(s.sign_out, SignOutState::None);
    }

    #[test]
    fn inbox_replaces_conflicts_and_errors() {
        let mut s = AppState::new(PathBuf::from("/tmp/root"));
        apply(
            &mut s,
            &AppUpdate::Inbox {
                conflicts: vec!["a.txt".into()],
                errored: vec![("b.bin".into(), "boom".into())],
            },
        );
        assert_eq!(s.conflicts, ["a.txt"]);
        assert_eq!(s.attention_count(), 2);
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
