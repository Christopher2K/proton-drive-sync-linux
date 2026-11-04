//! The sync engine (SPEC.md F1–F4, F6, F7).
//!
//! Single-task design: one `run()` loop multiplexes sidecar notifications,
//! debounced filesystem events, and control commands. Transfers are awaited
//! inline (`run_transfer`) with commands still serviced mid-transfer so
//! pause/resume applies to the in-flight transfer (F4).
//!
//! Hard rules enforced here (SPEC.md, AGENT.md):
//! - remote action for local deletes is **trash only**, only for DB rows with
//!   `sync_state = synced` (F3.1), subject to the mass-delete guard (F3.3);
//! - first run: an empty local folder deletes nothing (deletes only ever
//!   originate from watcher events against synced DB rows, and the watcher
//!   starts only after the initial materialization completes);
//! - conflicts are keep-both in both directions (F7).

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{DebouncedEvent, Debouncer, new_debouncer};
use pds_sidecar_client::{
    DriveEvent, EventKind, Node, NodeKind, Notification, SidecarApi, UploadParams,
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::conflict::conflict_name;
use crate::db::{Db, NodeRow, SyncState, Tombstone};
use crate::guard::DeleteGuard;
use crate::mode::SyncMode;
use crate::store::{Store, VNode, WorkRequest};

/// Name of the engine-private directory inside the sync root (DB, socket).
pub const STATE_DIR: &str = ".pds";
/// Backing store for the FUSE placeholder VFS, inside the root but invisible
/// to the sync pipeline (M3): never watched, never uploaded, never trashed.
pub const VFS_BACKING_DIR: &str = ".pds-vfs-backing";
const META_INITIAL_SYNC_DONE: &str = "initial_sync_done";
/// Set to "1" after a mirror-mode materialization pass ran; switching from
/// placeholders to mirror later re-triggers materialization exactly once.
const META_MATERIALIZED_FOR_MODE: &str = "materialized_for_mirror";

/// First backoff delay after a retryable upload failure (F2).
pub const RETRY_BASE_DEFAULT: Duration = Duration::from_secs(1);

/// How long a vanished backing file stays re-queued before its write is
/// dropped: long enough for a simultaneously-queued rename to re-key the
/// write queue, short enough that a real orphan doesn't hot-loop.
const WRITE_VANISH_GRACE: Duration = Duration::from_secs(5);
/// Cap for the exponential upload backoff (F2).
pub const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Consecutive retryable failures after which a node is marked errored and
/// the engine moves on (no hot-looping).
pub const MAX_UPLOAD_RETRIES: i64 = 10;

/// Engine run-state (F4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    Running,
    /// User-paused: queues frozen, watcher keeps buffering.
    Paused,
    /// Auto-paused by the mass-delete guard; explicit resume required (F3.3).
    GuardPaused,
}

impl std::fmt::Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineState::Running => write!(f, "running"),
            EngineState::Paused => write!(f, "paused"),
            EngineState::GuardPaused => write!(f, "guard-paused"),
        }
    }
}

/// Point-in-time status snapshot.
#[derive(Debug, Clone)]
pub struct EngineStatus {
    pub state: EngineState,
    pub synced_nodes: usize,
    pub dirty_nodes: usize,
    pub queued_tombstones: usize,
    /// Nodes whose upload failed terminally (non-retryable, or retries
    /// exhausted). Surfaced so the UI/CLI can show them (F2/F7).
    pub errored_nodes: usize,
    /// Dirty nodes parked after a 409 stale-base rejection; they heal via
    /// the next matching remote event (F7).
    pub conflicted_nodes: usize,
    /// How local file content is stored (F5): placeholders (on demand) or
    /// a full materialized mirror.
    pub mode: crate::mode::SyncMode,
    /// Backing-cache cap (placeholder mode): `Some(bytes)` = LRU eviction
    /// past the cap, `None` = unlimited. Live-adjustable via
    /// [`EngineHandle::set_cache_cap`].
    pub cache_cap: Option<u64>,
    /// Bytes the backing cache currently occupies on disk (placeholder
    /// mode). Measured with a directory walk, so it reflects reality even
    /// if the DB is stale. `0` in mirror mode (no backing cache).
    pub cache_used: u64,
    /// Where the backing cache lives (placeholder mode); surfaced so the
    /// UI/CLI can point users at the folder eating their disk. `None` in
    /// mirror mode.
    pub cache_path: Option<PathBuf>,
}

/// Control commands (CLI socket / UI).
pub enum Command {
    Pause,
    Resume,
    Shutdown,
    Status(oneshot::Sender<EngineStatus>),
    /// Keep `rel`'s content in the backing cache (download if missing).
    Pin(String, oneshot::Sender<Result<()>>),
    /// Drop the pin; the file becomes evictable again.
    Unpin(String, oneshot::Sender<Result<()>>),
    /// Change the backing-cache cap live (Settings → cache size).
    /// `None` disables eviction. Applied immediately: if the cache is
    /// already over the new cap, an eviction pass runs on the spot.
    SetEvictionCap(Option<u64>, oneshot::Sender<Result<()>>),
}

/// A queued pin command awaiting its (async) hydration.
struct PinRequest {
    rel: String,
    reply: oneshot::Sender<Result<()>>,
    pin: bool,
}
/// What kind of user-visible thing happened (activity feed, SPEC.md §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    /// Local file uploaded (new node or new revision).
    Uploaded,
    /// Remote file materialized locally.
    Downloaded,
    /// Synced node trashed remotely after a local delete (F3).
    Trashed,
    /// Keep-both conflict: local copy renamed aside (F7), or a revision
    /// upload parked as remote-conflicted awaiting reconciliation.
    Conflict,
    /// Mass-delete guard tripped; engine auto-paused (F3.3).
    GuardPaused,
    /// Upload failed terminally (non-retryable, or retries exhausted).
    UploadErrored,
    /// Download/hydration failed (permissions, no space, transport) and is
    /// waiting for a retry. Existing content is never touched.
    DownloadErrored,
    /// The sidecar could not be reached during startup/initial sync; the
    /// engine stays alive and retries with backoff.
    ConnectionError,
    /// A local item cannot be represented remotely (e.g. a non-UTF-8
    /// filename) and was deliberately left alone (never uploaded/deleted).
    Skipped,
}

/// A user-facing activity entry emitted by the engine. Broadcast to
/// subscribers (the M2 desktop shell); the CLI ignores it. Not persisted —
/// a window opened later sees only new entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityItem {
    /// Unix seconds.
    pub at_unix: i64,
    pub kind: ActivityKind,
    /// Root-relative path the entry concerns.
    pub path: String,
    /// Extra context (error message, conflict copy name, …).
    pub detail: Option<String>,
}

/// Handle to a running engine.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<Command>,
    activity_tx: broadcast::Sender<ActivityItem>,
    /// Placeholder-mode store (VFS hydrate queue + snapshots). `None` in
    /// materialized mode.
    store: Option<Store>,
    /// Flips once the engine attached its filesystem watcher (M4): local
    /// writes made before this are invisible to the sync (no inotify yet).
    /// Tests and embedding UIs synchronize on it before writing files.
    watcher_ready: Arc<AtomicBool>,
}

impl EngineHandle {
    /// The engine↔VFS store, when the engine runs in placeholder mode.
    pub fn store(&self) -> Option<&Store> {
        self.store.as_ref()
    }

    /// Wait (bounded) until the engine's filesystem watcher is attached.
    /// Local writes made before this may never be seen (F2) — inotify
    /// only covers files that exist when the watcher attaches.
    pub async fn wait_for_watcher(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.watcher_ready.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    pub async fn pause(&self) -> Result<()> {
        self.tx.send(Command::Pause).await.context("engine gone")
    }
    pub async fn resume(&self) -> Result<()> {
        self.tx.send(Command::Resume).await.context("engine gone")
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.tx.send(Command::Shutdown).await.context("engine gone")
    }
    pub async fn status(&self) -> Result<EngineStatus> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Status(tx))
            .await
            .context("engine gone")?;
        rx.await.context("engine gone")
    }

    /// Pin a file's content in the backing cache (placeholder mode). The
    /// engine downloads it if not hydrated. Fails on unknown paths and in
    /// materialized mode.
    pub async fn pin(&self, rel: &str) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Pin(rel.to_string(), tx))
            .await
            .context("engine gone")?;
        rx.await.context("engine gone")?
    }

    /// Drop a pin set via [`EngineHandle::pin`]. The file stays hydrated
    /// until the eviction pass picks it.
    pub async fn unpin(&self, rel: &str) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Unpin(rel.to_string(), tx))
            .await
            .context("engine gone")?;
        rx.await.context("engine gone")?
    }

    /// Change the backing-cache cap (placeholder mode): `Some(bytes)` caps
    /// the hydrated cache with LRU eviction, `None` disables eviction.
    /// Takes effect immediately, including an on-the-spot eviction pass
    /// when the cache already exceeds the new cap.
    pub async fn set_cache_cap(&self, cap: Option<u64>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SetEvictionCap(cap, tx))
            .await
            .context("engine gone")?;
        rx.await.context("engine gone")?
    }

    /// Subscribe to the engine's activity feed (uploaded / downloaded /
    /// trashed / conflict / guard / error entries). Lagging receivers skip
    /// ahead (`broadcast::error::RecvError::Lagged`) — the feed is
    /// informational only.
    pub fn subscribe_activity(&self) -> broadcast::Receiver<ActivityItem> {
        self.activity_tx.subscribe()
    }
}

pub struct EngineConfig {
    /// Local sync root folder.
    pub root: PathBuf,
    /// Path of the SQLite state DB (usually `<root>/.pds/state.db`).
    pub db_path: PathBuf,
    /// Base of the exponential upload retry backoff (normally
    /// `RETRY_BASE_DEFAULT`; tests shrink it).
    pub retry_base: Duration,
    /// Placeholder mode (F5): synced files are NOT materialized in `root`;
    /// a FUSE filesystem serves them with on-demand hydration via
    /// [`EngineHandle::store`]. This is the caller's default for NEW roots
    /// only — the persisted `sync_mode` setting (and `$PDS_SYNC_MODE`)
    /// decides for existing ones. See [`SyncMode`](crate::mode::SyncMode).
    pub placeholders: bool,
    /// Backing-cache size cap for placeholder mode (M3.2 eviction). When
    /// hydrated content exceeds this, the least-recently-used files are
    /// dehydrated (backing content deleted; placeholder stays). `None`
    /// disables eviction (tests, explicit opt-out).
    pub eviction_cap: Option<u64>,
    /// Where hydrated content lives. `None` = `<root>/.pds-vfs-backing`
    /// (the `pds mount` layout: mountpoint separate from root). Some path
    /// OUTSIDE the root = the desktop-app layout: the app mounts the FUSE
    /// view AT the root, so engine state (including the backing cache) must
    /// live elsewhere. External backing also enables the one-time local
    /// layout conversion (mirror ⇄ placeholders) in [`Engine::new`].
    pub backing_dir: Option<PathBuf>,
    /// Watch the sync root for local writes (F2). Keep `true` except when
    /// the root is itself the FUSE mountpoint: then every write already
    /// arrives through the VFS store queues, and watching the mount would
    /// double-upload write-through files.
    pub watch_local_writes: bool,
}

impl Default for EngineConfig {
    /// The classic materialized layout: state in `<root>/.pds`, backing in
    /// `<root>/.pds-vfs-backing`, local writes watched. Tests and embedders
    /// override individual fields.
    fn default() -> Self {
        Self {
            root: PathBuf::new(),
            db_path: PathBuf::new(),
            retry_base: RETRY_BASE_DEFAULT,
            placeholders: false,
            eviction_cap: None,
            backing_dir: None,
            watch_local_writes: true,
        }
    }
}

/// Default backing-cache cap: 10 GiB (SPEC F5).
pub const EVICTION_CAP_DEFAULT: u64 = 10 * 1024 * 1024 * 1024;

/// Outcome of a finished transfer relevant to the caller.
struct TransferOutcome {
    node_uid: Option<String>,
}

/// Failure of an awaited transfer. Distinguishes sidecar-reported content
/// errors (which drive the retry / keep-both policy, F2/F7) from local
/// teardown (shutdown, sidecar death), which is propagated.
#[derive(Debug)]
enum TransferFailure {
    /// A `transfer.error` notification from the sidecar.
    Remote {
        code: i64,
        message: String,
        retryable: bool,
        /// Server-driven rate-limit hint (`retryAfter`), honored over the
        /// computed backoff when present.
        retry_after: Option<Duration>,
    },
    /// Local abort: shutdown mid-transfer or the sidecar stream died.
    Aborted(anyhow::Error),
}

impl std::fmt::Display for TransferFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferFailure::Remote {
                code,
                message,
                retryable,
                ..
            } => write!(
                f,
                "transfer error {code}: {message} (retryable={retryable})"
            ),
            TransferFailure::Aborted(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TransferFailure {}

/// True for the 409 "Current revision is no longer up to date" class: the
/// revision's base is stale server-side. The sidecar maps `ApiError(409)` to
/// code 1099 with the API message text (`service.ts` `toRpcError` /
/// `transferFailed` forward no `data`), so we match on the message.
fn is_revision_conflict(code: i64, message: &str) -> bool {
    code == pds_sidecar_client::types::error_codes::CONFLICT
        || message
            .to_ascii_lowercase()
            .contains("no longer up to date")
}

/// True for the "destination name already exists remotely" rename class:
/// the SDK's `NodeWithSameNameExistsValidationError` maps to 1005
/// (`CONFLICT`), while the raw-`ApiError` path surfaces the same text as
/// 1099 (`INTERNAL`; `toRpcError` keeps the message). Retrying cannot
/// heal this alone — the remote name has to free up first — so the store
/// counts these toward its retry cap instead of re-queueing forever.
fn is_rename_name_conflict(e: &anyhow::Error) -> bool {
    use pds_sidecar_client::jsonrpc::Error as Rpc;
    e.chain()
        .find_map(|c| c.downcast_ref::<Rpc>())
        .is_some_and(|rpc| match rpc {
            Rpc::Rpc(r) => {
                r.code == pds_sidecar_client::types::error_codes::CONFLICT
                    || (r.code == pds_sidecar_client::types::error_codes::INTERNAL
                        && r.message.to_ascii_lowercase().contains("already exists"))
            }
            _ => false,
        })
}

pub struct Engine<S: SidecarApi> {
    sidecar: Arc<S>,
    db: Db,
    root: PathBuf,
    /// Placeholder mode (M3): serve synced files via the VFS store instead
    /// of materializing them in `root`.
    placeholders: bool,
    /// Engine ↔ VFS shared store; `Some` iff `placeholders`.
    store: Option<Store>,
    /// Hydrated file contents live here, outside the FUSE-visible root.
    backing_dir: PathBuf,
    /// Backing-cache size cap (placeholder mode). `None` disables eviction.
    eviction_cap: Option<u64>,
    /// The effective sync mode (F5), resolved once at construction
    /// (`$PDS_SYNC_MODE` > persisted `sync_mode` > caller default). The DB
    /// remains the source for FUTURE runs; this field answers "what mode am
    /// I running in" including session env overrides.
    mode: SyncMode,
    /// Watch the sync root (F2). Off when the root IS the FUSE mountpoint.
    watch_local_writes: bool,
    notifications: mpsc::Receiver<Notification>,
    backlog: VecDeque<Notification>,
    cmd_rx: mpsc::Receiver<Command>,
    state: EngineState,
    guard: DeleteGuard,
    /// Remote events received while paused (F4: cursor kept, applied later).
    buffered_events: VecDeque<DriveEvent>,
    scope_id: Option<String>,
    /// Tombstone paths (`local_path`) the user approved with an explicit
    /// resume after a guard pause (e2e-m1 bug #2). The guard's count/ratio
    /// limbs ignore these; only *new* deletes beyond the approved set can
    /// re-trip it. In-memory only — reset on engine restart is acceptable
    /// because the tombstones themselves are durable.
    approved_tombstones: HashSet<String>,
    /// Earliest time each path may be retried after a retryable upload
    /// failure (F2 backoff). In-memory; the failure COUNT is durable in the
    /// DB so restarts don't reset the give-up budget.
    retry_at: HashMap<String, Instant>,
    /// Writes whose backing file vanished, with the first-seen time: after
    /// a short grace (a rename may be re-keying the write queue) they are
    /// dropped instead of hot-looping "backing file vanished" retries.
    vanished_writes: HashMap<String, Instant>,
    /// Base delay for the exponential backoff (`EngineConfig::retry_base`).
    retry_base: Duration,
    /// In-flight transfer currently being awaited (for pause/resume).
    active_transfer: Option<String>,
    /// Pin commands queued from the control socket, awaiting async drain.
    pending_pins: VecDeque<PinRequest>,
    /// Set when Shutdown arrived mid-transfer; the main loop exits ASAP.
    shutting_down: bool,
    /// Set once `run()` has been entered.
    started: AtomicBool,
    /// Set once the filesystem watcher is attached (M4): local writes made
    /// before this are invisible to the engine (no inotify coverage yet).
    /// Shared with the `EngineHandle` so tests/UIs can synchronize on it.
    watcher_ready: Arc<AtomicBool>,
    /// Activity feed broadcast (UI); sends are no-ops without receivers.
    activity_tx: broadcast::Sender<ActivityItem>,
    /// Raw relative paths of non-UTF-8 items already surfaced in the
    /// activity feed — the warning is emitted once per path, not once per
    /// watcher batch.
    non_utf8_reported: HashSet<Vec<u8>>,
    /// `(device, inode)` of every tracked local directory, keyed by relative
    /// path. `notify`'s debounced API only reports paths, so a local folder
    /// rename arrives as "old path gone + new path appeared". The inode is
    /// stable across the rename and lets [`Engine::detect_dir_renames`] pair
    /// the two without guessing from names/content.
    dir_ids: HashMap<String, (u64, u64)>,
    /// The same identity map for tracked files. A case-only file rename on a
    /// case-sensitive filesystem also arrives as delete + create; the stable
    /// inode lets [`Engine::detect_case_only_file_renames`] preserve the node
    /// instead of trashing it and re-uploading as a new one (TG-12).
    file_ids: HashMap<String, (u64, u64)>,
}

impl<S: SidecarApi + 'static> Engine<S> {
    pub fn new(
        sidecar: Arc<S>,
        notifications: mpsc::Receiver<Notification>,
        mut config: EngineConfig,
    ) -> Result<(Self, EngineHandle)> {
        std::fs::create_dir_all(&config.root)?;
        let db = Db::open(&config.db_path).with_context(|| {
            format!(
                "open state DB {} (if it is corrupt, move it aside to rebuild the sync index; local files are never deleted)",
                config.db_path.display()
            )
        })?;
        let (tx, cmd_rx) = mpsc::channel(64);
        let (activity_tx, _) = broadcast::channel(256);
        // F5 mode resolution (single writer): `$PDS_SYNC_MODE` > persisted
        // `sync_mode` > the caller's `placeholders` flag, which is only the
        // default for NEW roots (stored on first use so later runs agree).
        // Embedding apps pre-read the same value to pick the layout; both
        // sides must resolve identically.
        let placeholders = crate::mode::apply_mode_setting(
            &db,
            if config.placeholders {
                SyncMode::Placeholders
            } else {
                SyncMode::Mirror
            },
            true,
        )? == SyncMode::Placeholders;
        config.placeholders = placeholders;
        let mode = if placeholders {
            SyncMode::Placeholders
        } else {
            SyncMode::Mirror
        };
        // Backing cache: explicit external path (desktop layout) or the
        // classic in-root dir (`pds mount` layout).
        let external_backing = config.backing_dir.clone();
        let backing_dir = external_backing
            .clone()
            .unwrap_or_else(|| config.root.join(VFS_BACKING_DIR));
        // One-time local layout conversion, only possible with an external
        // backing cache (the desktop layout; see EngineConfig::backing_dir):
        // adopting an old in-root mirror into placeholders, or materializing
        // again after a switch back to mirror.
        if let Some(ext) = &external_backing
            && let Err(e) = convert_layout(&db, &config.root, ext, mode)
        {
            warn!(error = %e, "local layout conversion failed; continuing");
        }
        // The engine and the handle share one store (engine owns a clone).
        let vfs_store = if placeholders {
            Some(Store::new())
        } else {
            None
        };
        let store = vfs_store.clone();
        let watcher_ready = Arc::new(AtomicBool::new(false));
        let engine = Self {
            sidecar,
            db,
            root: config.root.clone(),
            placeholders,
            store,
            backing_dir,
            eviction_cap: config.eviction_cap,
            mode,
            watch_local_writes: config.watch_local_writes,
            notifications,
            backlog: VecDeque::new(),
            cmd_rx,
            state: EngineState::Running,
            guard: DeleteGuard::default(),
            buffered_events: VecDeque::new(),
            approved_tombstones: HashSet::new(),
            retry_at: HashMap::new(),
            vanished_writes: HashMap::new(),
            retry_base: config.retry_base,
            scope_id: None,
            active_transfer: None,
            pending_pins: VecDeque::new(),
            shutting_down: false,
            started: AtomicBool::new(false),
            watcher_ready: watcher_ready.clone(),
            activity_tx: activity_tx.clone(),
            non_utf8_reported: HashSet::new(),
            dir_ids: HashMap::new(),
            file_ids: HashMap::new(),
        };
        Ok((
            engine,
            EngineHandle {
                tx,
                activity_tx,
                store: vfs_store,
                watcher_ready: watcher_ready.clone(),
            },
        ))
    }

    /// The effective sync mode (F5): placeholders (on demand) or a full
    /// materialized mirror. Resolved once at construction (env > persisted
    /// > caller default); the DB stays the source for future runs.
    pub fn sync_mode(&self) -> SyncMode {
        self.mode
    }

    /// Run until shutdown. Consumes the engine.
    pub async fn run(mut self) -> Result<()> {
        self.started.store(true, Ordering::SeqCst);
        // 1. Initial sync (first run only; F1). Offline/transport failures
        // must not kill the run loop (TG-22): report, back off, and retry
        // until the sidecar is reachable. Control commands (status/pause/
        // shutdown) stay serviceable during the backoff.
        let Some(root_node) = self.bootstrap().await? else {
            return Ok(()); // shutdown while offline
        };

        // 2. Resume the event stream from the persisted cursor (F6). Starting
        // the stream can hit the same transport failure; retry it too.
        if !self.start_event_stream_with_retry(&root_node).await? {
            return Ok(()); // shutdown while retrying
        }

        // 3. Crash recovery (M4): transfers interrupted by a previous run
        // leave rows stuck in `Uploading` (never drained: upload_one
        // requires Dirty) and `.pds-part` temps in the backing dir.
        if let Err(e) = self.recover_interrupted_transfers().await {
            warn!(error = %e, "interrupted-transfer recovery failed");
        }

        // 3. Filesystem watcher with ~500 ms debounce (F2) — unless the
        // root IS the FUSE mountpoint (desktop placeholder layout): writes
        // then arrive through the VFS store queues, and watching the mount
        // would surface backing-cache noise and double-upload write-through
        // files. The inert channel never yields (its sender is leaked, not
        // dropped — a closed receiver would spin the select loop).
        let (debouncer, fs_rx) = if self.watch_local_writes {
            let (fs_tx, fs_rx) = mpsc::channel::<Vec<DebouncedEvent>>(256);
            let debouncer = self.start_watcher(fs_tx)?;
            // Only now are local writes guaranteed to be seen (F2); tests
            // synchronize on this.
            self.watcher_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            (Some(debouncer), fs_rx)
        } else {
            self.watcher_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let (_never, fs_rx) = mpsc::channel::<Vec<DebouncedEvent>>(1);
            std::mem::forget(_never);
            (None, fs_rx)
        };
        let mut fs_rx = fs_rx;

        // 4. Placeholder mode: publish the tree snapshot the FUSE filesystem
        // serves, then keep it fresh from the main loop (M3).
        if let Some(store) = &self.store
            && let Err(e) = self.publish_vfs_snapshot(store)
        {
            warn!(error = %e, "initial VFS snapshot failed");
        }

        // 5. Main loop.
        let result = self.main_loop(&mut fs_rx).await;

        drop(debouncer);
        if let Some(scope) = &self.scope_id {
            let _ = self.sidecar.events_stop(scope).await;
        }
        result
    }

    /// Fetch the root and finish first-run setup, retrying transient
    /// transport failures with exponential backoff. Returns `None` when
    /// shutdown arrived while waiting.
    async fn bootstrap(&mut self) -> Result<Option<Node>> {
        let mut attempt = 0;
        loop {
            match self.try_bootstrap_once().await {
                Ok(root) => {
                    if attempt > 0 {
                        info!("sidecar reachable again; continuing startup");
                    }
                    return Ok(Some(root));
                }
                Err(e) => {
                    attempt += 1;
                    let backoff = retry_backoff(self.retry_base, attempt);
                    warn!(
                        attempt,
                        error = %e,
                        backoff = ?backoff,
                        "startup/initial sync failed; retrying"
                    );
                    self.emit_activity(
                        ActivityKind::ConnectionError,
                        "",
                        Some(format!("{e:#}; retrying in {backoff:?}")),
                    );
                    if !self.wait_with_commands(backoff).await? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    async fn try_bootstrap_once(&mut self) -> Result<Node> {
        let root_node = self.sidecar.get_root().await.context("drive.getRoot")?;
        self.scope_id = Some(root_node.tree_event_scope_id.clone());
        self.db.set_meta("root_uid", &root_node.uid)?;
        if self.db.meta(META_INITIAL_SYNC_DONE)? != Some("1".into()) {
            info!(root = %self.root.display(), "initial sync: enumerating remote tree");
            self.initial_sync(&root_node).await?;
            self.db.set_meta(META_INITIAL_SYNC_DONE, "1")?;
            info!("initial sync complete");
        } else if !self.placeholders
            && self.db.meta(META_MATERIALIZED_FOR_MODE)? != Some("1".into())
        {
            // The root synced in placeholder mode before and the user
            // switched to mirror: materialize now (the conversion moved any
            // hydrated copies back; fetch the rest). Cheap check per start.
            info!("mirror mode: materializing files not yet on disk");
            let files = self.remote_files_needing_download()?;
            for (rel, uid) in files {
                self.download_file(&uid, &rel).await?;
            }
        }
        self.db.set_meta(
            META_MATERIALIZED_FOR_MODE,
            if self.placeholders { "0" } else { "1" },
        )?;
        // Remember every existing directory's inode so a later local rename
        // (reported by the watcher as "old gone + new appeared") can be
        // paired without guessing from names or content (TG-06).
        if !self.placeholders {
            self.index_local_inodes();
        }
        Ok(root_node)
    }

    async fn start_event_stream_with_retry(&mut self, root_node: &Node) -> Result<bool> {
        let cursor = self.db.event_cursor(&root_node.tree_event_scope_id)?;
        let mut attempt = 0;
        loop {
            match self
                .sidecar
                .events_start(&root_node.tree_event_scope_id, cursor.as_deref())
                .await
            {
                Ok(()) => {
                    if attempt > 0 {
                        info!("event stream started after retry");
                    }
                    return Ok(true);
                }
                Err(e) => {
                    attempt += 1;
                    let backoff = retry_backoff(self.retry_base, attempt);
                    warn!(
                        attempt,
                        error = %e,
                        backoff = ?backoff,
                        "starting the event stream failed; retrying"
                    );
                    self.emit_activity(
                        ActivityKind::ConnectionError,
                        "",
                        Some(format!("drive.events.start failed: {e}; retrying in {backoff:?}")),
                    );
                    if !self.wait_with_commands(backoff).await? {
                        return Ok(false);
                    }
                }
            }
        }
    }

    /// Sleep for `wait` while still servicing control commands. Returns
    /// `false` when shutdown arrived (the caller should exit cleanly).
    async fn wait_with_commands(&mut self, wait: Duration) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::Shutdown) | None => {
                            self.shutting_down = true;
                            return Ok(false);
                        }
                        Some(c) => self.handle_command(c)?,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => return Ok(true),
            }
        }
    }

    /// Recover transfers a previous run left interrupted (M4 hardening):
    ///
    /// - Rows stuck in `Uploading` go back to `Dirty` so the normal drain
    ///   re-uploads them through the full machinery (F7 precondition,
    ///   retries, pause buffering). A crash AFTER the transfer landed
    ///   remotely but before bookkeeping re-runs `check_remote_head`,
    ///   which keeps BOTH versions instead of silently superseding.
    /// - Rows stuck in `Downloading` are re-fetched and downloaded again
    ///   from the current remote head (TG-17). The final path was never
    ///   renamed into place, so no partial bytes are trusted.
    /// - Stale `*.pds-part` transfer temps (upload scratch in the visible
    ///   root, hydration scratch in the backing dir) are deleted; a
    ///   half-written temp has no consistent content to resume from.
    ///
    /// In placeholder mode recovered rows upload from the backing dir via
    /// `content_root`, same as any other dirty file.
    async fn recover_interrupted_transfers(&mut self) -> Result<()> {
        let stuck = self.db.nodes_with_state(SyncState::Uploading)?;
        let interrupted_downloads = self.db.nodes_with_state(SyncState::Downloading)?;
        for row in stuck {
            // The file must still exist locally; if the user (or a crash
            // during a VFS unlink) removed it, the delete path owns the
            // row — tombstone it like a local delete.
            let abs = to_abs(
                content_root(self.placeholders, &self.root, &self.backing_dir),
                &row.local_path,
            );
            if !abs.is_file() {
                warn!(path = %row.local_path, "recovering upload whose file vanished; queuing delete");
                self.db.add_tombstone(&Tombstone {
                    local_path: row.local_path.clone(),
                    node_uid: row.node_uid.clone(),
                    deleted_at: now_unix(),
                })?;
                self.db.remove_node(&row.local_path)?;
                // Pre-approved: this completes a delete that already
                // happened locally — it must not trip the mass-deletion
                // guard like a fresh user deletion wave would.
                self.approved_tombstones.insert(row.local_path.clone());
                continue;
            }
            warn!(path = %row.local_path, "recovering upload interrupted by a previous run");
            self.db.set_sync_state(&row.local_path, SyncState::Dirty)?;
        }
        // Sweep transfer temps: ours are `*.pds-part` in both content
        // roots (is_temp_file also matches the sidecar's `*.pdstmp-*`, but
        // those die with their own transfer and are not ours to sweep).
        // Temps of nested files (`docs/a.txt.pds-part`) live in subdirs.
        for root_dir in [self.root.clone(), self.backing_dir.clone()] {
            let mut stack = vec![root_dir];
            while let Some(d) = stack.pop() {
                let Ok(entries) = std::fs::read_dir(&d) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                        continue;
                    }
                    if path.to_string_lossy().ends_with(".pds-part") {
                        let _ = std::fs::remove_file(&path);
                        debug!(path = %path.display(), "swept stale transfer temp");
                    }
                }
            }
        }
        // Re-download AFTER the sweep so a stale temp can never be mistaken
        // for the recovered content. `download_file` re-reads the node, so
        // the freshest remote bytes win over the interrupted attempt.
        for row in interrupted_downloads {
            warn!(path = %row.local_path, "recovering download interrupted by a previous run");
            if let Err(e) = self.download_file(&row.node_uid, &row.local_path).await {
                warn!(
                    path = %row.local_path,
                    error = %e,
                    "interrupted download recovery failed; a later event will retry"
                );
            }
        }
        Ok(())
    }

    fn start_watcher(
        &self,
        fs_tx: mpsc::Sender<Vec<DebouncedEvent>>,
    ) -> Result<Debouncer<RecommendedWatcher>> {
        let mut debouncer = new_debouncer(
            Duration::from_millis(500),
            move |res: notify_debouncer_mini::DebounceEventResult| {
                if let Ok(events) = res {
                    let _ = fs_tx.blocking_send(events);
                }
            },
        )
        .context("watcher init")?;
        debouncer
            .watcher()
            .watch(&self.root, RecursiveMode::Recursive)
            .context("watch sync root")?;
        Ok(debouncer)
    }

    /// Push an entry onto the activity feed; no-op when nobody listens.
    fn emit_activity(&self, kind: ActivityKind, path: &str, detail: Option<String>) {
        let _ = self.activity_tx.send(ActivityItem {
            at_unix: now_unix(),
            kind,
            path: path.to_string(),
            detail,
        });
    }

    /// Surface a filename the Drive protocol cannot carry (invalid UTF-8)
    /// and leave it untouched: never uploaded, never deleted. Emitted once
    /// per raw path — watcher batches repeat the same entry otherwise. The
    /// raw bytes are shown lossily plus as hex so the user can find the file.
    fn report_non_utf8(&mut self, raw: &[u8]) {
        if !self.non_utf8_reported.insert(raw.to_vec()) {
            return;
        }
        let lossy = String::from_utf8_lossy(raw);
        let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        warn!(path = %lossy, hex = %hex, "skipping non-UTF-8 filename");
        self.emit_activity(
            ActivityKind::Skipped,
            &lossy,
            Some(format!(
                "non-UTF-8 filename (hex {hex}); left untouched, not uploaded or deleted"
            )),
        );
    }

    /// Why `rel` cannot be materialized in the sync root, if it cannot.
    fn unmaterializable_reason(&self, rel: &str) -> Option<String> {
        path_limit_reason(&self.root, rel)
    }

    /// Surface a remote path the local filesystem cannot represent (over
    /// `PATH_MAX`/`NAME_MAX`) and skip it: no DB row, no transfer, so the
    /// event loop cannot retry it in a storm. The rest of the sync continues.
    fn report_unmaterializable(&mut self, rel: &str, reason: &str) {
        warn!(path = %rel, reason, "remote path cannot be materialized; skipped");
        self.emit_activity(ActivityKind::Skipped, rel, Some(reason.to_string()));
    }

    async fn main_loop(&mut self, fs_rx: &mut mpsc::Receiver<Vec<DebouncedEvent>>) -> Result<()> {
        loop {
            if self.shutting_down {
                return Ok(());
            }
            // Drain queued notifications first (they arrived mid-transfer).
            if let Some(n) = self.backlog.pop_front() {
                self.handle_notification(n).await?;
                continue;
            }
            // Placeholder mode: download requested files (M3 hydration).
            if self.store.is_some() {
                let store = self.store.clone().expect("store checked above");
                // Pins queue downloads too; drain them first so a just-pinned
                // file hydrates before unrelated requests.
                if let Some(req) = self.pending_pins.pop_front() {
                    let rel = req.rel.clone();
                    let result = if req.pin { self.pin_file(&rel).await } else { Ok(()) };
                    let _ = req.reply.send(result);
                    continue;
                }
                // Metadata-only changes (`setattr` mtime/mode): update the
                // row + served snapshot, never the remote (hash unchanged).
                if let Some((rel, mtime, mode)) = store.next_metadata_request() {
                    self.apply_metadata_update(&store, &rel, mtime, mode);
                    continue;
                }
                if let Some(rel) = store.next_hydrate_request() {
                    self.hydrate_one(&store, &rel).await;
                    // Hydration may have pushed the cache over the cap;
                    // the fresh file itself is protected from eviction.
                    self.maybe_evict(&store, &rel);
                    continue;
                }
                // Folders created through the mount with nothing inside
                // them yet: no write-queue entry exists to drag them out
                // (upload_write_one also drains mkdirs, but only for
                // parents of queued writes). Drain them here so a bare
                // mkdir reaches the remote and gets its DB row.
                if let Some(dir) = store.next_mkdir_request() {
                    if let Err(e) = self.ensure_remote_folder_async(&dir).await {
                        warn!(path = %dir, error = %e, "queued mkdir failed; re-queueing");
                        store.requeue_mkdir(&dir);
                    } else if let Err(e) = self.publish_vfs_snapshot(&store) {
                        warn!(error = %e, "post-mkdir VFS snapshot failed");
                    }
                    continue;
                }
                // Deletes drain BEFORE renames and writes: an
                // overwrite-rename (tmp → existing destination) queues a
                // delete of the destination plus a rename onto the now-free
                // path. If the rename ran first, the stale delete would
                // trash the just-renamed node (observed: the destination's
                // DB row vanishing while each save's tmp uploads as a
                // separate remote file). The priority is resolved
                // atomically in the pop: the VFS queues the triple under
                // one lock, and a single pop cannot interleave with it —
                // separate per-queue pops let the write drain before the
                // delete when the triple lands between two pops (observed:
                // v2 uploaded, then `trashed remotely` on the same node).
                match store.next_work_request() {
                    Some(WorkRequest::Delete(rel)) => {
                        if let Err(e) = self.handle_local_delete(&rel).await {
                            warn!(path = %rel, error = %e, "write-through delete failed");
                        }
                        if let Err(e) = self.publish_vfs_snapshot(&store) {
                            warn!(error = %e, "post-delete VFS snapshot failed");
                        }
                        continue;
                    }
                    Some(WorkRequest::Rename(old, new)) => {
                        match self.apply_local_rename(Some(&store), &old, &new).await {
                            Ok(()) => {
                                store.finish_rename(&old, &new, true, false);
                            }
                            Err(e) => {
                                warn!(from = %old, to = %new, error = %e, "write-through rename failed");
                                // A name conflict re-queues a few times and is
                                // then dropped: renames drain before pump_work,
                                // so retrying one forever would starve the very
                                // tombstone drain (dest trash) that frees the
                                // remote name.
                                let permanent = is_rename_name_conflict(&e);
                                if !store.finish_rename(&old, &new, false, permanent) {
                                    error!(
                                        from = %old,
                                        to = %new,
                                        attempts = crate::store::RENAME_MAX_CONFLICT_RETRIES,
                                        "write-through rename dropped after repeated name conflicts; the remote keeps the source name"
                                    );
                                }
                            }
                        }
                        if let Err(e) = self.publish_vfs_snapshot(&store) {
                            warn!(error = %e, "post-rename VFS snapshot failed");
                        }
                        continue;
                    }
                    Some(WorkRequest::Write(rel)) => {
                        self.upload_write_one(&store, &rel).await;
                        continue;
                    }
                    None => {}
                }
            }
            // F4: when running, pump queued work (tombstones → uploads → events).
            if self.state == EngineState::Running && self.pump_work().await? {
                continue;
            }
            // If dirty rows are only waiting out a retry backoff, wake up
            // when the earliest one matures (F2 — no busy loop, no stall).
            let wake = self.next_retry_wake();
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::Shutdown) | None => return Ok(()),
                        Some(c) => self.handle_command(c)?,
                    }
                }
                n = self.notifications.recv() => {
                    match n {
                        Some(n) => self.handle_notification(n).await?,
                        None => return Err(anyhow!("sidecar notification stream closed")),
                    }
                }
                batch = fs_rx.recv() => {
                    if let Some(events) = batch {
                        self.handle_fs_batch(events).await?;
                    }
                }
                _ = async {
                    match &self.store {
                        Some(store) => store.notify.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
                () = async move {
                    match wake {
                        Some(t) => tokio::time::sleep_until(t.into()).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
            }
        }
    }

    // ------------------------------------------------------------------
    // F1 — initial sync
    // ------------------------------------------------------------------

    /// First-run enumeration + materialization per the sync mode (F5):
    /// placeholders register the tree without downloading; mirror downloads
    /// everything. Both upload local-only files (first-run rule: a non-empty
    /// local folder means "upload these", never "delete the remote ones";
    /// an empty folder never deletes anything).
    async fn initial_sync(&mut self, root: &Node) -> Result<()> {
        self.enumerate_remote(root).await?;

        if self.placeholders {
            // M3: nothing to download — the VFS serves placeholders; files
            // hydrate on open. Local-only files still upload (first-run
            // rule: an empty folder never deletes anything). In the
            // mount-at-root layout the app attaches FUSE only after this
            // scan (it waits for the watcher-ready flag), so untracked
            // local files are still visible on the real folder here.
            let local_only = self.scan_local_only().await?;
            for rel in local_only {
                self.upsert_local_file(&rel).await?;
            }
            return Ok(());
        }

        // Mirror: materialize every remote file that is missing or changed.
        let files = self.remote_files_needing_download()?;
        for (rel, uid) in files {
            self.download_file(&uid, &rel).await?;
        }

        // Upload local-only files (a non-empty folder on first run means
        // "upload these", never "delete the remote ones").
        let local_only = self.scan_local_only().await?;
        for rel in local_only {
            self.upsert_local_file(&rel).await?;
        }
        Ok(())
    }

    /// BFS enumeration of the remote tree into the DB (allowed once per
    /// scope, per protocol compliance notes).
    async fn enumerate_remote(&mut self, root: &Node) -> Result<()> {
        let mut stack: Vec<(Node, String)> = vec![(root.clone(), String::new())];
        while let Some((dir, rel_dir)) = stack.pop() {
            let children = self
                .sidecar
                .list_children(&dir.uid)
                .await
                .with_context(|| format!("listChildren({})", dir.uid))?;
            for child in children {
                let rel = if rel_dir.is_empty() {
                    child.name.clone()
                } else {
                    format!("{rel_dir}/{}", child.name)
                };
                self.upsert_from_node(&child, &rel, SyncState::Synced)?;
                if child.is_folder() {
                    if !self.placeholders {
                        // Placeholder mode: the FUSE mount *is* the tree.
                        let abs = to_abs(&self.root, &rel);
                        if let Some(reason) = self.unmaterializable_reason(&rel) {
                            self.report_unmaterializable(&rel, &reason);
                        } else if !abs.exists() {
                            std::fs::create_dir_all(&abs)?;
                        }
                    }
                    stack.push((child, rel));
                }
            }
        }
        Ok(())
    }

    fn upsert_from_node(&self, node: &Node, rel: &str, state: SyncState) -> Result<()> {
        // Mode is local-only (D2): a remote update must not wipe a mode the
        // user set on the local copy.
        let mode = self.db.node_by_path(rel)?.and_then(|r| r.mode);
        self.db.upsert_node(&NodeRow {
            local_path: rel.to_string(),
            node_uid: node.uid.clone(),
            parent_uid: node.parent_uid.clone(),
            name: node.name.clone(),
            kind: node.kind,
            size: node.size,
            sha1: node.sha1.clone(),
            mtime: node.mtime,
            mode,
            sync_state: state,
            pinned: false,
            synced_sha1: node.sha1.clone(),
        })?;
        if state == SyncState::Synced {
            // Reconciled against remote state: any upload parking is stale.
            self.clear_upload_blocks(rel)?;
        }
        Ok(())
    }

    /// Remote files whose local copy is missing or content-different.
    /// Local files that differ get a keep-both conflict rename first (F7).
    /// Mirror mode only (placeholder mode materializes nothing).
    fn remote_files_needing_download(&mut self) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        let rows = self.db.nodes_with_state(SyncState::Synced)?;
        for row in rows.into_iter().filter(|r| r.is_file()) {
            let abs = to_abs(&self.root, &row.local_path);
            if !abs.exists() {
                out.push((row.local_path.clone(), row.node_uid.clone()));
                continue;
            }
            if local_content_diverged(&row, &abs) {
                let conflicted = rename_for_conflict(&self.db, &self.root, &row.local_path)?;
                warn!(
                    path = %row.local_path,
                    conflict = %conflicted,
                    "initial sync: local/remote content differ, keeping both"
                );
                self.emit_activity(
                    ActivityKind::Conflict,
                    &row.local_path,
                    Some(format!("kept both; local copy renamed to {conflicted}")),
                );
            }
            out.push((row.local_path.clone(), row.node_uid.clone()));
        }
        Ok(out)
    }

    /// All local files not present in the DB (recursive scan of the local
    /// folder — local-only, not a remote traversal). Folder chains are
    /// created remotely as a side effect.
    async fn scan_local_only(&mut self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(d) = stack.pop() {
            let mut dirs = Vec::new();
            for entry in std::fs::read_dir(&d)? {
                let path = entry?.path();
                let rel = match to_rel_checked(&self.root, &path) {
                    RelCheck::Valid(rel) => rel,
                    RelCheck::NotUtf8(raw) => {
                        self.report_non_utf8(&raw);
                        continue;
                    }
                    RelCheck::Outside => continue,
                };
                if is_internal_path(&rel) {
                    continue;
                }
                if path.is_dir() {
                    dirs.push((rel, path));
                } else if path.is_file() && self.db.node_by_path(&rel)?.is_none() {
                    out.push(rel);
                }
            }
            for (rel, path) in dirs {
                if self.db.node_by_path(&rel)?.is_none() {
                    self.ensure_remote_folder_async(&rel).await?;
                }
                stack.push(path);
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------------
    // F2 — local watcher → upload queue
    // ------------------------------------------------------------------

    async fn handle_fs_batch(&mut self, events: Vec<DebouncedEvent>) -> Result<()> {
        let mut events = events;
        // A local folder rename/move is not a delete + create (TG-06): pair
        // the inode of a vanished tracked directory with a newly appeared
        // one and re-key the subtree remotely before the delete path can
        // trash it. Replays the rename through the same machinery as a
        // write-through rename.
        for (old, new) in self.detect_dir_renames() {
            match self.apply_local_rename(None, &old, &new).await {
                Ok(()) => {
                    // Events for the old subtree are stale: the rows moved.
                    let prefix = format!("{old}/");
                    events.retain(|ev| match to_rel_checked(&self.root, &ev.path) {
                        RelCheck::Valid(rel) => rel != old && !rel.starts_with(&prefix),
                        _ => true,
                    });
                }
                Err(e) => warn!(from = %old, to = %new, error = %e, "local folder rename failed"),
            }
        }
        // Case-only file renames have the same delete+create shape on a
        // case-sensitive filesystem; pair them by inode before the delete
        // path can trash the node (TG-12).
        for (old, new) in self.detect_case_only_file_renames() {
            match self.apply_local_rename(None, &old, &new).await {
                Ok(()) => {
                    events.retain(|ev| match to_rel_checked(&self.root, &ev.path) {
                        RelCheck::Valid(rel) => rel != old,
                        _ => true,
                    });
                }
                Err(e) => warn!(from = %old, to = %new, error = %e, "local case-only rename failed"),
            }
        }
        // Queue of (rel, is_dir) to process, parent dirs before their
        // children so uploads can rely on the remote folder chain.
        let mut queue: Vec<(String, bool)> = Vec::new();
        let mut seen = HashSet::new();
        for ev in events {
            let path = ev.path;
            let rel = match to_rel_checked(&self.root, &path) {
                RelCheck::Valid(rel) => rel,
                RelCheck::NotUtf8(raw) => {
                    self.report_non_utf8(&raw);
                    continue;
                }
                RelCheck::Outside => continue,
            };
            if rel.is_empty() || is_internal_path(&rel) {
                continue;
            }
            if !seen.insert(rel.clone()) {
                continue; // debounce coalescing within the batch
            }
            let abs = to_abs(&self.root, &rel);
            if is_temp_file(&rel) {
                continue; // our own / the sidecar's transient transfer files
            }
            if abs.is_dir() {
                // A directory that is new to the DB may have been created
                // together with its contents; inotify attached its recursive
                // watch too late and drops the children's events (FSEvents
                // replays them, so this only bites Linux). Sweep the live
                // subtree now; `seen` dedups against the batch's own events.
                let known = self.db.node_by_path(&rel)?.is_some();
                queue.push((rel.clone(), true));
                if !known {
                    self.sweep_new_dir(&rel, &mut seen, &mut queue);
                }
            } else {
                // File, or already gone (the delete path re-checks liveness).
                queue.push((rel, false));
            }
        }
        for (rel, is_dir) in queue {
            let abs = to_abs(&self.root, &rel);
            if is_dir {
                self.ensure_remote_folder_async(&rel).await?;
                self.remember_dir_inode(&rel);
            } else if abs.is_file() {
                self.upsert_local_file(&rel).await?;
                self.remember_file_inode(&rel);
            } else {
                self.handle_local_delete(&rel).await?;
            }
        }
        Ok(())
    }

    /// Remember the identity of every tracked item currently on disk, so
    /// later watcher batches can tell a rename from a delete + create.
    fn index_local_inodes(&mut self) {
        self.dir_ids.clear();
        self.file_ids.clear();
        let mut stack = vec![String::new()];
        while let Some(rel) = stack.pop() {
            let abs = if rel.is_empty() {
                self.root.clone()
            } else {
                to_abs(&self.root, &rel)
            };
            let Ok(entries) = std::fs::read_dir(&abs) else {
                continue;
            };
            for entry in entries.flatten() {
                let child = match to_rel_checked(&self.root, &entry.path()) {
                    RelCheck::Valid(child) => child,
                    RelCheck::NotUtf8(_) | RelCheck::Outside => continue,
                };
                if is_internal_path(&child) {
                    continue;
                }
                // `DirEntry::metadata` does not follow symlinks, so a linked
                // directory is never indexed (and never walked).
                let Ok(meta) = entry.metadata() else {
                    continue;
                };
                if meta.is_dir() {
                    if let Some(id) = dir_identity(&meta) {
                        self.dir_ids.insert(child.clone(), id);
                    }
                    stack.push(child);
                } else if meta.is_file()
                    && let Some(id) = dir_identity(&meta)
                {
                    self.file_ids.insert(child, id);
                }
            }
        }
    }

    fn remember_dir_inode(&mut self, rel: &str) {
        if let Ok(meta) = std::fs::metadata(to_abs(&self.root, rel))
            && meta.is_dir()
            && let Some(id) = dir_identity(&meta)
        {
            self.dir_ids.insert(rel.to_string(), id);
        }
    }

    /// Index the inode of every ancestor of `rel`. A folder created together
    /// with its contents can miss its own watcher event (the recursive watch
    /// is attached after the fact) while the children still arrive; without
    /// this, a later rename of that folder looks like a delete + create and
    /// can trip the mass-deletion guard before the destination event lands.
    fn remember_ancestor_dir_inodes(&mut self, rel: &str) {
        let Some((parent, _)) = rel.rsplit_once('/') else {
            return;
        };
        let mut prefix = String::new();
        for part in parent.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(part);
            if !self.dir_ids.contains_key(&prefix) {
                self.remember_dir_inode(&prefix);
            }
        }
    }

    fn remember_file_inode(&mut self, rel: &str) {
        if let Ok(meta) = std::fs::metadata(to_abs(&self.root, rel))
            && meta.is_file()
            && let Some(id) = dir_identity(&meta)
        {
            self.file_ids.insert(rel.to_string(), id);
        }
    }

    /// Pair tracked files that vanished from disk with a still-untracked
    /// sibling in the same directory whose name differs only by ASCII case
    /// and whose filesystem identity is unchanged. On a case-sensitive
    /// filesystem a case-only rename arrives as delete + create; replaying it
    /// through the rename machinery preserves the node (uid, revisions)
    /// instead of trashing it and uploading a new one (TG-12).
    fn detect_case_only_file_renames(&mut self) -> Vec<(String, String)> {
        let vanished: Vec<(String, (u64, u64))> = self
            .file_ids
            .iter()
            .filter(|(rel, _)| !to_abs(&self.root, rel).is_file())
            .filter(|(rel, _)| {
                self.db
                    .node_by_path(rel)
                    .ok()
                    .flatten()
                    .is_some_and(|row| row.is_file())
            })
            .map(|(rel, id)| (rel.clone(), *id))
            .collect();
        if vanished.is_empty() {
            return Vec::new();
        }
        let mut used = HashSet::new();
        let mut pairs = Vec::new();
        for (old_rel, id) in vanished {
            let (parent, old_name) = match old_rel.rsplit_once('/') {
                Some((parent, name)) => (parent.to_string(), name.to_string()),
                None => (String::new(), old_rel.clone()),
            };
            let parent_abs = if parent.is_empty() {
                self.root.clone()
            } else {
                to_abs(&self.root, &parent)
            };
            let Ok(entries) = std::fs::read_dir(&parent_abs) else {
                continue;
            };
            for entry in entries.flatten() {
                let Ok(name) = entry.file_name().into_string() else {
                    continue;
                };
                if name == old_name || !name.eq_ignore_ascii_case(&old_name) {
                    continue;
                }
                let new_rel = if parent.is_empty() {
                    name
                } else {
                    format!("{parent}/{name}")
                };
                if used.contains(&new_rel)
                    || self.db.node_by_path(&new_rel).ok().flatten().is_some()
                {
                    continue;
                }
                let Ok(meta) = entry.metadata() else {
                    continue;
                };
                if !meta.is_file() || dir_identity(&meta) != Some(id) {
                    continue;
                }
                used.insert(new_rel.clone());
                pairs.push((old_rel.clone(), new_rel));
                break;
            }
        }
        pairs
    }

    /// Pair tracked directories that vanished from disk with newly appeared,
    /// still-untracked directories carrying the same filesystem identity.
    /// Returns `(old_rel, new_rel)` pairs to replay as renames.
    ///
    /// The debouncer can deliver the old path's events one batch BEFORE the
    /// destination's, so this inspects the live filesystem rather than only
    /// the current batch: the rename syscall has already completed by the
    /// time any of its events are delivered.
    fn detect_dir_renames(&mut self) -> Vec<(String, String)> {
        let mut vanished: Vec<(String, (u64, u64))> = self
            .dir_ids
            .iter()
            .filter(|(rel, _)| !to_abs(&self.root, rel).is_dir())
            .filter(|(rel, _)| {
                // Still tracked as a folder: a real user delete leaves the
                // row in place until its tombstone drains.
                self.db
                    .node_by_path(rel)
                    .ok()
                    .flatten()
                    .is_some_and(|row| !row.is_file())
            })
            .map(|(rel, id)| (rel.clone(), *id))
            .collect();
        if vanished.is_empty() {
            return Vec::new();
        }
        // Parents before children: re-keying a parent already moves the
        // whole tracked subtree (rows and directory identities).
        vanished.sort_by_key(|(rel, _)| rel.matches('/').count());

        let appeared = self.scan_untracked_dirs();
        let mut used = HashSet::new();
        let mut matched_old = Vec::new();
        let mut pairs = Vec::new();
        let mut unmatched = Vec::new();
        for (old, id) in vanished {
            if matched_old
                .iter()
                .any(|prefix: &String| old.starts_with(&format!("{prefix}/")))
            {
                continue;
            }
            let candidate = appeared
                .iter()
                .find(|(new, other)| *other == id && !used.contains(new));
            let Some((new, _)) = candidate else {
                unmatched.push(old);
                continue;
            };
            // Inode numbers can be reused after a deletion. Only pair when
            // the directory's direct children still look like the tracked
            // subtree, so an unrelated new folder never adopts the old
            // node's identity.
            if !self.rename_shape_matches(&old, new) {
                unmatched.push(old);
                continue;
            }
            used.insert(new.clone());
            matched_old.push(old.clone());
            pairs.push((old, new.clone()));
        }
        // Nothing new carries these identities: they are real deletions.
        // Drop their stale index entries so a later inode reuse cannot pair
        // an unrelated folder with them.
        for old in unmatched {
            let prefix = format!("{old}/");
            self.dir_ids
                .retain(|rel, _| rel != &old && !rel.starts_with(&prefix));
        }
        pairs
    }

    /// Every existing directory that is not already tracked at that exact
    /// path, with its filesystem identity. Used to locate a rename's
    /// destination even when its watcher event has not arrived yet.
    fn scan_untracked_dirs(&self) -> Vec<(String, (u64, u64))> {
        let mut out = Vec::new();
        let mut stack = vec![String::new()];
        while let Some(rel) = stack.pop() {
            let abs = if rel.is_empty() {
                self.root.clone()
            } else {
                to_abs(&self.root, &rel)
            };
            let Ok(entries) = std::fs::read_dir(&abs) else {
                continue;
            };
            for entry in entries.flatten() {
                let child = match to_rel_checked(&self.root, &entry.path()) {
                    RelCheck::Valid(child) => child,
                    RelCheck::NotUtf8(_) | RelCheck::Outside => continue,
                };
                if is_internal_path(&child) {
                    continue;
                }
                let Ok(meta) = entry.metadata() else {
                    continue;
                };
                if !meta.is_dir() {
                    continue;
                }
                if let Some(id) = dir_identity(&meta)
                    && self.dir_ids.get(&child).copied() != Some(id)
                    && self.db.node_by_path(&child).ok().flatten().is_none()
                {
                    out.push((child.clone(), id));
                }
                // Keep descending: a rename destination may sit inside an
                // already-tracked folder.
                stack.push(child);
            }
        }
        out
    }

    /// True when every tracked direct child of `old_rel` is present in the
    /// directory now living at `new_rel`.
    fn rename_shape_matches(&self, old_rel: &str, new_rel: &str) -> bool {
        let Ok(rows) = self.db.nodes_under(&format!("{old_rel}/%")) else {
            return false;
        };
        let mut disk = HashSet::new();
        if let Ok(entries) = std::fs::read_dir(to_abs(&self.root, new_rel)) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    disk.insert(name.to_string());
                }
            }
        }
        rows.iter().all(|row| {
            match row
                .local_path
                .strip_prefix(old_rel)
                .and_then(|rest| rest.strip_prefix('/'))
            {
                Some(child) if !child.contains('/') => disk.contains(child),
                _ => true,
            }
        })
    }

    /// Collect the live children of `rel` (recursively, parent-first) that
    /// are not already in `seen`. Recovers the inotify recursive-watch race:
    /// contents created in the same breath as their parent folder never
    /// produce watcher events of their own.
    fn sweep_new_dir(
        &mut self,
        rel: &str,
        seen: &mut HashSet<String>,
        queue: &mut Vec<(String, bool)>,
    ) {
        let entries = match std::fs::read_dir(to_abs(&self.root, rel)) {
            Ok(entries) => entries,
            Err(_) => return, // vanished or unreadable: nothing to recover
        };
        for entry in entries.flatten() {
            let child_rel = match to_rel_checked(&self.root, &entry.path()) {
                RelCheck::Valid(rel) => rel,
                RelCheck::NotUtf8(raw) => {
                    self.report_non_utf8(&raw);
                    continue;
                }
                RelCheck::Outside => continue,
            };
            if is_internal_path(&child_rel) {
                continue;
            }
            if is_temp_file(&child_rel) || !seen.insert(child_rel.clone()) {
                continue;
            }
            // DirEntry::metadata does not follow symlinks: linked dirs are
            // not walked (no loops), linked files upload as regular files.
            let is_dir = entry.metadata().map(|m| m.is_dir()).unwrap_or(false);
            queue.push((child_rel.clone(), is_dir));
            if is_dir {
                self.sweep_new_dir(&child_rel, seen, queue);
            }
        }
    }

    /// Earliest backoff expiry among scheduled retries, if any. Stale
    /// entries are harmless (the wake just finds nothing eligible).
    fn next_retry_wake(&self) -> Option<Instant> {
        self.retry_at.values().copied().min()
    }

    /// Create/write-close on a file: hash, then upload (new node or new
    /// revision) or mark dirty while paused (F4 buffering).
    async fn upsert_local_file(&mut self, rel: &str) -> Result<()> {
        self.remember_ancestor_dir_inodes(rel);
        let abs = self.content_path(rel);
        let meta = match std::fs::metadata(&abs) {
            Ok(m) => m,
            Err(_) => return Ok(()), // vanished again — delete path handles it
        };
        let (sha1, size) = match sha1_file(&abs) {
            Ok(v) => v,
            Err(e) => {
                return self.record_local_upload_failure(
                    rel,
                    anyhow::Error::new(e).context(format!("read local file {}", abs.display())),
                );
            }
        };
        let mtime = file_mtime(&meta);
        let mode = file_mode(&meta);
        let name = rel.rsplit('/').next().unwrap_or(rel).to_string();

        let (parent_uid, node_uid, synced_sha1, fresh_content) = match self.db.node_by_path(rel)? {
            Some(row) => {
                // A write that reaches an existing node proves the local file
                // is back: cancel any tombstone queued by a delete that lost
                // the race with this re-create (TG-05). Trashing the node now
                // would destroy the content this write is about to deliver.
                if !row.node_uid.starts_with("pending:") {
                    self.cancel_stale_tombstone(rel, &row.node_uid);
                }
                if row.sync_state == SyncState::Synced && row.sha1.as_deref() == Some(&sha1) {
                    // Content unchanged; a local `chmod` still updates the
                    // persisted mode (D2) without touching the remote.
                    if row.mode != mode {
                        let mut updated = row;
                        updated.mode = mode;
                        self.db.upsert_node(&updated)?;
                    }
                    return Ok(());
                }
                // A fresh local edit resets failure bookkeeping (the user
                // may have fixed the cause); re-hashes of an already-dirty
                // row must NOT clear an errored/backing-off state.
                let fresh =
                    row.sync_state != SyncState::Dirty || row.sha1.as_deref() != Some(&sha1);
                if row.node_uid.starts_with("pending:") {
                    (row.parent_uid.clone(), None, row.synced_sha1.clone(), fresh)
                } else {
                    (
                        row.parent_uid.clone(),
                        Some(row.node_uid.clone()),
                        row.synced_sha1.clone(),
                        fresh,
                    )
                }
            }
            None => {
                // New file: make sure the folder chain exists remotely.
                let parent_rel = rel.rsplit_once('/').map(|(d, _)| d);
                let parent_uid = match parent_rel {
                    Some(d) => Some(self.ensure_remote_folder_async(d).await?),
                    None => self.db_root_uid()?,
                };
                // F7: never silently overwrite a remote sibling.
                let parent = parent_uid.clone().ok_or_else(|| anyhow!("no root uid"))?;
                let available = self.sidecar.get_available_name(&parent, &name).await?;
                if available != name {
                    let new_rel = match rel.rsplit_once('/') {
                        Some((dir, _)) => format!("{dir}/{available}"),
                        None => available,
                    };
                    warn!(path = %rel, renamed = %new_rel, "remote name collision, keeping both");
                    std::fs::rename(&abs, self.content_path(&new_rel))?;
                    return Box::pin(self.upsert_local_file(&new_rel)).await;
                }
                (parent_uid, None, None, false)
            }
        };

        self.db.upsert_node(&NodeRow {
            local_path: rel.to_string(),
            node_uid: node_uid.clone().unwrap_or_else(|| format!("pending:{rel}")),
            parent_uid: parent_uid.clone(),
            name: name.clone(),
            kind: NodeKind::File,
            size: Some(size as i64),
            sha1: Some(sha1.clone()),
            mtime,
            mode,
            sync_state: SyncState::Dirty,
            pinned: false,
            // The dirty rewrite above tracks LOCAL content in `sha1`; keep
            // the last-synced remote head intact for the F7 precondition.
            synced_sha1,
        })?;
        if fresh_content {
            self.db.clear_upload_failures(rel)?;
            self.retry_at.remove(rel);
        }

        if self.state != EngineState::Running {
            debug!(path = %rel, "paused: buffered dirty file");
            return Ok(());
        }
        self.upload_one(rel).await
    }

    /// Upload one dirty node (must exist in DB with content hashes set).
    async fn upload_one(&mut self, rel: &str) -> Result<()> {
        let Some(row) = self.db.node_by_path(rel)? else {
            return Ok(());
        };
        if row.sync_state != SyncState::Dirty || !row.is_file() {
            return Ok(());
        }
        // Terminally errored, parked as remote-conflicted, or waiting out a
        // retry backoff: leave it alone (F2/F7 — no hot loop).
        if self.db.upload_error(rel)?.is_some() || self.db.is_remote_conflicted(rel)? {
            return Ok(());
        }
        if self.retry_at.get(rel).is_some_and(|t| *t > Instant::now()) {
            return Ok(());
        }
        let abs = self.content_path(rel);
        if !abs.is_file() {
            return Ok(()); // vanished before upload; delete path owns it
        }
        // F7 precondition (e2e-m1 bug #1): before uploading a new revision of
        // a previously-synced node, verify the remote head is still what we
        // last synced. The event stream lags (~30 s poll), so the remote edit
        // may not have reached us — ordering alone cannot prevent silently
        // superseding it.
        if !row.node_uid.starts_with("pending:") {
            self.check_remote_head(rel, &row).await?;
            // The check may have re-keyed the row (vanished node) or left it
            // untouched (fast path / conflict downloaded); re-read it.
            let Some(row) = self.db.node_by_path(rel)? else {
                return Ok(());
            };
            if row.sync_state != SyncState::Dirty {
                return Ok(());
            }
            return self.upload_row(rel, &row).await;
        }
        // Pending (never-uploaded) row without a parent uid: the sidecar
        // requires exactly one of parentUid/nodeUid, so resolve the parent
        // now (self-heal for legacy rows that lost it).
        let mut row = row;
        if row.parent_uid.is_none() {
            let parent_rel = rel.rsplit_once('/').map(|(d, _)| d);
            let parent_uid = match parent_rel {
                Some(d) => Some(self.ensure_remote_folder_async(d).await?),
                None => self.db_root_uid()?,
            };
            if let Some(p) = parent_uid {
                row.parent_uid = Some(p);
                self.db.upsert_node(&row)?;
            }
        }
        self.upload_row(rel, &row).await
    }

    /// Precondition for revision uploads: compare the remote head SHA-1
    /// (`drive.getNode`) against the last-synced head in the DB. On mismatch
    /// keep BOTH versions (F7): download the remote head as a conflict copy
    /// (it syncs back up as a new node), then let the caller upload the
    /// local dirty content as the new revision of the original node. Never
    /// silently overwrite in either direction.
    async fn check_remote_head(&mut self, rel: &str, row: &NodeRow) -> Result<()> {
        let node = match self.sidecar.get_node(&row.node_uid).await {
            Ok(n) => n,
            Err(e) => {
                // Node gone remotely (trashed by another client; the event
                // hasn't reached us). Re-upload as a NEW file rather than
                // reviving a trashed node or erroring against the old uid —
                // the local content survives either way.
                warn!(path = %rel, error = %e, "remote node vanished before upload; re-uploading as new file");
                if let Some(mut r) = self.db.node_by_path(rel)? {
                    r.node_uid = format!("pending:{rel}");
                    r.sync_state = SyncState::Dirty;
                    self.db.upsert_node(&r)?;
                }
                return Ok(());
            }
        };
        let head_matches = match (&row.synced_sha1, &node.sha1) {
            (Some(known), Some(remote)) => known == remote,
            // No recorded head (pre-migration row) or the remote doesn't
            // claim one: nothing reliable to compare against — proceed.
            _ => true,
        };
        if head_matches {
            return Ok(());
        }
        let conflict_rel = self
            .download_remote_head_as_conflict(rel, row, &node)
            .await?;
        warn!(
            path = %rel,
            conflict = %conflict_rel,
            "remote head changed while local copy dirty: keeping both"
        );
        Ok(())
    }

    /// Download the current remote head of `node` to
    /// `name (conflict YYYY-MM-DD HHmm).ext` and track it as a new dirty
    /// (pending) node so the copy uploads under its own uid — durable across
    /// restarts, unlike relying on the watcher alone.
    async fn download_remote_head_as_conflict(
        &mut self,
        rel: &str,
        row: &NodeRow,
        node: &Node,
    ) -> Result<String> {
        let conflict_rel = self.fresh_conflict_rel(rel, row).await?;
        let abs = self.content_path(&conflict_rel);
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp_rel = transfer_temp_rel(&conflict_rel);
        let tmp = self.content_path(&tmp_rel);
        let sidecar = Arc::clone(&self.sidecar);
        self.run_transfer(
            "download",
            Some(&conflict_rel),
            sidecar.download(&node.uid, &tmp),
        )
        .await?;
        std::fs::rename(&tmp, &abs)?;
        apply_file_metadata(&abs, node.mtime, None);
        let (sha1, size) = sha1_file(&abs)?;
        self.db.upsert_node(&NodeRow {
            local_path: conflict_rel.clone(),
            node_uid: format!("pending:{conflict_rel}"),
            parent_uid: row.parent_uid.clone().or_else(|| node.parent_uid.clone()),
            name: conflict_rel
                .rsplit('/')
                .next()
                .unwrap_or(&conflict_rel)
                .to_string(),
            kind: NodeKind::File,
            size: Some(size as i64),
            sha1: Some(sha1),
            mtime: node.mtime,
            mode: None,
            sync_state: SyncState::Dirty,
            pinned: false,
            synced_sha1: None,
        })?;
        Ok(conflict_rel)
    }

    /// Pick a conflict path next to `rel`: deduped against disk + DB
    /// locally, then (best effort) against remote sibling names.
    async fn fresh_conflict_rel(&self, rel: &str, row: &NodeRow) -> Result<String> {
        let local = local_free_conflict_rel_in(&self.db, self.content_dir(), rel)?;
        // Remote collision: ask the Drive side for an available name.
        if let Some(parent) = row.parent_uid.clone() {
            let name = local.rsplit('/').next().unwrap_or(&local);
            if let Ok(available) = self.sidecar.get_available_name(&parent, name).await
                && available != name
            {
                let remote_rel = match rel.rsplit_once('/') {
                    Some((dir, _)) => format!("{dir}/{available}"),
                    None => available,
                };
                if !self.content_path(&remote_rel).exists()
                    && self.db.node_by_path(&remote_rel)?.is_none()
                {
                    return Ok(remote_rel);
                }
            }
        }
        Ok(local)
    }

    /// Perform the actual upload transfer for a dirty row.
    async fn upload_row(&mut self, rel: &str, row: &NodeRow) -> Result<()> {
        let abs = self.content_path(rel);
        // Never send a path the sidecar may read as empty: an unreadable
        // local source must not overwrite the remote revision (TG-19).
        if let Err(e) = std::fs::File::open(&abs) {
            return self.record_local_upload_failure(
                rel,
                anyhow::Error::new(e).context(format!("read local file {}", abs.display())),
            );
        }
        let params = upload_params(row, &abs);
        self.db.set_sync_state(rel, SyncState::Uploading)?;
        let sidecar = Arc::clone(&self.sidecar);
        let outcome = self
            .run_transfer("upload", Some(rel), sidecar.upload(&params))
            .await;
        match outcome {
            Ok(done) => self.finish_upload_success(rel, row, done),
            Err(failure) => self.handle_upload_failure(rel, row, failure).await,
        }
    }

    /// Success bookkeeping shared by `upload_row` and the single keep-both
    /// retry: the uploaded content IS the remote head now.
    fn finish_upload_success(
        &mut self,
        rel: &str,
        row: &NodeRow,
        done: TransferOutcome,
    ) -> Result<()> {
        let mut row = self.db.node_by_path(rel)?.unwrap_or_else(|| row.clone());
        if let Some(uid) = done.node_uid {
            row.node_uid = uid;
        }
        row.sync_state = SyncState::Synced;
        row.synced_sha1 = row.sha1.clone();
        self.db.upsert_node(&row)?;
        self.clear_upload_blocks(rel)?;
        self.retry_at.remove(rel);
        info!(path = %rel, "uploaded");
        self.emit_activity(ActivityKind::Uploaded, rel, None);
        Ok(())
    }

    /// F2/F7 upload failure policy (e2e-m1 bug #1 follow-ups):
    /// - 409 "revision no longer up to date" class → park the node as
    ///   remote-conflicted; NO inline retry and NO conflict copy yet (the
    ///   sidecar's drive.getNode serves the SDK MemoryCache, which lags the
    ///   real head — a "fresh head" fetched now can be stale). The next
    ///   matching remote event runs the proven event-application keep-both
    ///   path with genuinely fresh content and clears the flag;
    /// - other non-retryable errors → mark errored immediately (never
    ///   blind-requeued);
    /// - retryable errors → exponential backoff (base·2ⁿ, capped), and after
    ///   MAX_UPLOAD_RETRIES consecutive failures → errored.
    async fn handle_upload_failure(
        &mut self,
        rel: &str,
        row: &NodeRow,
        failure: TransferFailure,
    ) -> Result<()> {
        let (code, message, retryable, retry_after) = match failure {
            TransferFailure::Aborted(e) => {
                // Shutdown/transport teardown — leave dirty for next run.
                if self.db.node_by_path(rel)?.is_some() {
                    self.db.set_sync_state(rel, SyncState::Dirty)?;
                }
                return Err(e);
            }
            TransferFailure::Remote {
                code,
                message,
                retryable,
                retry_after,
            } => (code, message, retryable, retry_after),
        };
        if self.db.node_by_path(rel)?.is_none() {
            return Ok(()); // vanished mid-transfer; delete path owns it
        }
        self.db.set_sync_state(rel, SyncState::Dirty)?;

        // The 409 class: the remote head moved under us (e.g. its event was
        // applied first and updated our synced head, so the getNode
        // precondition saw a false "match" — the precondition covers only
        // the no-event-received window; THIS is the safety net).
        if is_revision_conflict(code, &message) && !row.node_uid.starts_with("pending:") {
            self.db.set_remote_conflicted(rel, true)?;
            warn!(
                path = %rel, error = %message,
                "revision upload rejected (stale base); parked as remote-conflicted, awaiting remote event"
            );
            self.emit_activity(
                ActivityKind::Conflict,
                rel,
                Some("remote changed during upload; awaiting reconciliation".into()),
            );
            return Ok(());
        }

        if !retryable {
            self.db.mark_upload_errored(rel, &message)?;
            self.retry_at.remove(rel);
            warn!(path = %rel, code, error = %message, "non-retryable upload error; node marked errored");
            self.emit_activity(ActivityKind::UploadErrored, rel, Some(message));
            return Ok(());
        }
        let count = self.db.record_retryable_failure(rel)?;
        if count >= MAX_UPLOAD_RETRIES {
            self.db.mark_upload_errored(rel, &message)?;
            self.retry_at.remove(rel);
            warn!(path = %rel, count, error = %message, "upload failed too many times; node marked errored");
            self.emit_activity(ActivityKind::UploadErrored, rel, Some(message));
        } else {
            // Honor the server's Retry-After hint when present (M4);
            // otherwise compute the exponential schedule (F2).
            let backoff = retry_after.unwrap_or_else(|| retry_backoff(self.retry_base, count));
            self.retry_at
                .insert(rel.to_string(), Instant::now() + backoff);
            info!(path = %rel, count, backoff = ?backoff, server_hint = retry_after.is_some(), "retryable upload error; backing off");
        }
        Ok(())
    }

    /// A local source could not be read. Never contact the uploader with it
    /// (an empty/partial read would destroy the remote copy): surface the
    /// error, leave the row dirty-but-parked, and let a fresh edit clear the
    /// failure through the normal `upsert_local_file` path.
    fn record_local_upload_failure(&mut self, rel: &str, error: anyhow::Error) -> Result<()> {
        let message = format!("{error:#}");
        self.db.set_sync_state(rel, SyncState::Dirty)?;
        self.db.mark_upload_errored(rel, &message)?;
        self.retry_at.remove(rel);
        warn!(path = %rel, error = %message, "local upload source unreadable; node marked errored");
        self.emit_activity(ActivityKind::UploadErrored, rel, Some(message));
        Ok(())
    }

    /// Clear everything that parks a node's upload queue (retry bookkeeping
    /// plus the remote-conflicted flag). Called when the node is healed: a
    /// successful upload, or the event-application path reconciling it
    /// against genuinely fresh remote state.
    fn clear_upload_blocks(&self, rel: &str) -> Result<()> {
        self.db.clear_upload_failures(rel)?;
        self.db.set_remote_conflicted(rel, false)?;
        Ok(())
    }

    /// Ensure every component of `rel` (a folder path) exists remotely;
    /// returns the uid of the deepest folder.
    async fn ensure_remote_folder_async(&mut self, rel: &str) -> Result<String> {
        let mut parent_uid = self
            .db_root_uid()?
            .ok_or_else(|| anyhow!("root uid unknown before initial sync"))?;
        let mut prefix = String::new();
        for part in rel.split('/') {
            prefix = if prefix.is_empty() {
                part.to_string()
            } else {
                format!("{prefix}/{part}")
            };
            if let Some(row) = self.db.node_by_path(&prefix)? {
                parent_uid = row.node_uid;
                continue;
            }
            let node = match self.sidecar.create_folder(&parent_uid, part).await {
                Ok(n) => n,
                Err(e) if e.code() == Some(pds_sidecar_client::types::error_codes::CONFLICT) => {
                    // Exists remotely but not in our DB — adopt it.
                    self.sidecar
                        .list_children(&parent_uid)
                        .await?
                        .into_iter()
                        .find(|n| n.name == part && n.is_folder())
                        .ok_or_else(|| anyhow!("folder `{part}` conflict but not found"))?
                }
                Err(e) => return Err(e.into()),
            };
            self.upsert_from_node(&node, &prefix, SyncState::Synced)?;
            parent_uid = node.uid;
        }
        Ok(parent_uid)
    }

    fn db_root_uid(&self) -> Result<Option<String>> {
        Ok(self.db.meta("root_uid")?)
    }

    // ------------------------------------------------------------------
    // F3 — conservative trash
    // ------------------------------------------------------------------

    /// A rename made through the VFS mount. Re-keys the DB rows beneath
    /// the old path and mirrors it remotely as `renameNode` (+ `moveNodes`
    /// when the parent changed) — never a trash + re-upload, so content
    /// and revision history survive. Rows that never synced (no remote
    /// uid) just re-key; their queued upload picks up the new path.
    async fn apply_local_rename(
        &mut self,
        store: Option<&Store>,
        old_rel: &str,
        new_rel: &str,
    ) -> Result<()> {
        // Guard against cycles: renaming a folder into itself is invalid.
        if new_rel.starts_with(&format!("{old_rel}/")) || old_rel == new_rel {
            warn!(from = %old_rel, to = %new_rel, "invalid rename target (into itself)");
            return Ok(());
        }
        let Some(row) = self.db.node_by_path(old_rel)? else {
            // Source vanished (deleted before the queue drained) or never
            // tracked. Nothing remote to rename; if backing content moved
            // on disk the write queue will upload it as a new file.
            debug!(path = %old_rel, "rename of unknown local item: no remote action");
            return Ok(());
        };

        // Overwrite-rename (tmp → existing destination): keep the
        // destination's node. The generic re-key below would move the
        // source's row onto the destination path, clobbering the
        // destination's synced row — the content would upload as a
        // BRAND-NEW node while the overwrite delete's tombstone trashes
        // the destination's real node. The file's identity (node uid,
        // share links, revisions) dies, and a tombstone draining after
        // the upload loses the file entirely. The destination's row wins
        // instead: drop the source row, mark the destination dirty, and
        // let its queued write deliver the content as a revision of the
        // EXISTING node (its overwrite-delete tombstone is cancelled when
        // that revision upload lands — cancel_stale_tombstone).
        //
        // Two source shapes, same outcome:
        // - `pending:` row (PR #22): never synced, no remote node.
        // - synced row: the tmp's upload drained before the rename, so
        //   its remote node exists. Trash it — leaving it behind shows a
        //   `*.tmp` orphan. And a plain renameNode here would 409 while
        //   the destination's overwrite-delete tombstone is still
        //   pending: tombstones drain in pump_work, which the engine only
        //   reaches when the rename queue is empty — the failed rename
        //   re-queues itself instantly and starves it (observed in
        //   production: 66 failed API calls in 36 s).
        if row.is_file()
            && let Some(dest) = self.db.node_by_path(new_rel)?
            && dest.is_file()
        {
            let source_is_remote = !row.node_uid.starts_with("pending:");
            if source_is_remote {
                let results = self
                    .sidecar
                    .trash(std::slice::from_ref(&row.node_uid))
                    .await?;
                if !results.iter().any(|r| r.uid == row.node_uid && r.ok) {
                    // Transport errors retry above, but a per-node refusal
                    // (e.g. already trashed by an earlier attempt) must not
                    // block the save: the orphan is cosmetic, the content
                    // is not.
                    warn!(
                        uid = %row.node_uid,
                        "overwritten source node trash rejected; continuing"
                    );
                }
            }
            self.db.remove_node(old_rel)?;
            self.file_ids.remove(old_rel);
            self.remember_file_inode(new_rel);
            let uid = dest.node_uid.clone();
            self.db.upsert_node(&NodeRow {
                sync_state: SyncState::Dirty,
                ..dest
            })?;
            // The VFS queued the destination write with the
            // overwrite pair; record_write is a dedup'ing no-op in
            // that case and the sole queue source when the racing
            // tmp upload already drained (and dropped its request).
            if let Some(store) = store {
                store.record_write(new_rel);
            }
            info!(
                from = %old_rel,
                to = %new_rel,
                uid = %uid,
                "overwrite rename merged into the existing node"
            );
            return Ok(());
        }

        let new_name = new_rel
            .rsplit('/')
            .next()
            .unwrap_or(new_rel)
            .to_string();
        let new_parent_rel = new_rel.rsplit_once('/').map(|(d, _)| d.to_string());
        let old_parent_rel = old_rel.rsplit_once('/').map(|(d, _)| d.to_string());
        let cross_folder = new_parent_rel != old_parent_rel;

        // Mirror remotely first (while rows still key the old path): a
        // synced node renames in place; an unsynced one only re-keys.
        if !row.node_uid.starts_with("pending:") {
            if cross_folder {
                // New parent: None = root.
                let parent_uid = match &new_parent_rel {
                    Some(p) => self.ensure_remote_folder_async(p).await?,
                    None => self
                        .db_root_uid()?
                        .ok_or_else(|| anyhow!("root uid unknown before initial sync"))?,
                };
                let ok = self.sidecar.move_node(&row.node_uid, &parent_uid).await?;
                if !ok {
                    anyhow::bail!("remote move of {} rejected", row.node_uid);
                }
            }
            if row.name != new_name {
                self.sidecar.rename_node(&row.node_uid, &new_name).await?;
            }
        }

        // Re-key rows: read them all first, delete the old-path rows (the
        // `node_uid` UNIQUE constraint would reject the insert otherwise),
        // then insert with the prefix-swapped paths. Children first so a
        // mid-flight crash leaves an adoptable gap, not a duplicate.
        let like = format!("{old_rel}/%");
        let mut rows: Vec<NodeRow> = self.db.nodes_under(&like)?;
        rows.push(row.clone());
        for r in &rows {
            self.db.remove_node(&r.local_path)?;
        }
        for mut r in rows {
            r.local_path = if r.local_path == old_rel {
                new_rel.to_string()
            } else {
                format!("{new_rel}/{}", &r.local_path[old_rel.len() + 1..])
            };
            if r.local_path == new_rel {
                r.name = new_name.clone();
            }
            self.db.upsert_node(&r)?;
        }
        // Parent uid follows the (possibly new) parent folder row; a move
        // to the root re-anchors to the root uid.
        if cross_folder {
            let parent_uid = match &new_parent_rel {
                Some(p) => self.db.node_by_path(p)?.map(|r| r.node_uid),
                None => self.db_root_uid()?,
            };
            if let (Some(parent_uid), Some(mut r)) = (parent_uid, self.db.node_by_path(new_rel)?) {
                r.parent_uid = Some(parent_uid);
                self.db.upsert_node(&r)?;
            }
        }

        // Move backing content so hydration state and pending uploads
        // follow the rows. `forget(old)` first drops the old-path store
        // entries; hydrated children re-register under the new prefix.
        let from = to_abs(&self.backing_dir, old_rel);
        let to = to_abs(&self.backing_dir, new_rel);
        if from.exists() {
            if let Some(dir) = to.parent() {
                std::fs::create_dir_all(dir).ok();
            }
            std::fs::rename(&from, &to)?;
        }
        if let Some(store) = store {
            store.forget_tree(old_rel);
            let like = format!("{new_rel}/%");
            for child in self.db.nodes_under(&like)? {
                let abs = to_abs(&self.backing_dir, &child.local_path);
                if abs.is_file() && store.backing_path(&child.local_path).is_none() {
                    store.set_backing(&child.local_path, abs);
                }
            }
            let own = to_abs(&self.backing_dir, new_rel);
            if own.is_file() && store.backing_path(new_rel).is_none() {
                store.set_backing(new_rel, own);
            }
        }
        // Filesystem identities follow the rows, so the next rename inside
        // the moved subtree is recognized without re-indexing the root.
        self.rekey_local_inodes(old_rel, new_rel);
        if row.is_file() {
            self.remember_file_inode(new_rel);
        }
        info!(from = %old_rel, to = %new_rel, "rename applied");
        Ok(())
    }

    /// Move tracked `(device, inode)` entries (directories and files) from
    /// the old path prefix to the new one after a local (or mirrored remote)
    /// rename.
    fn rekey_local_inodes(&mut self, old_rel: &str, new_rel: &str) {
        rekey_inode_map(&mut self.dir_ids, old_rel, new_rel);
        rekey_inode_map(&mut self.file_ids, old_rel, new_rel);
    }

    async fn handle_local_delete(&mut self, rel: &str) -> Result<()> {
        // An overwrite-rename (tmp → existing destination) queues this
        // delete AND a rename onto this path. By the time the delete
        // drains, the VFS has already moved the NEW content into the
        // destination's backing file — removing it would destroy the very
        // content the rename is delivering. Skip the local teardown; the
        // tombstone (old node) still proceeds.
        let rename_inbound = self
            .store
            .as_ref()
            .is_some_and(|s| s.rename_inbound(rel));
        let Some(row) = self.db.node_by_path(rel)? else {
            debug!(path = %rel, "delete of unknown local item: no remote action");
            return Ok(());
        };
        // The path no longer holds this tracked inode; the next create/rename
        // registers its own identity.
        self.file_ids.remove(rel);
        if row.sync_state != SyncState::Synced {
            // Never confirmed synced → local removal only (F3.1). An
            // inbound overwrite-rename owns this path: its merge keeps
            // the row (the destination's identity wins) and re-queues the
            // content. Dropping the row here would leave the merge with
            // no destination node — the next rename would upload as a
            // brand-new node and orphan the real one (the back-to-back
            // save case: the previous save's revision upload is still
            // pending, so the destination row is Dirty).
            if !rename_inbound {
                self.db.remove_node(rel)?;
                if let Some(store) = &self.store {
                    store.forget(rel);
                    std::fs::remove_file(to_abs(&self.backing_dir, rel)).ok();
                }
            }
            return Ok(());
        }
        // Durable tombstone first; drained immediately if running (F3.4).
        self.db.add_tombstone(&Tombstone {
            local_path: rel.to_string(),
            node_uid: row.node_uid.clone(),
            deleted_at: now_unix(),
        })?;
        // Write-through: drop queued work and the hydrated copy too.
        if let Some(store) = &self.store
            && !rename_inbound
        {
            store.forget(rel);
            std::fs::remove_file(to_abs(&self.backing_dir, rel)).ok();
        }
        Ok(())
    }

    /// Drain at most one tombstone. Returns false if the guard tripped.
    async fn drain_one_tombstone(&mut self) -> Result<bool> {
        let Some(t) = self.db.tombstones()?.into_iter().next() else {
            return Ok(true);
        };
        // Bug #2: tombstones approved by an explicit resume drain without
        // touching the guard's window; only deletes beyond the approved set
        // count toward the count/percentage limbs.
        let approved = self.approved_tombstones.contains(&t.local_path);
        if !approved {
            let synced_total = self.db.count_synced()?;
            if self.guard.record_and_check(Instant::now(), synced_total) {
                self.state = EngineState::GuardPaused;
                warn!(
                    deletes = self.guard.window_len(),
                    synced_total,
                    "mass-deletion guard tripped: auto-paused, explicit resume required"
                );
                self.emit_activity(ActivityKind::GuardPaused, &t.local_path, None);
                return Ok(false);
            }
        }
        let results = self
            .sidecar
            .trash(std::slice::from_ref(&t.node_uid))
            .await?;
        if results.iter().any(|r| r.uid == t.node_uid && r.ok) {
            info!(path = %t.local_path, uid = %t.node_uid, "trashed remotely");
            self.remove_node_recursive(&t.local_path)?;
            self.db.remove_tombstone(&t.local_path)?;
            self.approved_tombstones.remove(&t.local_path);
            self.emit_activity(ActivityKind::Trashed, &t.local_path, None);
        } else {
            warn!(path = %t.local_path, "trash failed remotely; keeping tombstone");
            return Ok(false);
        }
        Ok(true)
    }

    fn remove_node_recursive(&self, rel: &str) -> Result<()> {
        // Children first (their rows carry the path prefix).
        let conn_rows = {
            let like = format!("{rel}/%");
            let conn = self.db.clone();
            conn.nodes_under(&like)?
        };
        for row in conn_rows {
            self.db.remove_node(&row.local_path)?;
        }
        self.db.remove_node(rel)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // F6 — remote events
    // ------------------------------------------------------------------

    async fn handle_notification(&mut self, n: Notification) -> Result<()> {
        match n {
            Notification::DriveEvent(ev) => {
                if self.state == EngineState::Running {
                    self.apply_event(*ev).await?;
                } else {
                    debug!(event = %ev.event_id, "paused: buffering remote event");
                    self.buffered_events.push_back(*ev);
                }
            }
            Notification::TransferProgress {
                transfer_id,
                bytes,
                total,
            } => {
                debug!(transfer_id, bytes, total, "transfer progress");
            }
            Notification::TransferDone { .. } | Notification::TransferError { .. } => {
                // Stray completion for a transfer we're not awaiting (e.g.
                // cancelled) — ignore; run_transfer consumes its own.
                debug!("ignoring stray transfer notification");
            }
            Notification::AuthSignInUrl { .. } | Notification::Other { .. } => {}
        }
        Ok(())
    }

    async fn apply_event(&mut self, ev: DriveEvent) -> Result<()> {
        debug!(event = %ev.event_id, kind = ?ev.kind, "remote event");
        match ev.kind {
            EventKind::FastForward => {}
            EventKind::NodeCreated | EventKind::NodeUpdated => {
                if let Some(uid) = ev.node_uid.clone() {
                    if ev.is_trashed == Some(true) {
                        self.apply_remote_delete(&uid)?;
                    } else if let Err(e) = self.apply_node_upsert(&uid).await {
                        warn!(uid = %uid, error = %e, "failed to apply node event");
                    }
                }
            }
            EventKind::NodeDeleted | EventKind::TreeRemove => {
                if let Some(uid) = ev.node_uid.clone() {
                    self.apply_remote_delete(&uid)?;
                }
            }
            EventKind::TreeRefresh => {
                let root = self.sidecar.get_root().await?;
                self.enumerate_remote(&root).await?;
                if !self.placeholders {
                    let files = self.remote_files_needing_download()?;
                    for (rel, uid) in files {
                        if let Some(reason) = self.unmaterializable_reason(&rel) {
                            self.report_unmaterializable(&rel, &reason);
                            continue;
                        }
                        self.download_file(&uid, &rel).await?;
                    }
                }
                // Placeholder mode: nothing to download — the VFS serves
                // the refreshed tree as placeholders (hydrate on open).
            }
        }
        // Persist the resume token after applying (protocol.md F6).
        self.db.set_event_cursor(&ev.scope_id, &ev.event_id)?;
        // Keep the VFS tree in step with applied events (M3).
        if let Some(store) = &self.store
            && let Err(e) = self.publish_vfs_snapshot(store)
        {
            warn!(error = %e, "post-event VFS snapshot failed");
        }
        Ok(())
    }

    /// Ensure every ancestor of `node` has a DB row, fetching the unknown
    /// ones from the sidecar. Events can arrive child-first, so a moved
    /// parent may reference a grandparent row the engine has never seen.
    /// Rows are inserted farthest-first so each path resolves.
    async fn resolve_unknown_ancestors(&mut self, node: &Node) -> Result<()> {
        let root_uid = self.db_root_uid()?;
        let mut chain = Vec::new();
        let mut parent = node.parent_uid.clone();
        let mut seen = HashSet::from([node.uid.clone()]);
        while let Some(uid) = parent {
            // The root is a path boundary, not a node row: never fetch or
            // insert it (its empty name would corrupt every derived path).
            if Some(uid.as_str()) == root_uid.as_deref()
                || self.db.node_by_uid(&uid)?.is_some()
            {
                break;
            }
            if !seen.insert(uid.clone()) {
                warn!(uid = %uid, "remote parent cycle detected; stopping ancestor walk");
                break;
            }
            let ancestor = match self.sidecar.get_node(&uid).await {
                Ok(n) => n,
                Err(e) => {
                    debug!(uid = %uid, error = %e, "unknown ancestor vanished; anchoring at root");
                    break;
                }
            };
            parent = ancestor.parent_uid.clone();
            chain.push(ancestor);
        }
        // `chain` is nearest-first; the farthest ancestor must land first.
        for ancestor in chain.into_iter().rev() {
            let rel = self.local_path_for(&ancestor)?;
            self.upsert_from_node(&ancestor, &rel, SyncState::Synced)?;
            if self.placeholders || !ancestor.is_folder() {
                continue;
            }
            if self.unmaterializable_reason(&rel).is_some() {
                // Keep the logical row (descendant paths stay correct) but do
                // not attempt a directory the OS cannot create. The node that
                // triggered this walk gets its own visible skip.
                continue;
            }
            std::fs::create_dir_all(to_abs(&self.root, &rel))?;
            self.remember_dir_inode(&rel);
        }
        Ok(())
    }

    /// Fetch a node and reconcile it locally (download/create).
    async fn apply_node_upsert(&mut self, uid: &str) -> Result<()> {
        let node = match self.sidecar.get_node(uid).await {
            Ok(n) => n,
            Err(e) => {
                debug!(uid = %uid, error = %e, "node vanished before fetch; skipping");
                return Ok(());
            }
        };
        // A moved parent's OWN parent may still be unknown (its creation
        // event can arrive after the child's update). Without its row,
        // `local_path_for` anchors the parent at the root and the move is
        // lost (TG-02 event ordering).
        self.resolve_unknown_ancestors(&node).await?;
        let rel = self.local_path_for(&node)?;
        // A remote path the local filesystem cannot hold must fail visibly
        // BEFORE any FS/transfer work (TG-14): no partial dir chain, no temp
        // file, and — for a file — no DB row to retry forever.
        if !self.placeholders
            && let Some(reason) = self.unmaterializable_reason(&rel)
        {
            if node.is_folder() {
                // Keep the logical row so descendant paths still resolve.
                self.upsert_from_node(&node, &rel, SyncState::Synced)?;
            }
            self.report_unmaterializable(&rel, &reason);
            return Ok(());
        }
        // Resolve by node uid FIRST: a remote rename/move changes the path
        // derived from the node's current name/parent, while the DB row (and
        // its hydrated content) still sits at the old path. Without this the
        // upsert below hits the `node_uid` UNIQUE constraint and the old
        // path stays stale forever (TG-01/TG-02).
        let existing = self.db.node_by_uid(uid)?;
        let relocating = existing
            .as_ref()
            .is_some_and(|row| row.local_path != rel);
        // Never overwrite whatever already lives at the destination
        // (different node, or a file/folder type clash): keep both (F7).
        self.clear_remote_path(&node, &rel, relocating).await?;
        if let Some(old) = &existing
            && relocating
        {
            self.rekey_remote_node(old, &node, &rel).await?;
        }
        let abs = to_abs(&self.root, &rel);

        if node.is_folder() {
            if !self.placeholders && !abs.exists() {
                std::fs::create_dir_all(&abs)?;
            }
            self.upsert_from_node(&node, &rel, SyncState::Synced)?;
            if !self.placeholders {
                self.remember_dir_inode(&rel);
            }
            return Ok(());
        }

        // File: keep-both whenever the local copy carries content the remote
        // doesn't know about (F7): dirty/uploading rows, untracked files, or
        // on-disk content diverging from the last synced hash. Divergence is
        // judged against the CONTENT copy (backing dir in placeholder mode).
        if abs.is_file() {
            let diverged = match self.db.node_by_path(&rel)? {
                Some(row) => {
                    row.sync_state != SyncState::Synced
                        || local_content_diverged(&row, &self.content_path(&rel))
                }
                None => true, // untracked local file in the way
            };
            if diverged {
                let conflicted = rename_for_conflict_in(&self.db, self.content_dir(), &rel)?;
                warn!(path = %rel, conflict = %conflicted, "remote event vs local content: keeping both");
                self.emit_activity(
                    ActivityKind::Conflict,
                    &rel,
                    Some(format!("kept both; local copy renamed to {conflicted}")),
                );
            }
        }

        if let Some(row) = self.db.node_by_path(&rel)?
            && row.sync_state == SyncState::Synced
            && row.sha1 == node.sha1
        {
            return Ok(()); // already current
        }
        if self.placeholders {
            // M3: register the new remote version; hydrate only if the
            // content was already open (fresh bytes for a live reader).
            self.upsert_from_node(&node, &rel, SyncState::Synced)?;
            return Ok(());
        }
        self.download_file(uid, &rel).await
    }

    /// Free `rel` for an incoming remote node. A different tracked node, or
    /// local content of the opposite type (file vs folder), is renamed aside
    /// with the F7 keep-both convention — the remote node keeps the original
    /// name, matching the existing event-application conflict policy. Never
    /// destroys either side.
    async fn clear_remote_path(
        &mut self,
        node: &Node,
        rel: &str,
        relocating: bool,
    ) -> Result<()> {
        let row_at = self.db.node_by_path(rel)?;
        if row_at
            .as_ref()
            .is_some_and(|row| row.node_uid == node.uid)
        {
            return Ok(()); // this IS the node: a normal update in place
        }
        let abs = self.content_path(rel);
        let type_clash = if node.is_folder() {
            abs.is_file()
        } else {
            abs.is_dir()
        };
        // A move/rename must not land on anything already at the
        // destination; an in-place create only conflicts on a different
        // tracked node or a type mismatch (an existing folder dir is the
        // normal first-run state).
        let row_clash = row_at.is_some();
        let dest_taken = relocating && abs.exists();
        if row_clash || type_clash || dest_taken {
            let conflict = self.rename_local_for_conflict(rel, &node.uid).await?;
            warn!(
                path = %rel,
                conflict = %conflict,
                "remote node collides with local path: keeping both"
            );
            self.emit_activity(
                ActivityKind::Conflict,
                rel,
                Some(format!("kept both; local copy renamed to {conflict}")),
            );
        }
        Ok(())
    }

    /// Rename a local file or folder (and its whole tracked subtree) aside
    /// to a conflict name, moving hydrated content with it. A node with a
    /// real remote uid is renamed remotely as well so the remote tree keeps
    /// agreeing with the local paths; never-synced rows follow the rename
    /// as dirty pending uploads.
    async fn rename_local_for_conflict(
        &mut self,
        rel: &str,
        incoming_uid: &str,
    ) -> Result<String> {
        let new_rel = local_free_conflict_rel_in(&self.db, self.content_dir(), rel)?;
        let from = self.content_path(rel);
        let to = self.content_path(&new_rel);
        if from.exists() {
            if let Some(dir) = to.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::rename(&from, &to)?;
        }

        let mut rows = self.db.nodes_under(&format!("{rel}/%"))?;
        if let Some(row) = self.db.node_by_path(rel)? {
            rows.push(row);
        }
        let new_name = new_rel
            .rsplit('/')
            .next()
            .unwrap_or(&new_rel)
            .to_string();
        let mut moves = Vec::with_capacity(rows.len());
        for mut row in rows {
            let old_path = row.local_path.clone();
            let was_root = old_path == rel;
            row.local_path = if was_root {
                new_rel.clone()
            } else {
                format!("{new_rel}/{}", &old_path[rel.len() + 1..])
            };
            if was_root {
                row.name = new_name.clone();
            }
            // The conflict copy is local content the remote does not know
            // under that name: let it upload if it has pending changes.
            if row.sync_state != SyncState::Synced {
                row.sync_state = SyncState::Dirty;
            }
            moves.push((old_path, row));
        }
        if !moves.is_empty() {
            self.db.rekey_nodes(&moves)?;
        }

        if let Some(mut row) = self.db.node_by_path(&new_rel)? {
            if row.node_uid == incoming_uid {
                // The remote node changed type in place (same uid): the
                // displaced local content cannot keep that identity, so it
                // re-uploads as a new node under the conflict name.
                row.node_uid = format!("pending:{new_rel}");
                row.sync_state = SyncState::Dirty;
                self.db.upsert_node(&row)?;
            } else if !row.node_uid.starts_with("pending:") {
                // Keep the local node's identity (uid, revisions, share
                // links) and rename it remotely too.
                if let Err(e) = self.sidecar.rename_node(&row.node_uid, &new_name).await {
                    warn!(
                        path = %new_rel,
                        error = %e,
                        "remote rename for conflict failed; local copy kept"
                    );
                }
            } else {
                row.node_uid = format!("pending:{new_rel}");
                row.sync_state = SyncState::Dirty;
                self.db.upsert_node(&row)?;
            }
        }

        if let Some(store) = &self.store {
            store.forget_tree(rel);
            let like = format!("{new_rel}/%");
            for child in self.db.nodes_under(&like)? {
                let abs = self.content_path(&child.local_path);
                if abs.is_file() {
                    store.set_backing(&child.local_path, abs);
                }
            }
            if to.is_file() {
                store.set_backing(&new_rel, to);
            }
        }
        self.rekey_local_inodes(rel, &new_rel);
        info!(from = %rel, to = %new_rel, "local item renamed aside for conflict");
        Ok(new_rel)
    }

    /// Re-key a remote node (and, for folders, every descendant) from its
    /// old local path to the path derived from its current remote name and
    /// parent. Hydrated content moves with it, so a rename never triggers a
    /// re-download; the VFS snapshot is republished by the caller.
    async fn rekey_remote_node(
        &mut self,
        old: &NodeRow,
        node: &Node,
        new_rel: &str,
    ) -> Result<()> {
        let old_rel = old.local_path.clone();
        let mut rows = if old.is_file() {
            Vec::new()
        } else {
            self.db.nodes_under(&format!("{old_rel}/%"))?
        };
        rows.push(old.clone());
        let mut moves = Vec::with_capacity(rows.len());
        for mut row in rows {
            let old_path = row.local_path.clone();
            let was_root = old_path == old_rel;
            row.local_path = if was_root {
                new_rel.to_string()
            } else {
                format!("{new_rel}/{}", &old_path[old_rel.len() + 1..])
            };
            if was_root {
                // Relocation only: content metadata stays as-is so a rename
                // combined with a remote content change still compares the
                // real local hash against the remote head and downloads the
                // new bytes (a pure rename keeps the matching hash and skips
                // the re-download).
                row.name = node.name.clone();
                row.parent_uid = node.parent_uid.clone();
                row.kind = node.kind;
            }
            moves.push((old_path, row));
        }
        // One transaction: a crash can never leave both the old and the new
        // path rows (or a half-moved folder subtree) behind.
        self.db.rekey_nodes(&moves)?;

        // Move the hydrated copy (visible root in mirror mode, backing cache
        // in placeholder mode). The row paths moved first, so a failure here
        // simply re-downloads instead of losing the node.
        let from = self.content_path(&old_rel);
        let to = self.content_path(new_rel);
        if from.exists() {
            if let Some(dir) = to.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::rename(&from, &to)?;
        }
        if let Some(store) = &self.store {
            store.forget_tree(&old_rel);
            let like = format!("{new_rel}/%");
            for child in self.db.nodes_under(&like)? {
                let abs = self.content_path(&child.local_path);
                if abs.is_file() {
                    store.set_backing(&child.local_path, abs);
                }
            }
            if to.is_file() {
                store.set_backing(new_rel, to);
            }
        }
        self.rekey_local_inodes(&old_rel, new_rel);
        if old.is_file() {
            self.remember_file_inode(new_rel);
        }
        info!(from = %old_rel, to = %new_rel, uid = %node.uid, "remote rename/move applied");
        Ok(())
    }

    /// Compute the local relative path for a remote node by walking parent
    /// uids through the DB. Unknown parents fall back to the sync root.
    /// Iterative and uncapped: a deep tree (TG-14) must keep its full path,
    /// not silently re-anchor 64 levels down. A parent cycle (corrupt state)
    /// stops at the last known ancestor instead of looping forever.
    fn local_path_for(&self, node: &Node) -> Result<String> {
        let mut parts = vec![node.name.clone()];
        let mut parent = node.parent_uid.clone();
        let mut seen = HashSet::from([node.uid.clone()]);
        while let Some(uid) = parent {
            if !seen.insert(uid.clone()) {
                warn!(uid = %uid, "remote parent cycle detected; stopping path walk");
                break;
            }
            match self.db.node_by_uid(&uid)? {
                Some(row) => {
                    parts.push(row.name.clone());
                    parent = row.parent_uid.clone();
                }
                None => break, // parent unknown → anchor at root (M1)
            }
        }
        parts.reverse();
        Ok(parts.join("/"))
    }

    fn apply_remote_delete(&mut self, uid: &str) -> Result<()> {
        let Some(row) = self.db.node_by_uid(uid)? else {
            return Ok(());
        };
        let content_abs = self.content_path(&row.local_path);
        // F7 (M4 soak find): the row's state alone can LIE — a local write
        // still inside the watcher's debounce window has not marked the row
        // dirty yet. Hash the on-disk content before destroying anything.
        let locally_unsynced = row.sync_state != SyncState::Synced
            || local_content_diverged(&row, &content_abs);
        if locally_unsynced {
            // Local unsynced changes → keep both (F7); the dirty row will
            // re-upload under an available name. Unpark it so the upload
            // path can re-key it (the remote uid is gone).
            self.clear_upload_blocks(&row.local_path)?;
            info!(path = %row.local_path, "remote delete of dirty local node: keeping local copy");
            return Ok(());
        }
        if content_abs.is_dir() {
            std::fs::remove_dir_all(&content_abs).ok();
        } else {
            std::fs::remove_file(&content_abs).ok();
        }
        // Mirror mode also removes the visible copy. In placeholder mode
        // the visible path may be the FUSE mount itself (desktop layout):
        // unlinking through it would round-trip into the store as a fresh
        // local delete of an already-deleted node. The placeholder vanishes
        // on the next snapshot publish instead.
        if self.placeholders {
            if let Some(store) = &self.store {
                store.clear_backing(&row.local_path);
            }
        } else {
            let abs = to_abs(&self.root, &row.local_path);
            if abs.is_dir() {
                std::fs::remove_dir_all(&abs).ok();
            } else {
                std::fs::remove_file(&abs).ok();
            }
        }
        if row.is_file() {
            self.file_ids.remove(&row.local_path);
        } else {
            let prefix = format!("{}/", row.local_path);
            self.dir_ids
                .retain(|rel, _| rel != &row.local_path && !rel.starts_with(&prefix));
            self.file_ids
                .retain(|rel, _| rel != &row.local_path && !rel.starts_with(&prefix));
        }
        self.remove_node_recursive(&row.local_path)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Transfers
    // ------------------------------------------------------------------

    /// Where this engine reads local file content from (visible root, or
    /// backing dir in placeholder mode — see [`content_root`]).
    fn content_path(&self, rel: &str) -> PathBuf {
        to_abs(self.content_dir(), rel)
    }

    /// The directory holding this engine's local file content (visible root
    /// in mirror mode, backing dir in placeholder mode).
    fn content_dir(&self) -> &Path {
        content_root(self.placeholders, &self.root, &self.backing_dir)
    }

    /// Publish the current DB state as the VFS tree snapshot (M3). Files
    /// with a backing file on disk are flagged `content_ready`.
    fn publish_vfs_snapshot(&self, store: &Store) -> Result<()> {
        let rows = self.db.all_nodes()?;
        let mut by_path = HashMap::with_capacity(rows.len());
        for row in rows {
            let Some(name) = row.local_path.rsplit('/').next().map(str::to_string) else {
                continue;
            };
            let parent = row
                .local_path
                .rsplit_once('/')
                .map(|(p, _)| p.to_string())
                .unwrap_or_default();
            by_path.insert(
                row.local_path.clone(),
                VNode {
                    rel: row.local_path.clone(),
                    parent,
                    name,
                    is_dir: !row.is_file(),
                    size: row.size.unwrap_or(0).max(0) as u64,
                    mtime: row.mtime.unwrap_or(0),
                    mode: if row.is_file() {
                        row.mode.unwrap_or(0o644) & 0o777
                    } else {
                        0o755
                    },
                    content_ready: to_abs(&self.backing_dir, &row.local_path).is_file(),
                },
            );
        }
        store.publish_snapshot(by_path);
        Ok(())
    }

    /// Apply a VFS `setattr` metadata change to the row and snapshot.
    /// Timestamp-only changes do NOT queue an upload: the content hash is
    /// unchanged, and a metadata-only revision would be remote churn. The
    /// stored mtime is preserved and rides along on the next real edit
    /// (`upload_params` reads the row). An overlay-only file has no row
    /// yet; its upload captures the on-disk mtime/mode.
    fn apply_metadata_update(
        &mut self,
        store: &Store,
        rel: &str,
        mtime: Option<i64>,
        mode: Option<u32>,
    ) {
        let Some(mut row) = self.db.node_by_path(rel).ok().flatten() else {
            return;
        };
        if let Some(mtime) = mtime {
            row.mtime = Some(mtime);
        }
        if let Some(mode) = mode {
            row.mode = Some(mode & 0o777);
        }
        if let Err(e) = self.db.upsert_node(&row) {
            warn!(path = %rel, error = %e, "metadata update failed");
            return;
        }
        if let Err(e) = self.publish_vfs_snapshot(store) {
            warn!(path = %rel, error = %e, "post-metadata VFS snapshot failed");
        }
    }

    /// Hydrate one requested placeholder: download to the backing dir,
    /// register the backing file, refresh the snapshot. Failures just
    /// unblock the waiting reader (it retries with EIO → new open).
    async fn hydrate_one(&mut self, store: &Store, rel: &str) {
        let Some(row) = self.db.node_by_path(rel).ok().flatten() else {
            store.finish_hydrate(rel, false);
            return;
        };
        if matches!(row.sync_state, SyncState::Synced | SyncState::Downloading)
            && let Err(e) = self.db.set_sync_state(rel, SyncState::Downloading)
        {
            warn!(path = %rel, error = %e, "failed to mark hydration in progress");
        }
        match self
            .download_to_backing(&row.node_uid, rel, row.mtime, row.mode)
            .await
        {
            Ok(()) => {
                store.set_backing(rel, to_abs(&self.backing_dir, rel));
                // Hydration only owns the download direction: never rewrite a
                // row that has pending local changes (`Dirty`/`Uploading`).
                let state = self.db.node_by_path(rel).ok().flatten().map(|r| r.sync_state);
                if matches!(state, Some(SyncState::Downloading | SyncState::Synced)) {
                    if let Err(e) = self.db.set_sync_state(rel, SyncState::Synced) {
                        warn!(path = %rel, error = %e, "failed to mark hydration complete");
                    }
                    if let Err(e) = self.clear_upload_blocks(rel) {
                        warn!(path = %rel, error = %e, "failed to clear hydration error");
                    }
                    self.retry_at.remove(rel);
                }
                if let Err(e) = self.publish_vfs_snapshot(store) {
                    warn!(path = %rel, error = %e, "post-hydrate snapshot failed");
                }
                info!(path = %rel, "hydrated on demand");
            }
            Err(e) => {
                warn!(path = %rel, error = %e, "hydration failed");
                if !self.shutting_down {
                    self.record_download_failure(rel, &e);
                }
            }
        }
        store.finish_hydrate(rel, true);
    }

    /// Eviction pass (M3.2): while hydrated content exceeds the cap,
    /// dehydrate the least-recently-used files. Never touches pinned,
    /// dirty/un-uploaded (sync_state != Synced covers write-through
    /// files until their upload lands), or currently open files; never
    /// evicts the file that just hydrated. Runs on the idle path of the
    /// main loop (single-threaded engine → no hydration race).
    fn maybe_evict(&mut self, store: &Store, protect: &str) {
        let Some(cap) = self.eviction_cap else {
            return;
        };
        let Ok(rows) = self.db.all_nodes() else {
            return;
        };
        let by_path: HashMap<&str, &NodeRow> =
            rows.iter().map(|r| (r.local_path.as_str(), r)).collect();
        let total: u64 = store
            .hydrated_by_age()
            .iter()
            .filter_map(|(rel, _)| by_path.get(rel.as_str()).and_then(|r| r.size))
            .map(|s| s.max(0) as u64)
            .sum();
        if total <= cap {
            return;
        }
        let mut total = total;
        for (rel, _age) in store.hydrated_by_age() {
            if total <= cap {
                break;
            }
            if rel == protect {
                continue;
            }
            let Some(row) = by_path.get(rel.as_str()) else {
                // No row (stale backing entry after a delete): drop the file.
                let _ = std::fs::remove_file(to_abs(&self.backing_dir, &rel));
                store.clear_backing(&rel);
                continue;
            };
            if row.pinned
                || row.sync_state != SyncState::Synced
                || store.is_open(&rel)
                || store.is_uploading(&rel)
            {
                continue;
            }
            let size = row.size.unwrap_or(0).max(0) as u64;
            let abs = to_abs(&self.backing_dir, &rel);
            if std::fs::remove_file(&abs).is_ok() {
                store.clear_backing(&rel);
                total = total.saturating_sub(size);
                debug!(path = %rel, bytes = size, "dehydrated (LRU eviction)");
            }
        }
        // Snapshot so content_ready flips back to placeholder immediately.
        if let Err(e) = self.publish_vfs_snapshot(store) {
            warn!(error = %e, "post-evict VFS snapshot failed");
        }
    }

    /// Download a node into the backing dir (placeholder mode). Same atomic
    /// tmp+rename pattern as [`Engine::download_file`], different target.
    async fn download_to_backing(
        &mut self,
        uid: &str,
        rel: &str,
        mtime: Option<i64>,
        mode: Option<u32>,
    ) -> Result<()> {
        if let Some(reason) = path_limit_reason(&self.backing_dir, rel) {
            // Hydration target is the backing cache: same OS limits, and the
            // cache path is longer than the visible root's. Surface it once
            // instead of letting the reader see a bare EIO.
            self.report_unmaterializable(rel, &reason);
            anyhow::bail!("{reason}");
        }
        let abs = to_abs(&self.backing_dir, rel);
        if abs.is_file() {
            return Ok(()); // already hydrated (lost wake-up)
        }
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = to_abs(&self.backing_dir, &transfer_temp_rel(rel));
        // Surface an unwritable backing dir before the transfer (see
        // `try_download_file`); this is also the temp the transfer writes.
        std::fs::File::create(&tmp)
            .with_context(|| format!("prepare hydration target {}", abs.display()))?;
        let sidecar = Arc::clone(&self.sidecar);
        let outcome = match self
            .run_transfer("download", Some(rel), sidecar.download(uid, &tmp))
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(anyhow::Error::new(e).context(format!("hydrate `{rel}`")));
            }
        };
        let _ = outcome;
        if let Err(e) = std::fs::rename(&tmp, &abs) {
            let _ = std::fs::remove_file(&tmp);
            return Err(anyhow::Error::new(e)
                .context(format!("install hydrated file {}", abs.display())));
        }
        apply_file_metadata(&abs, mtime, mode);
        Ok(())
    }

    /// Drain one write-queue entry: upload the backing content through the
    /// standard machinery (F7 precondition, retries, pause buffering). The
    /// engine creates the DB row from scratch here — the VFS does not own
    /// rows. Failures stay dirty and re-queue via the store.
    async fn upload_write_one(&mut self, store: &Store, rel: &str) {
        // Parent folders first: the VFS queues mkdirs before the writes
        // inside them, but a crash could leave an orphan write queued.
        while let Some(dir) = store.next_mkdir_request() {
            if let Err(e) = self.ensure_remote_folder_async(&dir).await {
                warn!(path = %dir, error = %e, "write mkdir failed");
            }
        }
        let result = self.write_upload_row(rel).await;
        if let Err(e) = &result {
            warn!(path = %rel, error = %e, "write upload failed");
        }
        // A vanished backing file usually means the file was renamed or
        // deleted through the mount while this upload was queued/in flight;
        // those queue entries own the path now. Re-queueing would hot-loop
        // forever (the file never comes back at this path). Detect both the
        // explicit "backing file vanished" context (metadata probe) and a
        // raw NotFound from the hash/read racing the rename. Brief grace
        // first: a rename queued a heartbeat ago may not have re-keyed the
        // write queue yet, and the rename's own upload will carry the
        // content.
        let vanished = result.as_ref().is_err_and(|e| {
            e.chain().any(|c| c.to_string() == "backing file vanished")
                || e.chain()
                    .filter_map(|c| c.downcast_ref::<std::io::Error>())
                    .any(|io| io.kind() == std::io::ErrorKind::NotFound)
        });
        if vanished {
            let first_seen = self
                .vanished_writes
                .entry(rel.to_string())
                .or_insert_with(Instant::now);
            if first_seen.elapsed() < WRITE_VANISH_GRACE {
                store.finish_write(rel, false);
                return;
            }
            self.vanished_writes.remove(rel);
        }
        store.finish_write(rel, result.is_ok() || vanished);
        if let Err(e) = self.publish_vfs_snapshot(store) {
            warn!(error = %e, "post-write VFS snapshot failed");
        }
    }

    /// A tombstone names a node queued for trash. When a write pushes new
    /// content onto that very node, the delete that queued the tombstone is
    /// stale: an overwrite-rename whose rename half found no tracked source
    /// row ("rename of unknown local item") updates the destination row in
    /// place, so the destination's queued delete still points at the node
    /// the write just delivered. Trashing it would destroy the file. Drop
    /// the tombstone instead. When the rename re-keyed a tracked source
    /// onto the path, the tombstone's uid differs from the uploaded node's
    /// and the old node's trash correctly stands.
    fn cancel_stale_tombstone(&self, rel: &str, uploaded_uid: &str) {
        let Ok(tombstones) = self.db.tombstones() else { return };
        let Some(t) = tombstones.iter().find(|t| t.local_path == rel) else {
            return;
        };
        if t.node_uid != uploaded_uid {
            return;
        }
        match self.db.remove_tombstone(rel) {
            Ok(()) => info!(
                path = %rel,
                uid = %uploaded_uid,
                "cancelled stale tombstone: in-place update superseded the queued delete"
            ),
            Err(e) => warn!(path = %rel, error = %e, "failed to cancel stale tombstone"),
        }
    }

    /// DB row creation + upload for a write-through file. Mirrors
    /// [`Engine::upsert_local_file`] but sources content from the backing
    /// dir and never renames on collision (the VFS file stays where the
    /// user put it; the collision path uploads under an available name
    /// instead and re-keys the row).
    async fn write_upload_row(&mut self, rel: &str) -> Result<()> {
        let abs = to_abs(&self.backing_dir, rel);
        // Unreadable backing content is a local environment failure, not a
        // vanished file: surface it and let `upload_write_one` drop the
        // queue entry (a fresh write heals it), never upload empty bytes.
        if let Err(e) = std::fs::File::open(&abs) {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Err(anyhow::Error::new(e).context("backing file vanished"));
            }
            return self.record_local_upload_failure(
                rel,
                anyhow::Error::new(e).context(format!("read local file {}", abs.display())),
            );
        }
        let meta = std::fs::metadata(&abs).context("backing file vanished")?;
        let (sha1, size) = sha1_file(&abs)?;
        let mtime = file_mtime(&meta);
        let mode = file_mode(&meta);
        let name = rel.rsplit('/').next().unwrap_or(rel).to_string();

        let (parent_uid, node_uid, synced_sha1) = match self.db.node_by_path(rel)? {
            Some(row) => {
                if row.sync_state == SyncState::Synced && row.sha1.as_deref() == Some(&sha1) {
                    // Content identical to the synced revision. A queued
                    // delete for this node is stale (an overwrite-rename
                    // delivering identical content); the file demonstrably
                    // still exists with its known content, so the newest
                    // signal — the write — wins over the older delete.
                    self.cancel_stale_tombstone(rel, &row.node_uid);
                    // A chmod may still have changed the local mode.
                    if row.mode != mode {
                        let mut updated = row;
                        updated.mode = mode;
                        self.db.upsert_node(&updated)?;
                    }
                    return Ok(()); // content unchanged (spurious close)
                }
                // Revision of an existing (possibly pending) node. A pending
                // row without a parent (legacy/bugged rows — the second
                // upsert here used to clobber it to NULL) re-resolves the
                // parent now: without it the sidecar rejects the upload
                // (`exactly one of parentUid/nodeUid`).
                let parent_uid = match row.parent_uid.clone() {
                    Some(p) => Some(p),
                    None if row.node_uid.starts_with("pending:") => {
                        let parent_rel = rel.rsplit_once('/').map(|(d, _)| d);
                        match parent_rel {
                            Some(d) => Some(self.ensure_remote_folder_async(d).await?),
                            None => self.db_root_uid()?,
                        }
                    }
                    None => None,
                };
                (
                    parent_uid,
                    (!row.node_uid.starts_with("pending:")).then(|| row.node_uid.clone()),
                    row.synced_sha1.clone(),
                )
            }
            None => {
                let parent_rel = rel.rsplit_once('/').map(|(d, _)| d);
                let parent_uid = match parent_rel {
                    Some(d) => Some(self.ensure_remote_folder_async(d).await?),
                    None => self.db_root_uid()?,
                };
                // F7: never silently overwrite a remote sibling — but the
                // backing file cannot move (the VFS serves that path), so
                // upload under an available name and re-key the row to it.
                let parent = parent_uid.clone().ok_or_else(|| anyhow!("no root uid"))?;
                let mut remote_name = name.clone();
                if let Ok(available) = self.sidecar.get_available_name(&parent, &name).await
                    && available != name
                {
                    warn!(path = %rel, renamed = %available, "remote name collision on write-through");
                    remote_name = available;
                }
                self.db.upsert_node(&NodeRow {
                    local_path: rel.to_string(),
                    node_uid: format!("pending:{rel}"),
                    parent_uid,
                    name: remote_name,
                    kind: NodeKind::File,
                    size: Some(size as i64),
                    sha1: Some(sha1.clone()),
                    mtime,
                    mode,
                    sync_state: SyncState::Dirty,
                    pinned: false,
                    synced_sha1: None,
                })?;
                (None, None, None)
            }
        };

        // Re-read the row: for a brand-new file the first upsert above
        // already resolved and stored the parent uid (and possibly a
        // collision-renamed `name`); the `None` tuple must not clobber it.
        if let Some(row) = self.db.node_by_path(rel)? {
            self.db.upsert_node(&NodeRow {
                local_path: rel.to_string(),
                node_uid: node_uid.clone().unwrap_or_else(|| row.node_uid.clone()),
                parent_uid: parent_uid.or(row.parent_uid.clone()),
                name: row.name.clone(),
                kind: NodeKind::File,
                size: Some(size as i64),
                sha1: Some(sha1),
                mtime,
                mode,
                sync_state: SyncState::Dirty,
                pinned: row.pinned,
                synced_sha1,
            })?;
            self.db.clear_upload_failures(rel).ok();
            self.retry_at.remove(rel);
            self.vanished_writes.remove(rel);

            if self.state != EngineState::Running {
                debug!(path = %rel, "paused: write-through upload buffered");
                return Ok(());
            }
            self.upload_one(rel).await?;
            // A revision just landed on an existing remote node; see the
            // helper for why a same-node tombstone is stale.
            if let Some(uid) = &node_uid {
                self.cancel_stale_tombstone(rel, uid);
            }
        }
        Ok(())
    }

    /// Download `uid` to `rel`. A local/transport failure never fails the
    /// caller (an event, an enumeration pass, or the retry pump): it parks
    /// the row as `Downloading` with the error and a backoff, leaving any
    /// existing local content untouched (TG-19).
    async fn download_file(&mut self, uid: &str, rel: &str) -> Result<()> {
        if let Some(reason) = self.unmaterializable_reason(rel) {
            self.report_unmaterializable(rel, &reason);
            return Ok(());
        }
        match self.try_download_file(uid, rel).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // A shutdown/cancel is not an environment failure: leave the
                // row alone and let the next run recover it.
                if !self.shutting_down {
                    self.record_download_failure(rel, &e);
                }
                Ok(())
            }
        }
    }

    async fn try_download_file(&mut self, uid: &str, rel: &str) -> Result<()> {
        let abs = self.content_path(rel);
        let node = self.sidecar.get_node(uid).await.ok();
        // Make the row visible to the retry pump before the transfer: a
        // brand-new node (created by an event) needs a row to record the
        // failure against.
        if self.db.node_by_path(rel)?.is_none()
            && let Some(node) = &node
        {
            self.upsert_from_node(node, rel, SyncState::Downloading)?;
        }
        let state = self.db.node_by_path(rel)?.map(|r| r.sync_state);
        if matches!(state, None | Some(SyncState::Synced | SyncState::Downloading)) {
            self.db.set_sync_state(rel, SyncState::Downloading)?;
        }
        // Mode is local-only (D2): the remote has none, but a row that
        // already knows the user's `chmod` keeps it across re-downloads.
        let existing_mode = self.db.node_by_path(rel)?.and_then(|r| r.mode);
        let remote_mtime = node.as_ref().and_then(|n| n.mtime);
        let tmp_rel = transfer_temp_rel(rel);
        let tmp = self.content_path(&tmp_rel);
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("create directory for {}", abs.display()))?;
        }
        // Fail with the OS cause *before* the transfer when the target is not
        // writable: the sidecar reports its own write errors, but not every
        // transport surfaces one, and a missing temp afterwards would hide
        // the real reason (a bare rename ENOENT). `*.pds-part` is a transfer
        // temp, ignored by the watcher.
        std::fs::File::create(&tmp)
            .with_context(|| format!("prepare download target {}", abs.display()))?;
        let sidecar = Arc::clone(&self.sidecar);
        let outcome = match self
            .run_transfer("download", Some(rel), sidecar.download(uid, &tmp))
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(anyhow::Error::new(e).context(format!("download `{rel}`")));
            }
        };
        let _ = outcome;
        if let Err(e) = std::fs::rename(&tmp, &abs) {
            let _ = std::fs::remove_file(&tmp);
            return Err(anyhow::Error::new(e)
                .context(format!("install downloaded file {}", abs.display())));
        }
        apply_file_metadata(&abs, remote_mtime, existing_mode);
        self.db.upsert_node(&NodeRow {
            local_path: rel.to_string(),
            node_uid: uid.to_string(),
            parent_uid: node.as_ref().and_then(|n| n.parent_uid.clone()),
            name: rel.rsplit('/').next().unwrap_or(rel).to_string(),
            kind: NodeKind::File,
            size: node.as_ref().and_then(|n| n.size),
            sha1: node.as_ref().and_then(|n| n.sha1.clone()),
            mtime: remote_mtime,
            mode: existing_mode,
            sync_state: SyncState::Synced,
            pinned: false,
            synced_sha1: node.as_ref().and_then(|n| n.sha1.clone()),
        })?;
        self.clear_upload_blocks(rel)?;
        self.retry_at.remove(rel);
        self.remember_file_inode(rel);
        info!(path = %rel, "downloaded");
        self.emit_activity(ActivityKind::Downloaded, rel, None);
        Ok(())
    }

    /// Record a failed download/hydration: keep the row visible as errored,
    /// leave the previous local copy in place, and schedule an exponential
    /// retry (an explicit resume clears the backoff for "try now").
    fn record_download_failure(&mut self, rel: &str, error: &anyhow::Error) {
        let message = format!("{error:#}");
        let current = self.db.node_by_path(rel).ok().flatten().map(|r| r.sync_state);
        if matches!(current, None | Some(SyncState::Synced | SyncState::Downloading))
            && let Err(e) = self.db.set_sync_state(rel, SyncState::Downloading)
        {
            warn!(path = %rel, error = %e, "failed to mark download as errored");
        }
        // `last_error`/`retry_count` are direction-agnostic storage: the same
        // columns back upload and download failure bookkeeping.
        let count = self.db.record_retryable_failure(rel).unwrap_or(1);
        if let Err(e) = self.db.mark_upload_errored(rel, &message) {
            warn!(path = %rel, error = %e, "failed to persist download error");
        }
        let backoff = retry_backoff(self.retry_base, count);
        self.retry_at.insert(rel.to_string(), Instant::now() + backoff);
        warn!(path = %rel, error = %message, backoff = ?backoff, "download failed; keeping local content and retrying");
        self.emit_activity(ActivityKind::DownloadErrored, rel, Some(message));
    }

    /// Issue a transfer request, then await its completion notification while
    /// still servicing control commands (F4 mid-transfer pause/resume).
    async fn run_transfer(
        &mut self,
        kind: &str,
        local_path: Option<&str>,
        start: impl std::future::Future<Output = Result<String, pds_sidecar_client::Error>>,
    ) -> Result<TransferOutcome, TransferFailure> {
        let transfer_id = start
            .await
            .map_err(|e| TransferFailure::Aborted(e.into()))?;
        self.active_transfer = Some(transfer_id.clone());
        self.db
            .upsert_transfer(
                &transfer_id,
                kind,
                local_path,
                None,
                0,
                None,
                "running",
                None,
                now_unix(),
            )
            .map_err(|e| TransferFailure::Aborted(e.into()))?;
        let kind = kind.to_string();
        let local_path = local_path.map(str::to_string);
        let result = loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::Shutdown) | None => {
                            let _ = self.sidecar.transfer_cancel(&transfer_id).await;
                            self.shutting_down = true;
                            break Err(TransferFailure::Aborted(anyhow!("shutdown during transfer")));
                        }
                        Some(c) => {
                            if let Err(e) = self.handle_command(c) {
                                break Err(TransferFailure::Aborted(e));
                            }
                        }
                    }
                }
                n = self.notifications.recv() => {
                    match n {
                        Some(Notification::TransferDone { transfer_id: tid, node_uid, .. })
                            if tid == transfer_id =>
                        {
                            break Ok(TransferOutcome { node_uid });
                        }
                        Some(Notification::TransferError {
                            transfer_id: tid,
                            code,
                            message,
                            retryable,
                            retry_after,
                        }) if tid == transfer_id =>
                        {
                            break Err(TransferFailure::Remote {
                                code,
                                message,
                                retryable,
                                retry_after,
                            });
                        }
                        Some(other) => self.backlog.push_back(other),
                        None => break Err(TransferFailure::Aborted(anyhow!("sidecar closed during transfer"))),
                    }
                }
            }
        };
        self.active_transfer = None;
        self.db
            .upsert_transfer(
                &transfer_id,
                &kind,
                local_path.as_deref(),
                None,
                0,
                None,
                if result.is_ok() { "done" } else { "error" },
                result.as_ref().err().map(|e| e.to_string()).as_deref(),
                now_unix(),
            )
            .map_err(|e| TransferFailure::Aborted(e.into()))?;
        result
    }

    // ------------------------------------------------------------------
    // F4 — pause / resume
    // ------------------------------------------------------------------

    fn handle_command(&mut self, cmd: Command) -> Result<()> {
        match cmd {
            Command::Pause => {
                if self.state == EngineState::Running {
                    self.state = EngineState::Paused;
                    info!("paused");
                    // Freeze the in-flight transfer too (block-level resume
                    // is the sidecar's job).
                    if let Some(tid) = self.active_transfer.clone() {
                        let sidecar = Arc::clone(&self.sidecar);
                        tokio::spawn(async move {
                            let _ = sidecar.transfer_pause(&tid).await;
                        });
                    }
                }
            }
            Command::Resume => {
                match self.state {
                    EngineState::Paused => {
                        self.state = EngineState::Running;
                        info!("resumed");
                    }
                    EngineState::GuardPaused => {
                        // Explicit resume IS the user confirmation (F3.3) —
                        // and it approves the tombstones pending *right now*
                        // (bug #2): they drain without re-tripping the guard
                        // as the synced base shrinks. Only deletes beyond
                        // this approved set can trip it again.
                        if let Ok(pending) = self.db.tombstones() {
                            self.approved_tombstones
                                .extend(pending.into_iter().map(|t| t.local_path));
                        }
                        self.guard.reset();
                        self.state = EngineState::Running;
                        info!("resumed after mass-delete guard");
                    }
                    EngineState::Running => {}
                }
                // Explicit resume = "try now": drop pending retry backoffs.
                self.retry_at.clear();
                if let Some(tid) = self.active_transfer.clone() {
                    let sidecar = Arc::clone(&self.sidecar);
                    tokio::spawn(async move {
                        let _ = sidecar.transfer_resume(&tid).await;
                    });
                }
            }
            Command::Status(reply) => {
                // Placeholder mode: report real on-disk usage of the backing
                // cache (du-style walk, skipping in-flight *.pds-part tmp
                // files is unnecessary — they are renamed atomically and a
                // half-written tmp only ever inflates the count briefly).
                let (cache_used, cache_path) = match self.store.as_ref() {
                    Some(_) => {
                        let used = dir_size(&self.backing_dir);
                        (used, Some(self.backing_dir.clone()))
                    }
                    None => (0, None),
                };
                let _ = reply.send(EngineStatus {
                    state: self.state,
                    synced_nodes: self.db.count_synced().unwrap_or(0),
                    dirty_nodes: self
                        .db
                        .nodes_with_state(SyncState::Dirty)
                        .map(|v| v.len())
                        .unwrap_or(0),
                    queued_tombstones: self.db.tombstones().map(|v| v.len()).unwrap_or(0),
                    errored_nodes: self.db.count_errored().unwrap_or(0),
                    conflicted_nodes: self.db.count_remote_conflicted().unwrap_or(0),
                    mode: self.sync_mode(),
                    cache_cap: self.eviction_cap,
                    cache_used,
                    cache_path,
                });
            }
            Command::Shutdown => {}
            Command::SetEvictionCap(cap, reply) => {
                self.eviction_cap = cap;
                // Enforce immediately: shrinking the cap below current
                // usage should reclaim space now, not on the next idle tick.
                if let Some(store) = self.store.clone() {
                    self.maybe_evict(&store, "");
                }
                info!(cap = cap.map(|c| c.to_string()).unwrap_or_else(|| "unlimited".into()), "cache cap changed");
                let _ = reply.send(Ok(()));
            }
            Command::Pin(rel, reply) => {
                // pin_file downloads, so it cannot run inline (non-async
                // context): queue it and complete the reply when drained.
                self.pending_pins.push_back(PinRequest {
                    rel,
                    reply,
                    pin: true,
                });
            }
            Command::Unpin(rel, reply) => {
                let _ = reply.send(self.unpin_file(&rel));
            }
        }
        Ok(())
    }

    /// Pin: mark the row (and rows under it, for folders) `pinned` and make
    /// sure the content is hydrated. Directories pin recursively — a pin is
    /// "keep this subtree offline".
    async fn pin_file(&mut self, rel: &str) -> Result<()> {
        if !self.placeholders {
            bail!("pins apply to placeholder (mount) mode");
        }
        let Some(row) = self.db.node_by_path(rel)? else {
            bail!("unknown path `{rel}`");
        };
        let targets: Vec<String> = if row.is_file() {
            vec![rel.to_string()]
        } else {
            self.db
                .nodes_under(&format!("{rel}/%"))?
                .into_iter()
                .filter(|r| r.is_file())
                .map(|r| r.local_path)
                .chain(std::iter::once(rel.to_string()))
                .collect()
        };
        for t in &targets {
            self.db.set_pinned(t, true)?;
        }
        // Hydrate whatever is missing (sequential; sidecar transfers are
        // serialized per engine anyway).
        for t in targets.iter().filter(|t| *t != rel || row.is_file()) {
            if self.store.as_ref().and_then(|s| s.backing_path(t)).is_none() {
                let store = self.store.clone().expect("placeholders checked");
                self.hydrate_one(&store, t).await;
            }
        }
        info!(path = %rel, files = targets.len(), "pinned");
        Ok(())
    }

    /// Unpin: drop the flag; content stays until the eviction pass picks it.
    fn unpin_file(&mut self, rel: &str) -> Result<()> {
        if !self.placeholders {
            bail!("pins apply to placeholder (mount) mode");
        }
        let Some(row) = self.db.node_by_path(rel)? else {
            bail!("unknown path `{rel}`");
        };
        if row.is_file() {
            self.db.set_pinned(rel, false)?;
        } else {
            self.db.set_pinned_tree(rel, false)?;
        }
        info!(path = %rel, "unpinned");
        Ok(())
    }

    /// Drain queued work: tombstones (guarded) → buffered remote events →
    /// uploads. Buffered events are applied BEFORE the upload queue (bug #1):
    /// it narrows the window in which a not-yet-arrived remote edit would be
    /// superseded by a local revision upload. (Deviation from the SPEC F4
    /// drain order "uploads then events", accepted for F7 safety.)
    /// Returns true if any work was done.
    async fn pump_work(&mut self) -> Result<bool> {
        if !self.db.tombstones()?.is_empty() {
            // Guard may auto-pause; false means "stop pumping".
            self.drain_one_tombstone().await?;
            // Write-through: the VFS tree must drop the path right away.
            if let Some(store) = &self.store
                && let Err(e) = self.publish_vfs_snapshot(store)
            {
                warn!(error = %e, "post-trash VFS snapshot failed");
            }
            return Ok(true);
        }
        if let Some(ev) = self.buffered_events.pop_front() {
            self.apply_event(ev).await?;
            return Ok(true);
        }
        // Failed downloads/hydrations: retry once their backoff matures (an
        // explicit resume clears it). Mirror downloads go to the visible
        // root; placeholder mode hydrates into the backing cache.
        let now = Instant::now();
        if let Some(row) = self
            .db
            .nodes_with_state(SyncState::Downloading)?
            .into_iter()
            .find(|r| self.retry_at.get(&r.local_path).is_none_or(|t| *t <= now))
        {
            if let Some(store) = self.store.clone() {
                self.hydrate_one(&store, &row.local_path).await;
                self.maybe_evict(&store, &row.local_path);
            } else {
                self.download_file(&row.node_uid, &row.local_path).await?;
            }
            return Ok(true);
        }
        // Uploads: skip terminally errored / remote-conflicted nodes and
        // rows waiting out a retry backoff (F2/F7 — no hot loop).
        let errored: HashSet<String> = self.db.errored_paths()?.into_iter().collect();
        let conflicted: HashSet<String> = self.db.remote_conflicted_paths()?.into_iter().collect();
        let now = Instant::now();
        if let Some(row) = self
            .db
            .nodes_with_state(SyncState::Dirty)?
            .into_iter()
            .find(|r| {
                !errored.contains(&r.local_path)
                    && !conflicted.contains(&r.local_path)
                    && self.retry_at.get(&r.local_path).is_none_or(|t| *t <= now)
            })
        {
            self.upload_one(&row.local_path).await?;
            return Ok(true);
        }
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Unix seconds from a file's modification time.
fn file_mtime(meta: &std::fs::Metadata) -> Option<i64> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

/// POSIX permission bits (0o777 mask) of a file.
fn file_mode(meta: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt as _;
    Some(meta.permissions().mode() & 0o777)
}

/// Apply a remote/row mtime and a locally persisted mode to a downloaded
/// file (D6/D2). Times are set before permissions: a restrictive mode
/// (e.g. 0444) would otherwise make the write-open needed for `set_times`
/// fail. Missing files are ignored — callers only use this after a rename.
fn apply_file_metadata(path: &Path, mtime: Option<i64>, mode: Option<u32>) {
    if let Some(mtime) = mtime {
        let when = std::time::UNIX_EPOCH + std::time::Duration::from_secs(mtime.max(0) as u64);
        let times = std::fs::FileTimes::new().set_modified(when);
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(path) {
            let _ = f.set_times(times);
        }
    }
    if let Some(mode) = mode {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & 0o777));
    }
}

/// Recursive on-disk size of `dir` (regular files only, symlinks not
/// followed). Missing dir → 0. Used for the backing-cache usage figure in
/// [`EngineStatus`] — a walk beats summing DB rows because it reflects
/// reality even when the index is stale.
fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_dir() {
            total = total.saturating_add(dir_size(&entry.path()));
        } else if meta.is_file() {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

/// Human-readable byte count (`4.6 MB`, `10 GB`, `512 B`) for CLI/UI
/// display. Uses binary units (1 GB = 2³⁰ B), matching the cache-cap
/// presets in the app settings.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 5] = [
        ("B", 1),
        ("KB", 1 << 10),
        ("MB", 1 << 20),
        ("GB", 1 << 30),
        ("TB", 1 << 40),
    ];
    let mut unit = UNITS[0];
    for u in UNITS.iter() {
        if bytes >= u.1 {
            unit = *u;
        }
    }
    if unit.1 == 1 {
        return format!("{} B", bytes);
    }
    let value = bytes as f64 / unit.1 as f64;
    if value >= 10.0 {
        format!("{value:.0} {}", unit.0)
    } else {
        format!("{value:.1} {}", unit.0)
    }
}

/// Where the engine reads local file content from for `rel`: the visible
/// root in materialized mode, the backing dir in placeholder mode (VFS
/// writes never touch the visible root).
fn content_root<'a>(placeholders: bool, root: &'a Path, backing_dir: &'a Path) -> &'a Path {
    if placeholders {
        backing_dir
    } else {
        root
    }
}

/// One-time-per-direction local layout conversion for the desktop app
/// (external backing cache): mirror ⇄ placeholders without re-downloading
/// anything.
///
/// - Switching TO placeholders: every tracked file materialized in the root
///   moves into the backing cache and becomes a hydrated placeholder (its
///   content is on disk, `content_ready`); untracked leftover files move
///   into the backing cache too and register as dirty (pending upload), so
///   nothing real is stranded under the future mount; empty tracked dirs
///   are pruned.
/// - Switching TO mirror: hydrated backing files move back into the root;
///   placeholders without backing content get re-downloaded by the normal
///   materialization pass (nothing is deleted or lost).
///
/// The `.layout-converted` marker records WHICH layout the conversion last
/// produced, so switching back and forth re-runs the right pass (a
/// direction-blind "ran once" marker would strand content on the second
/// toggle). Conservative: files that fail to move stay where they are and
/// the normal sync machinery reconciles them (a left-behind copy in the
/// root re-uploads; a missing one re-downloads). Rows are left as-is:
/// `Synced` rows whose content just moved stay hash-consistent (the bytes
/// did not change).
fn convert_layout(db: &Db, root: &Path, backing_dir: &Path, target: SyncMode) -> Result<()> {
    let marker = backing_dir.join(LAYOUT_CONVERTED);
    if SyncMode::from_meta_value(std::fs::read_to_string(&marker).ok().as_deref()) == Some(target)
    {
        return Ok(());
    }
    let to_placeholders = target == SyncMode::Placeholders;
    std::fs::create_dir_all(backing_dir)?;
    let rows = db.all_nodes()?;
    let mut moved = 0usize;
    for row in rows.iter().filter(|r| r.is_file()) {
        let in_root = to_abs(root, &row.local_path);
        let in_backing = to_abs(backing_dir, &row.local_path);
        let (from, to) = if to_placeholders {
            (in_root, in_backing)
        } else {
            (in_backing, in_root)
        };
        if !from.is_file() || to.exists() {
            continue;
        }
        match crate::mode::move_file(&from, &to) {
            Ok(()) => moved += 1,
            Err(e) => warn!(path = %row.local_path, error = %e, "layout move failed"),
        }
    }
    if to_placeholders {
        // Adoption: files that never synced (failed uploads, pre-existing
        // local content) would be stranded invisible under the mount. Move
        // them into the backing cache and register them dirty (pending
        // upload) — same representation `upsert_local_file` uses.
        let mut stack = vec![root.to_path_buf()];
        while let Some(d) = stack.pop() {
            let mut dirs = Vec::new();
            let Ok(entries) = std::fs::read_dir(&d) else {
                continue; // unreadable dir: leave it; never fail adoption
            };
            for entry in entries {
                let Ok(entry) = entry else { continue };
                let path = entry.path();
                let Some(rel) = to_rel(root, &path) else {
                    continue;
                };
                if is_internal_path(&rel) || is_temp_file(&rel) {
                    // Temp files are swept by crash recovery (M4), never
                    // uploaded or adopted.
                    continue;
                }
                if path.is_dir() {
                    dirs.push((rel, path));
                } else if path.is_file() && db.node_by_path(&rel)?.is_none() {
                    let dest = to_abs(backing_dir, &rel);
                    match crate::mode::move_file(&path, &dest) {
                        Ok(()) => {
                            moved += 1;
                            let (sha1, size) =
                                sha1_file(&dest).unwrap_or_else(|_| (String::new(), 0));
                            let meta = std::fs::metadata(&dest).ok();
                            let mtime = meta.as_ref().and_then(file_mtime);
                            let mode = meta.as_ref().and_then(file_mode);
                            let name =
                                rel.rsplit('/').next().unwrap_or(&rel).to_string();
                            db.upsert_node(&NodeRow {
                                local_path: rel.clone(),
                                node_uid: format!("pending:{rel}"),
                                parent_uid: None,
                                name,
                                kind: pds_sidecar_client::NodeKind::File,
                                size: Some(size as i64),
                                sha1: Some(sha1),
                                mtime,
                                mode,
                                sync_state: SyncState::Dirty,
                                pinned: false,
                                synced_sha1: None,
                            })?;
                        }
                        Err(e) => warn!(path = %rel, error = %e, "adoption move failed"),
                    }
                }
            }
            for (_rel, path) in dirs {
                stack.push(path);
            }
        }
        // Empty directories in the root would linger forever (placeholders
        // mode never materializes dirs); prune the tracked empty ones.
        for row in rows.iter().filter(|r| !r.is_file()) {
            let abs = to_abs(root, &row.local_path);
            std::fs::remove_dir(&abs).ok(); // fails while non-empty: fine
        }
    }
    std::fs::write(&marker, target.as_meta_value())?;
    if moved > 0 {
        info!(files = moved, layout = target.as_meta_value(), "local layout converted");
    }
    Ok(())
}

/// Marker file inside the backing dir recording which layout the conversion
/// last produced (`mirror`/`placeholders`); a repeat pass for the same
/// layout is a no-op, the opposite direction re-runs the moves.
const LAYOUT_CONVERTED: &str = ".layout-converted";

/// Upload params for a dirty row: `pending:` uids upload as a NEW file under
/// the parent; real uids upload as a new revision of the existing node.
fn upload_params(row: &NodeRow, abs: &Path) -> UploadParams {
    UploadParams {
        parent_uid: if row.node_uid.starts_with("pending:") {
            row.parent_uid.clone()
        } else {
            None
        },
        node_uid: if row.node_uid.starts_with("pending:") {
            None
        } else {
            Some(row.node_uid.clone())
        },
        name: row.name.clone(),
        path: abs.to_string_lossy().to_string(),
        size: row.size.unwrap_or(0),
        sha1: row.sha1.clone().unwrap_or_default(),
        mtime: row.mtime.unwrap_or(0),
    }
}

/// Exponential backoff after the `count`-th consecutive failure (1-based):
/// base, 2·base, 4·base, … capped at RETRY_BACKOFF_MAX (F2).
fn retry_backoff(base: Duration, count: i64) -> Duration {
    let shift = (count - 1).clamp(0, 10) as u32; // 2^10·base already ≫ the cap
    (base * 2u32.pow(shift)).min(RETRY_BACKOFF_MAX)
}

/// Stable filesystem identity of a directory: `(device, inode)`. Both are
/// unchanged by a rename or move, which is what makes rename detection
/// possible with a debounced watcher that only reports paths.
fn dir_identity(meta: &std::fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((meta.dev(), meta.ino()))
}

/// Move every tracked identity at `old_rel` or below to the `new_rel`
/// prefix. Shared by the directory and file maps so both follow a rename.
fn rekey_inode_map(
    map: &mut std::collections::HashMap<String, (u64, u64)>,
    old_rel: &str,
    new_rel: &str,
) {
    let old_prefix = format!("{old_rel}/");
    let moved: Vec<(String, (u64, u64))> = map
        .iter()
        .filter(|(path, _)| *path == old_rel || path.starts_with(&old_prefix))
        .map(|(path, id)| (path.clone(), *id))
        .collect();
    for (path, _) in &moved {
        map.remove(path);
    }
    for (path, id) in moved {
        let moved_path = if path == old_rel {
            new_rel.to_string()
        } else {
            format!("{new_rel}/{}", &path[old_rel.len() + 1..])
        };
        map.insert(moved_path, id);
    }
}

/// Absolute path from a root-relative '/'-separated path.
pub fn to_abs(root: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(root.to_path_buf(), |p, c| p.join(c))
}

/// Linux filesystem limits, in bytes. `PATH_MAX` includes the terminating
/// NUL, so a valid path must be strictly shorter; `NAME_MAX` bounds one
/// component. Both are enforced by the kernel at syscall time, but checking
/// first turns an over-long REMOTE path into a visible skip instead of a
/// partial directory chain, a half-written temp file, or a retry loop.
const PATH_MAX_BYTES: usize = 4096;
const NAME_MAX_BYTES: usize = 255;

/// Why `rel` cannot be materialized under `base`, if it cannot.
fn path_limit_reason(base: &Path, rel: &str) -> Option<String> {
    if let Some(component) = rel.split('/').find(|c| c.len() > NAME_MAX_BYTES) {
        return Some(format!(
            "name component is {} bytes (NAME_MAX {NAME_MAX_BYTES}); skipped",
            component.len()
        ));
    }
    let len = to_abs(base, rel).as_os_str().len();
    (len >= PATH_MAX_BYTES)
        .then(|| format!("path is {len} bytes (PATH_MAX {PATH_MAX_BYTES}); skipped"))
}

/// Relative-path classification: the Drive protocol only carries UTF-8
/// names, so an invalid filename must be reported (never silently dropped).
enum RelCheck {
    Valid(String),
    NotUtf8(Vec<u8>),
    Outside,
}

/// Like [`to_rel`], but distinguishes a non-UTF-8 filename (returning its
/// raw bytes) from a path outside the root. The sync pipeline uses this at
/// every local scan/watch site so such items are surfaced once and skipped.
fn to_rel_checked(root: &Path, path: &Path) -> RelCheck {
    use std::os::unix::ffi::OsStrExt;
    let Ok(rel) = path.strip_prefix(root) else {
        return RelCheck::Outside;
    };
    let mut parts = Vec::new();
    for component in rel.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        let Some(name) = name.to_str() else {
            return RelCheck::NotUtf8(rel.as_os_str().as_bytes().to_vec());
        };
        parts.push(name.to_string());
    }
    RelCheck::Valid(parts.join("/"))
}

/// Root-relative '/'-separated path, or None if outside root.
pub fn to_rel(root: &Path, path: &Path) -> Option<String> {
    let rel = path.strip_prefix(root).ok()?;
    let s = rel
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect::<Vec<_>>()
        .join("/");
    Some(s)
}

/// Streaming SHA-1 of a file; returns (hex, size).
pub fn sha1_file(path: &Path) -> std::io::Result<(String, u64)> {
    use sha1::Digest;
    let mut f = std::fs::File::open(path)?;
    let mut h = sha1::Sha1::new();
    let mut size = 0u64;
    let mut buf = [0u8; 64 * 1024];
    loop {
        use std::io::Read;
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        size += n as u64;
    }
    Ok((
        h.finalize().iter().map(|b| format!("{b:02x}")).collect(),
        size,
    ))
}

/// True when the on-disk content no longer matches the last synced hash —
/// i.e. there are local changes the remote doesn't know about (F7 input).
fn local_content_diverged(row: &NodeRow, abs: &Path) -> bool {
    match sha1_file(abs) {
        Ok((sha1, _)) => Some(&sha1) != row.sha1.as_ref(),
        Err(_) => false,
    }
}

/// Transient transfer temp files: ours are `*.pds-part`, and the sidecar
/// downloads via `{path}.pdstmp-{transferId}` + rename — so its temps match
/// neither a `.pds-part` suffix check nor our names exactly. Ignore both on
/// create AND delete events (they are rename artifacts, never user content).
/// Paths the sync pipeline must ignore: engine state, and the VFS backing
/// dir whose contents mirror remote files (placeholder mode). Without the
/// backing-dir filter, hydration/eviction/revision writes would re-enter
/// the watcher as "local changes" of themselves.
fn is_internal_path(rel: &str) -> bool {
    rel == STATE_DIR
        || rel.starts_with(&format!("{STATE_DIR}/"))
        || rel == VFS_BACKING_DIR
        || rel.starts_with(&format!("{VFS_BACKING_DIR}/"))
}

fn is_temp_file(rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    name.contains(".pds-part") || name.contains(".pdstmp-")
}

/// Short, unique temp sibling for a download target.
///
/// The final `rename` needs the temp in the same directory to stay atomic,
/// but appending `.pds-part` to a `NAME_MAX`-sized component overflows the
/// filesystem limit. Hash the target path instead, keeping the suffix and
/// directory (the stale-temp sweep keys off the suffix).
fn transfer_temp_rel(rel: &str) -> String {
    use sha1::Digest;
    let mut hasher = sha1::Sha1::new();
    hasher.update(rel.as_bytes());
    let name = format!(".pds-{:x}.pds-part", hasher.finalize());
    match rel.rsplit_once('/') {
        Some((dir, _)) => format!("{dir}/{name}"),
        None => name,
    }
}

/// Insert `suffix` before the extension: `a.txt` + ` (2)` → `a (2).txt`.
/// Dotfiles / extension-less names get the suffix appended whole (same
/// convention as `conflict_name`).
fn with_suffix_before_ext(name: &str, suffix: &str) -> String {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => format!("{stem}{suffix}.{ext}"),
        _ => format!("{name}{suffix}"),
    }
}

/// The F7 conflict path next to `rel`, deduped against the local disk and
/// the DB: `name (conflict YYYY-MM-DD HHmm).ext`, then `… (2).ext`, etc.
/// Never overwrites an existing file or tracked row. `base` is the content
/// root whose namespace the candidate must not collide with.
fn local_free_conflict_rel_in(db: &Db, base: &Path, rel: &str) -> Result<String> {
    local_free_conflict_rel_at(db, base, rel, chrono::Local::now())
}

fn local_free_conflict_rel_at(
    db: &Db,
    base: &Path,
    rel: &str,
    at: chrono::DateTime<chrono::Local>,
) -> Result<String> {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    let stamped = conflict_name(name, at);
    let with_name = |n: &str| match rel.rsplit_once('/') {
        Some((dir, _)) => format!("{dir}/{n}"),
        None => n.to_string(),
    };
    let mut candidate = stamped.clone();
    for i in 2.. {
        let rel_candidate = with_name(&candidate);
        if !to_abs(base, &rel_candidate).exists() && db.node_by_path(&rel_candidate)?.is_none() {
            return Ok(rel_candidate);
        }
        candidate = with_suffix_before_ext(&stamped, &format!(" ({i})"));
    }
    unreachable!("unbounded dedupe loop")
}

/// Rename the local file at `rel` to its F7 conflict name (deduped — an
/// existing conflict copy is never overwritten); returns the new relative
/// path. Any tracked row is re-keyed to the new path as dirty (pending
/// upload) so the renamed copy uploads under its conflict name. `content`
/// is where the file bytes live (visible root in mirror mode, backing dir
/// in placeholder mode — a conflict copy is content, never a placeholder).
fn rename_for_conflict_in(db: &Db, content: &Path, rel: &str) -> Result<String> {
    let abs = to_abs(content, rel);
    let new_rel = local_free_conflict_rel_in(db, content, rel)?;
    std::fs::rename(&abs, to_abs(content, &new_rel))?;
    if let Some(mut row) = db.node_by_path(rel)? {
        // Free the old path for the incoming remote node, then track the
        // conflicted local copy under its new path as dirty (to-upload).
        // Refresh content hashes so the upload queue doesn't see staleness.
        db.remove_node(rel)?;
        let new_abs = to_abs(content, &new_rel);
        row.local_path = new_rel.clone();
        row.name = new_rel.rsplit('/').next().unwrap_or(&new_rel).to_string();
        row.node_uid = format!("pending:{new_rel}");
        row.sync_state = SyncState::Dirty;
        if let Ok((sha1, size)) = sha1_file(&new_abs) {
            row.sha1 = Some(sha1);
            row.size = Some(size as i64);
        }
        row.mtime = std::fs::metadata(&new_abs)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64);
        db.upsert_node(&row)?;
    }
    Ok(new_rel)
}

/// `rename_for_conflict_in` against the visible root (mirror mode).
fn rename_for_conflict(db: &Db, root: &Path, rel: &str) -> Result<String> {
    rename_for_conflict_in(db, root, rel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SyncState;
    use crate::mode::META_SYNC_MODE;

    fn tracked_row(rel: &str, sha1: Option<String>) -> NodeRow {
        NodeRow {
            local_path: rel.to_string(),
            node_uid: format!("uid-{rel}"),
            parent_uid: Some("root".into()),
            name: rel.rsplit('/').next().unwrap_or(rel).to_string(),
            kind: NodeKind::File,
            size: None,
            sha1: sha1.clone(),
            mtime: None,
            mode: None,
            sync_state: SyncState::Synced,
            pinned: false,
            synced_sha1: sha1,
        }
    }

    /// A file row backed by a known remote uid (engine merge tests).
    fn row_at(rel: &str, uid: &str, state: SyncState) -> NodeRow {
        NodeRow {
            local_path: rel.to_string(),
            node_uid: uid.to_string(),
            parent_uid: Some("root".into()),
            name: rel.rsplit('/').next().unwrap_or(rel).to_string(),
            kind: NodeKind::File,
            size: None,
            sha1: None,
            mtime: None,
            mode: None,
            sync_state: state,
            pinned: false,
            synced_sha1: None,
        }
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        let base = Duration::from_secs(1);
        assert_eq!(retry_backoff(base, 1), Duration::from_secs(1));
        assert_eq!(retry_backoff(base, 2), Duration::from_secs(2));
        assert_eq!(retry_backoff(base, 3), Duration::from_secs(4));
        // 2^10 · base is far beyond the cap: clamp at RETRY_BACKOFF_MAX.
        assert_eq!(retry_backoff(base, 11), RETRY_BACKOFF_MAX);
        assert_eq!(retry_backoff(base, 500), RETRY_BACKOFF_MAX);
        // Tiny bases scale up before hitting the cap.
        assert_eq!(
            retry_backoff(Duration::from_millis(20), 4),
            Duration::from_millis(160)
        );
    }

    #[test]
    fn diverged_detects_content_change() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("a.txt");
        std::fs::write(&f, b"one").unwrap();
        let (sha1, _) = sha1_file(&f).unwrap();
        let row = tracked_row("a.txt", Some(sha1));
        assert!(!local_content_diverged(&row, &f));
        std::fs::write(&f, b"two").unwrap();
        assert!(local_content_diverged(&row, &f));
    }

    #[test]
    fn conflict_rename_moves_file_and_rekeys_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_memory().unwrap();
        std::fs::create_dir_all(dir.path().join("docs")).unwrap();
        let f = dir.path().join("docs/a.txt");
        std::fs::write(&f, b"local").unwrap();
        let (sha1, _) = sha1_file(&f).unwrap();
        db.upsert_node(&tracked_row("docs/a.txt", Some(sha1)))
            .unwrap();

        let new_rel = rename_for_conflict(&db, dir.path(), "docs/a.txt").unwrap();
        assert!(new_rel.starts_with("docs/a (conflict "));
        assert!(new_rel.ends_with(").txt"));
        assert!(!f.exists());
        assert_eq!(
            std::fs::read(to_abs(dir.path(), &new_rel)).unwrap(),
            b"local"
        );

        let row = db.node_by_path(&new_rel).unwrap().unwrap();
        assert_eq!(row.sync_state, SyncState::Dirty);
        assert!(row.node_uid.starts_with("pending:"));
        // Old path no longer tracked — the incoming remote node takes it.
        assert!(db.node_by_path("docs/a.txt").unwrap().is_none());
    }

    #[test]
    fn conflict_rename_untracked_file_leaves_db_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_memory().unwrap();
        std::fs::write(dir.path().join("b.bin"), b"x").unwrap();
        let new_rel = rename_for_conflict(&db, dir.path(), "b.bin").unwrap();
        assert!(new_rel.starts_with("b (conflict "));
        assert!(db.node_by_path("b.bin").unwrap().is_none());
        assert!(db.node_by_path(&new_rel).unwrap().is_none());
    }

    #[test]
    fn conflict_path_dedupes_existing_copies() {
        use chrono::TimeZone;
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open_memory().unwrap();
        let at = chrono::Local
            .with_ymd_and_hms(2026, 10, 24, 9, 30, 0)
            .unwrap();
        let stamped = conflict_name("a.txt", at);
        // A same-minute conflict copy already exists on disk…
        std::fs::write(dir.path().join(&stamped), b"older conflict").unwrap();
        // …and a doubly-conflicted one is tracked in the DB.
        let taken2 = with_suffix_before_ext(&stamped, " (2)");
        db.upsert_node(&tracked_row(&taken2, None)).unwrap();

        let rel = local_free_conflict_rel_at(&db, dir.path(), "a.txt", at).unwrap();
        assert_eq!(rel, with_suffix_before_ext(&stamped, " (3)"));
        // The existing copies are untouched.
        assert_eq!(
            std::fs::read(dir.path().join(&stamped)).unwrap(),
            b"older conflict"
        );
    }

    #[test]
    fn temp_file_detection_covers_sidecar_temps() {
        assert!(is_temp_file("docs/x.txt.pds-part"));
        assert!(is_temp_file("x.txt.pds-part.pdstmp-abc123"));
        assert!(is_temp_file("x.txt.pdstmp-abc123"));
        assert!(!is_temp_file("x.txt"));
        assert!(!is_temp_file("docs/report.pds.txt"));
    }

    #[test]
    fn transfer_temps_stay_next_to_the_target_within_name_max() {
        // Same directory so the final rename is atomic; short name so a
        // 255-byte target component still fits the temp (TG-14).
        let long = format!("{}.bin", "y".repeat(251));
        let tmp = transfer_temp_rel(&long);
        assert_eq!(tmp.len(), ".pds-".len() + 40 + ".pds-part".len());
        assert!(is_temp_file(&tmp));
        assert!(tmp.starts_with(".pds-"));
        assert_ne!(tmp, transfer_temp_rel(&format!("{long}.other")));

        let nested = transfer_temp_rel(&format!("docs/{long}"));
        assert_eq!(
            nested.rsplit_once('/').map(|(dir, _)| dir),
            Some("docs")
        );
        assert!(!nested.contains(&long));
    }

    /// External-backing layout (the desktop app): switching a mirror root
    /// to placeholders moves tracked AND untracked content into the backing
    /// cache without losing anything.
    #[test]
    fn convert_layout_to_placeholders_moves_all_content() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let backing = dir.path().join("backing");
        std::fs::create_dir_all(root.join("docs")).unwrap();
        std::fs::write(root.join("docs/a.txt"), b"tracked").unwrap();
        std::fs::write(root.join("untracked.txt"), b"never synced").unwrap();

        let db = Db::open_memory().unwrap();
        let (sha1, _) = sha1_file(&root.join("docs/a.txt")).unwrap();
        db.upsert_node(&tracked_row("docs/a.txt", Some(sha1))).unwrap();
        db.upsert_node(&NodeRow {
            kind: NodeKind::Folder,
            ..tracked_row("docs", None)
        })
        .unwrap();

        convert_layout(&db, &root, &backing, SyncMode::Placeholders).unwrap();

        // Tracked + untracked files moved; the root shows neither.
        assert_eq!(std::fs::read(backing.join("docs/a.txt")).unwrap(), b"tracked");
        assert_eq!(
            std::fs::read(backing.join("untracked.txt")).unwrap(),
            b"never synced"
        );
        assert!(!root.join("docs/a.txt").exists());
        assert!(!root.join("untracked.txt").exists());
        // The empty tracked dir was pruned.
        assert!(!root.join("docs").exists());

        // The untracked file registered as a dirty pending upload.
        let row = db.node_by_path("untracked.txt").unwrap().unwrap();
        assert_eq!(row.sync_state, SyncState::Dirty);
        assert!(row.node_uid.starts_with("pending:"));
        // The tracked row stayed synced (bytes unchanged → hash-consistent).
        let row = db.node_by_path("docs/a.txt").unwrap().unwrap();
        assert_eq!(row.sync_state, SyncState::Synced);

        // Idempotent: a repeat pass moves nothing and keeps the marker.
        convert_layout(&db, &root, &backing, SyncMode::Placeholders).unwrap();
        assert_eq!(
            std::fs::read_to_string(backing.join(LAYOUT_CONVERTED)).unwrap(),
            "placeholders"
        );
        assert_eq!(std::fs::read(backing.join("docs/a.txt")).unwrap(), b"tracked");
    }

    /// Switching back to mirror moves hydrated content into the root.
    #[test]
    fn convert_layout_to_mirror_moves_content_back() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let backing = dir.path().join("backing");
        std::fs::create_dir_all(backing.join("docs")).unwrap();
        std::fs::write(backing.join("docs/a.txt"), b"hydrated").unwrap();

        let db = Db::open_memory().unwrap();
        let (sha1, _) = sha1_file(&backing.join("docs/a.txt")).unwrap();
        db.upsert_node(&tracked_row("docs/a.txt", Some(sha1))).unwrap();

        convert_layout(&db, &root, &backing, SyncMode::Mirror).unwrap();

        assert_eq!(std::fs::read(root.join("docs/a.txt")).unwrap(), b"hydrated");
        assert!(!backing.join("docs/a.txt").exists());
    }

    /// Toggling modes back and forth must re-run the conversion each time
    /// (the marker records WHICH layout it produced, not just that it ran).
    #[test]
    fn convert_layout_round_trip_follows_every_toggle() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let backing = dir.path().join("backing");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.txt"), b"content a").unwrap();

        let db = Db::open_memory().unwrap();
        let (sha1, _) = sha1_file(&root.join("a.txt")).unwrap();
        db.upsert_node(&tracked_row("a.txt", Some(sha1))).unwrap();

        // mirror → placeholders: content moves into the backing cache.
        convert_layout(&db, &root, &backing, SyncMode::Placeholders).unwrap();
        assert_eq!(std::fs::read(backing.join("a.txt")).unwrap(), b"content a");
        assert!(!root.join("a.txt").exists());

        // placeholders → mirror: the SAME bytes move back (no re-download).
        convert_layout(&db, &root, &backing, SyncMode::Mirror).unwrap();
        assert_eq!(std::fs::read(root.join("a.txt")).unwrap(), b"content a");
        assert!(!backing.join("a.txt").exists());

        // …and once more into placeholders — the marker must not short-
        // circuit this second crossing.
        convert_layout(&db, &root, &backing, SyncMode::Placeholders).unwrap();
        assert_eq!(std::fs::read(backing.join("a.txt")).unwrap(), b"content a");
        assert!(!root.join("a.txt").exists());
    }

    /// The persisted `sync_mode` setting decides the effective mode for
    /// existing roots — the caller's flag is only the first-run default.
    #[tokio::test]
    async fn mode_resolution_persisted_wins() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let db = Db::open(&db_path).unwrap();
        db.set_meta(META_SYNC_MODE, SyncMode::Mirror.as_meta_value())
            .unwrap();
        drop(db);

        let (client, _mock) = pds_sidecar_client::MockSidecar::new().connect();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let (engine, _handle) = Engine::new(
            Arc::new(client),
            rx,
            EngineConfig {
                root: dir.path().join("root"),
                db_path,
                placeholders: true, // ignored: persisted mirror wins
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(engine.sync_mode(), SyncMode::Mirror);

        // First-run with no persisted value: the caller's default is stored.
        let dir2 = tempfile::tempdir().unwrap();
        let db_path2 = dir2.path().join("state.db");
        let (client2, _mock2) = pds_sidecar_client::MockSidecar::new().connect();
        let (_tx2, rx2) = tokio::sync::mpsc::channel(1);
        let (engine2, _handle2) = Engine::new(
            Arc::new(client2),
            rx2,
            EngineConfig {
                root: dir2.path().join("root"),
                db_path: db_path2,
                placeholders: false, // new root defaults to mirror here
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(engine2.sync_mode(), SyncMode::Mirror);
    }

    /// The production case: the save's tmp file finished uploading before
    /// the rename drained, so the source node is real. The merge must trash
    /// that node and keep the destination's (revision history), never call
    /// renameNode onto an occupied name — which 409s while the
    /// destination's overwrite-delete tombstone is still pending, and the
    /// requeue starves the tombstone drain (the 66-call hot loop).
    #[tokio::test]
    async fn overwrite_rename_merges_synced_source_into_destination() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let (client, mock) = pds_sidecar_client::MockSidecar::new().connect();
        let dest_uid = mock.remote_create_file("root", "base.plasticity", b"old");
        let src_uid = mock.remote_create_file("root", "tmp.plasticity", b"new");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let (mut engine, _handle) = Engine::new(
            Arc::new(client),
            rx,
            EngineConfig {
                root: dir.path().join("root"),
                db_path,
                placeholders: true,
                ..Default::default()
            },
        )
        .unwrap();
        let store = engine.store.clone().expect("placeholder store");
        engine
            .db
            .upsert_node(&row_at("base.plasticity", &dest_uid, SyncState::Synced))
            .unwrap();
        engine
            .db
            .upsert_node(&row_at("tmp.plasticity", &src_uid, SyncState::Synced))
            .unwrap();

        // What the VFS queues for `rename(tmp → base)` over an existing base.
        store.record_overwrite("base.plasticity", "tmp.plasticity", "base.plasticity");
        let Some(WorkRequest::Delete(rel)) = store.next_work_request() else {
            panic!("overwrite delete must drain first");
        };
        engine.handle_local_delete(&rel).await.unwrap();
        let Some(WorkRequest::Rename(old, new)) = store.next_work_request() else {
            panic!("rename must follow the delete");
        };
        engine.apply_local_rename(Some(&store), &old, &new).await.unwrap();

        // The tmp's node is dropped; the destination's node survives; no
        // remote rename was attempted (its name was never free).
        assert!(mock.trashed_uids().contains(&src_uid));
        assert!(!mock.trashed_uids().contains(&dest_uid));
        assert!(mock.renames().is_empty());
        assert!(engine.db.node_by_path("tmp.plasticity").unwrap().is_none());
        let dest = engine
            .db
            .node_by_path("base.plasticity")
            .unwrap()
            .expect("destination row");
        assert_eq!(dest.node_uid, dest_uid);
        assert_eq!(dest.sync_state, SyncState::Dirty);
        // The content is queued as a revision of that node.
        assert!(matches!(
            store.next_work_request(),
            Some(WorkRequest::Write(rel)) if rel == "base.plasticity"
        ));
    }

    /// Back-to-back saves: the previous save's revision upload has not
    /// drained, so the destination row is Dirty. The overwrite delete must
    /// keep it (the inbound rename's merge needs it) and the second merge
    /// must land on the same incumbent node.
    #[tokio::test]
    async fn back_to_back_overwrite_rename_keeps_incumbent_destination() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let (client, mock) = pds_sidecar_client::MockSidecar::new().connect();
        let dest_uid = mock.remote_create_file("root", "base.plasticity", b"old");
        let src_uid = mock.remote_create_file("root", "tmp2.plasticity", b"newer");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let (mut engine, _handle) = Engine::new(
            Arc::new(client),
            rx,
            EngineConfig {
                root: dir.path().join("root"),
                db_path,
                placeholders: true,
                ..Default::default()
            },
        )
        .unwrap();
        let store = engine.store.clone().expect("placeholder store");
        engine
            .db
            .upsert_node(&row_at("base.plasticity", &dest_uid, SyncState::Dirty))
            .unwrap();
        engine
            .db
            .upsert_node(&row_at("tmp2.plasticity", &src_uid, SyncState::Synced))
            .unwrap();

        store.record_overwrite("base.plasticity", "tmp2.plasticity", "base.plasticity");
        let Some(WorkRequest::Delete(rel)) = store.next_work_request() else {
            panic!("overwrite delete must drain first");
        };
        engine.handle_local_delete(&rel).await.unwrap();
        assert!(
            engine.db.node_by_path("base.plasticity").unwrap().is_some(),
            "an inbound rename keeps the dirty destination row"
        );
        let Some(WorkRequest::Rename(old, new)) = store.next_work_request() else {
            panic!("rename must follow the delete");
        };
        engine.apply_local_rename(Some(&store), &old, &new).await.unwrap();

        assert!(mock.trashed_uids().contains(&src_uid));
        let dest = engine.db.node_by_path("base.plasticity").unwrap().unwrap();
        assert_eq!(dest.node_uid, dest_uid);
        assert_eq!(dest.sync_state, SyncState::Dirty);
    }

    /// A plain rename (no local destination row) still moves the source's
    /// node under its new name — the merge must not hijack it.
    #[tokio::test]
    async fn rename_without_destination_row_rekeys_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let (client, mock) = pds_sidecar_client::MockSidecar::new().connect();
        let src_uid = mock.remote_create_file("root", "tmp.plasticity", b"new");
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let (mut engine, _handle) = Engine::new(
            Arc::new(client),
            rx,
            EngineConfig {
                root: dir.path().join("root"),
                db_path,
                placeholders: true,
                ..Default::default()
            },
        )
        .unwrap();
        let store = engine.store.clone().expect("placeholder store");
        engine
            .db
            .upsert_node(&row_at("tmp.plasticity", &src_uid, SyncState::Synced))
            .unwrap();

        store.record_rename("tmp.plasticity", "final.plasticity");
        let Some(WorkRequest::Rename(old, new)) = store.next_work_request() else {
            panic!("rename queued");
        };
        engine.apply_local_rename(Some(&store), &old, &new).await.unwrap();

        assert_eq!(
            mock.renames(),
            vec![(src_uid.clone(), "final.plasticity".to_string())]
        );
        assert!(mock.trashed_uids().is_empty());
        assert!(engine.db.node_by_path("tmp.plasticity").unwrap().is_none());
        let row = engine
            .db
            .node_by_path("final.plasticity")
            .unwrap()
            .expect("re-keyed row");
        assert_eq!(row.node_uid, src_uid);
    }
}
