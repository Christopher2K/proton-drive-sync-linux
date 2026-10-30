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
use tracing::{debug, info, warn};

use crate::conflict::conflict_name;
use crate::db::{Db, NodeRow, SyncState, Tombstone};
use crate::guard::DeleteGuard;
use crate::store::{Store, VNode};

/// Name of the engine-private directory inside the sync root (DB, socket).
pub const STATE_DIR: &str = ".pds";
/// Backing store for the FUSE placeholder VFS, inside the root but invisible
/// to the sync pipeline (M3): never watched, never uploaded, never trashed.
pub const VFS_BACKING_DIR: &str = ".pds-vfs-backing";
const META_INITIAL_SYNC_DONE: &str = "initial_sync_done";

/// First backoff delay after a retryable upload failure (F2).
pub const RETRY_BASE_DEFAULT: Duration = Duration::from_secs(1);
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
}

impl EngineHandle {
    /// The engine↔VFS store, when the engine runs in placeholder mode.
    pub fn store(&self) -> Option<&Store> {
        self.store.as_ref()
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
    /// Placeholder mode (M3): synced files are NOT materialized in `root`;
    /// a FUSE filesystem serves them with on-demand hydration via
    /// [`EngineHandle::store`]. Default `false` (proven materialized mode).
    pub placeholders: bool,
    /// Backing-cache size cap for placeholder mode (M3.2 eviction). When
    /// hydrated content exceeds this, the least-recently-used files are
    /// dehydrated (backing content deleted; placeholder stays). `None`
    /// disables eviction (tests, explicit opt-out).
    pub eviction_cap: Option<u64>,
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
    /// Activity feed broadcast (UI); sends are no-ops without receivers.
    activity_tx: broadcast::Sender<ActivityItem>,
}

impl<S: SidecarApi + 'static> Engine<S> {
    pub fn new(
        sidecar: Arc<S>,
        notifications: mpsc::Receiver<Notification>,
        config: EngineConfig,
    ) -> Result<(Self, EngineHandle)> {
        std::fs::create_dir_all(&config.root)?;
        let db = Db::open(&config.db_path)?;
        let (tx, cmd_rx) = mpsc::channel(64);
        let (activity_tx, _) = broadcast::channel(256);
        let placeholders = config.placeholders;
        // The engine and the handle share one store (engine owns a clone).
        let vfs_store = if placeholders {
            Some(Store::new())
        } else {
            None
        };
        let store = vfs_store.clone();
        let engine = Self {
            sidecar,
            db,
            root: config.root.clone(),
            placeholders,
            store,
            backing_dir: config.root.join(".pds-vfs-backing"),
            eviction_cap: config.eviction_cap,
            notifications,
            backlog: VecDeque::new(),
            cmd_rx,
            state: EngineState::Running,
            guard: DeleteGuard::default(),
            buffered_events: VecDeque::new(),
            approved_tombstones: HashSet::new(),
            retry_at: HashMap::new(),
            retry_base: config.retry_base,
            scope_id: None,
            active_transfer: None,
            pending_pins: VecDeque::new(),
            shutting_down: false,
            started: AtomicBool::new(false),
            activity_tx: activity_tx.clone(),
        };
        Ok((
            engine,
            EngineHandle {
                tx,
                activity_tx,
                store: vfs_store,
            },
        ))
    }

    /// Run until shutdown. Consumes the engine.
    pub async fn run(mut self) -> Result<()> {
        self.started.store(true, Ordering::SeqCst);

        // 1. Initial sync (first run only; F1).
        let root_node = self.sidecar.get_root().await.context("drive.getRoot")?;
        self.scope_id = Some(root_node.tree_event_scope_id.clone());
        self.db.set_meta("root_uid", &root_node.uid)?;
        if self.db.meta(META_INITIAL_SYNC_DONE)? != Some("1".into()) {
            info!(root = %self.root.display(), "initial sync: enumerating remote tree");
            self.initial_sync(&root_node).await?;
            self.db.set_meta(META_INITIAL_SYNC_DONE, "1")?;
            info!("initial sync complete");
        }

        // 2. Resume the event stream from the persisted cursor (F6).
        let cursor = self.db.event_cursor(&root_node.tree_event_scope_id)?;
        self.sidecar
            .events_start(&root_node.tree_event_scope_id, cursor.as_deref())
            .await
            .context("drive.events.start")?;

        // 3. Filesystem watcher with ~500 ms debounce (F2).
        let (fs_tx, mut fs_rx) = mpsc::channel::<Vec<DebouncedEvent>>(256);
        let debouncer = self.start_watcher(fs_tx)?;

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
                if let Some(rel) = store.next_hydrate_request() {
                    self.hydrate_one(&store, &rel).await;
                    // Hydration may have pushed the cache over the cap;
                    // the fresh file itself is protected from eviction.
                    self.maybe_evict(&store, &rel);
                    continue;
                }
                if let Some(rel) = store.next_write_request() {
                    self.upload_write_one(&store, &rel).await;
                    continue;
                }
                if let Some(rel) = store.next_delete_request() {
                    if let Err(e) = self.handle_local_delete(&rel).await {
                        warn!(path = %rel, error = %e, "write-through delete failed");
                    }
                    if let Err(e) = self.publish_vfs_snapshot(&store) {
                        warn!(error = %e, "post-delete VFS snapshot failed");
                    }
                    continue;
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

    /// First-run enumeration + full materialization (M1 downloads everything;
    /// VFS placeholders arrive in M3/M4). Also uploads local-only files.
    /// Never deletes anything, in either direction.
    async fn initial_sync(&mut self, root: &Node) -> Result<()> {
        self.enumerate_remote(root).await?;

        if self.placeholders {
            // M3: nothing to download — the VFS serves placeholders; files
            // hydrate on open. Local-only files still upload (first-run
            // rule: an empty folder never deletes anything).
            let local_only = self.scan_local_only().await?;
            for rel in local_only {
                self.upsert_local_file(&rel).await?;
            }
            return Ok(());
        }

        // Materialize: download every remote file that is missing or changed.
        let files = self.remote_files_needing_download()?;
        for (rel, uid) in files {
            let abs = to_abs(&self.root, &rel);
            if let Some(dir) = abs.parent() {
                std::fs::create_dir_all(dir)?;
            }
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
                        if !abs.exists() {
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
        self.db.upsert_node(&NodeRow {
            local_path: rel.to_string(),
            node_uid: node.uid.clone(),
            parent_uid: node.parent_uid.clone(),
            name: node.name.clone(),
            kind: node.kind,
            size: node.size,
            sha1: node.sha1.clone(),
            mtime: node.mtime,
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
                let Some(rel) = to_rel(&self.root, &path) else {
                    continue;
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
        // Queue of (rel, is_dir) to process, parent dirs before their
        // children so uploads can rely on the remote folder chain.
        let mut queue: Vec<(String, bool)> = Vec::new();
        let mut seen = HashSet::new();
        for ev in events {
            let path = ev.path;
            let Some(rel) = to_rel(&self.root, &path) else {
                continue;
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
            } else if abs.is_file() {
                self.upsert_local_file(&rel).await?;
            } else {
                self.handle_local_delete(&rel).await?;
            }
        }
        Ok(())
    }

    /// Collect the live children of `rel` (recursively, parent-first) that
    /// are not already in `seen`. Recovers the inotify recursive-watch race:
    /// contents created in the same breath as their parent folder never
    /// produce watcher events of their own.
    fn sweep_new_dir(&self, rel: &str, seen: &mut HashSet<String>, queue: &mut Vec<(String, bool)>) {
        let entries = match std::fs::read_dir(to_abs(&self.root, rel)) {
            Ok(entries) => entries,
            Err(_) => return, // vanished or unreadable: nothing to recover
        };
        for entry in entries.flatten() {
            let child_rel = format!("{rel}/{}", entry.file_name().to_string_lossy());
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
        let abs = self.content_path(rel);
        let meta = match std::fs::metadata(&abs) {
            Ok(m) => m,
            Err(_) => return Ok(()), // vanished again — delete path handles it
        };
        let (sha1, size) = sha1_file(&abs)?;
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64);
        let name = rel.rsplit('/').next().unwrap_or(rel).to_string();

        let (parent_uid, node_uid, synced_sha1, fresh_content) = match self.db.node_by_path(rel)? {
            Some(row) => {
                if row.sync_state == SyncState::Synced && row.sha1.as_deref() == Some(&sha1) {
                    return Ok(()); // content unchanged
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
                    std::fs::rename(&abs, to_abs(&self.root, &new_rel))?;
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
        let abs = to_abs(&self.root, &conflict_rel);
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp_rel = format!("{conflict_rel}.pds-part");
        let tmp = to_abs(&self.root, &tmp_rel);
        let sidecar = Arc::clone(&self.sidecar);
        self.run_transfer(
            "download",
            Some(&conflict_rel),
            sidecar.download(&node.uid, &tmp),
        )
        .await?;
        std::fs::rename(&tmp, &abs)?;
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
            sync_state: SyncState::Dirty,
            pinned: false,
            synced_sha1: None,
        })?;
        Ok(conflict_rel)
    }

    /// Pick a conflict path next to `rel`: deduped against disk + DB
    /// locally, then (best effort) against remote sibling names.
    async fn fresh_conflict_rel(&self, rel: &str, row: &NodeRow) -> Result<String> {
        let local = local_free_conflict_rel(&self.db, &self.root, rel)?;
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
                if !to_abs(&self.root, &remote_rel).exists()
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
        let (code, message, retryable) = match failure {
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
            } => (code, message, retryable),
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
            let backoff = retry_backoff(self.retry_base, count);
            self.retry_at
                .insert(rel.to_string(), Instant::now() + backoff);
            info!(path = %rel, count, backoff = ?backoff, "retryable upload error; backing off");
        }
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

    /// Local delete detected by the watcher. EXACTLY per SPEC F3:
    /// - only nodes present in the DB with `sync_state = synced` propagate;
    /// - unknown / never-synced items are simply forgotten locally;
    /// - remote action is trash only; the mass-delete guard auto-pauses.
    async fn handle_local_delete(&mut self, rel: &str) -> Result<()> {
        let Some(row) = self.db.node_by_path(rel)? else {
            debug!(path = %rel, "delete of unknown local item: no remote action");
            return Ok(());
        };
        if row.sync_state != SyncState::Synced {
            // Never confirmed synced → local removal only (F3.1).
            self.db.remove_node(rel)?;
            if let Some(store) = &self.store {
                store.forget(rel);
                std::fs::remove_file(to_abs(&self.backing_dir, rel)).ok();
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
        if let Some(store) = &self.store {
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
                        self.download_file(&uid, &rel).await?;
                    }
                }
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

    /// Fetch a node and reconcile it locally (download/create).
    async fn apply_node_upsert(&mut self, uid: &str) -> Result<()> {
        let node = match self.sidecar.get_node(uid).await {
            Ok(n) => n,
            Err(e) => {
                debug!(uid = %uid, error = %e, "node vanished before fetch; skipping");
                return Ok(());
            }
        };
        let rel = self.local_path_for(&node)?;
        let abs = to_abs(&self.root, &rel);

        if node.is_folder() {
            if !self.placeholders && !abs.exists() {
                std::fs::create_dir_all(&abs)?;
            }
            self.upsert_from_node(&node, &rel, SyncState::Synced)?;
            return Ok(());
        }

        // File: keep-both whenever the local copy carries content the remote
        // doesn't know about (F7): dirty/uploading rows, untracked files, or
        // on-disk content diverging from the last synced hash.
        if abs.is_file() {
            let diverged = match self.db.node_by_path(&rel)? {
                Some(row) => {
                    row.sync_state != SyncState::Synced || local_content_diverged(&row, &abs)
                }
                None => true, // untracked local file in the way
            };
            if diverged {
                let conflicted = rename_for_conflict(&self.db, &self.root, &rel)?;
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
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir)?;
        }
        self.download_file(uid, &rel).await
    }

    /// Compute the local relative path for a remote node by walking parent
    /// uids through the DB. Unknown parents fall back to the sync root.
    fn local_path_for(&self, node: &Node) -> Result<String> {
        let mut parts = vec![node.name.clone()];
        let mut parent = node.parent_uid.clone();
        let mut depth = 0;
        while let Some(uid) = parent {
            depth += 1;
            if depth > 64 {
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
        let abs = to_abs(&self.root, &row.local_path);
        if row.sync_state != SyncState::Synced {
            // Local unsynced changes → keep both (F7); the dirty row will
            // re-upload under an available name. Unpark it so the upload
            // path can re-key it (the remote uid is gone).
            self.clear_upload_blocks(&row.local_path)?;
            info!(path = %row.local_path, "remote delete of dirty local node: keeping local copy");
            return Ok(());
        }
        if abs.is_dir() {
            std::fs::remove_dir_all(&abs).ok();
        } else {
            std::fs::remove_file(&abs).ok();
        }
        // Placeholder mode: drop the hydrated backing copy too.
        if self.placeholders
            && let Some(store) = &self.store
        {
            store.clear_backing(&row.local_path);
            std::fs::remove_file(to_abs(&self.backing_dir, &row.local_path)).ok();
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
        to_abs(content_root(self.placeholders, &self.root, &self.backing_dir), rel)
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
                    content_ready: to_abs(&self.backing_dir, &row.local_path).is_file(),
                },
            );
        }
        store.publish_snapshot(by_path);
        Ok(())
    }

    /// Hydrate one requested placeholder: download to the backing dir,
    /// register the backing file, refresh the snapshot. Failures just
    /// unblock the waiting reader (it retries with EIO → new open).
    async fn hydrate_one(&mut self, store: &Store, rel: &str) {
        let Some(row) = self.db.node_by_path(rel).ok().flatten() else {
            store.finish_hydrate(rel, false);
            return;
        };
        match self.download_to_backing(&row.node_uid, rel).await {
            Ok(()) => {
                store.set_backing(rel, to_abs(&self.backing_dir, rel));
                if let Err(e) = self.publish_vfs_snapshot(store) {
                    warn!(path = %rel, error = %e, "post-hydrate snapshot failed");
                }
                info!(path = %rel, "hydrated on demand");
            }
            Err(e) => {
                warn!(path = %rel, error = %e, "hydration failed");
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
    async fn download_to_backing(&mut self, uid: &str, rel: &str) -> Result<()> {
        let abs = to_abs(&self.backing_dir, rel);
        if abs.is_file() {
            return Ok(()); // already hydrated (lost wake-up)
        }
        if let Some(dir) = abs.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = to_abs(&self.backing_dir, &format!("{rel}.pds-part"));
        let sidecar = Arc::clone(&self.sidecar);
        let outcome = self
            .run_transfer("download", Some(rel), sidecar.download(uid, &tmp))
            .await?;
        let _ = outcome;
        std::fs::rename(&tmp, &abs)?;
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
        store.finish_write(rel, result.is_ok());
        if let Err(e) = self.publish_vfs_snapshot(store) {
            warn!(error = %e, "post-write VFS snapshot failed");
        }
    }

    /// DB row creation + upload for a write-through file. Mirrors
    /// [`Engine::upsert_local_file`] but sources content from the backing
    /// dir and never renames on collision (the VFS file stays where the
    /// user put it; the collision path uploads under an available name
    /// instead and re-keys the row).
    async fn write_upload_row(&mut self, rel: &str) -> Result<()> {
        let abs = to_abs(&self.backing_dir, rel);
        let meta = std::fs::metadata(&abs).context("backing file vanished")?;
        let (sha1, size) = sha1_file(&abs)?;
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64);
        let name = rel.rsplit('/').next().unwrap_or(rel).to_string();

        let (parent_uid, node_uid, synced_sha1) = match self.db.node_by_path(rel)? {
            Some(row) => {
                if row.sync_state == SyncState::Synced && row.sha1.as_deref() == Some(&sha1) {
                    return Ok(()); // content unchanged (spurious close)
                }
                // Revision of an existing (possibly pending) node.
                (
                    row.parent_uid.clone(),
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
                    sync_state: SyncState::Dirty,
                    pinned: false,
                    synced_sha1: None,
                })?;
                (None, None, None)
            }
        };

        if let Some(row) = self.db.node_by_path(rel)? {
            self.db.upsert_node(&NodeRow {
                local_path: rel.to_string(),
                node_uid: node_uid.unwrap_or_else(|| row.node_uid.clone()),
                parent_uid,
                name: row.name.clone(),
                kind: NodeKind::File,
                size: Some(size as i64),
                sha1: Some(sha1),
                mtime,
                sync_state: SyncState::Dirty,
                pinned: row.pinned,
                synced_sha1,
            })?;
            self.db.clear_upload_failures(rel).ok();
            self.retry_at.remove(rel);

            if self.state != EngineState::Running {
                debug!(path = %rel, "paused: write-through upload buffered");
                return Ok(());
            }
            self.upload_one(rel).await?;
        }
        Ok(())
    }

    /// Download `uid` to `rel` and mark the row synced.
    async fn download_file(&mut self, uid: &str, rel: &str) -> Result<()> {
        let abs = self.content_path(rel);
        let node = self.sidecar.get_node(uid).await.ok();
        let tmp_rel = format!("{rel}.pds-part");
        let tmp = self.content_path(&tmp_rel);
        let sidecar = Arc::clone(&self.sidecar);
        let outcome = self
            .run_transfer("download", Some(rel), sidecar.download(uid, &tmp))
            .await?;
        let _ = outcome;
        std::fs::rename(&tmp, &abs)?;
        self.db.upsert_node(&NodeRow {
            local_path: rel.to_string(),
            node_uid: uid.to_string(),
            parent_uid: node.as_ref().and_then(|n| n.parent_uid.clone()),
            name: rel.rsplit('/').next().unwrap_or(rel).to_string(),
            kind: NodeKind::File,
            size: node.as_ref().and_then(|n| n.size),
            sha1: node.as_ref().and_then(|n| n.sha1.clone()),
            mtime: node.as_ref().and_then(|n| n.mtime),
            sync_state: SyncState::Synced,
            pinned: false,
            synced_sha1: node.as_ref().and_then(|n| n.sha1.clone()),
        })?;
        self.clear_upload_blocks(rel)?;
        info!(path = %rel, "downloaded");
        self.emit_activity(ActivityKind::Downloaded, rel, None);
        Ok(())
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
                        Some(Notification::TransferError { transfer_id: tid, code, message, retryable })
                            if tid == transfer_id =>
                        {
                            break Err(TransferFailure::Remote { code, message, retryable });
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
                });
            }
            Command::Shutdown => {}
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

/// Absolute path from a root-relative '/'-separated path.
pub fn to_abs(root: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(root.to_path_buf(), |p, c| p.join(c))
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
/// Never overwrites an existing file or tracked row.
fn local_free_conflict_rel(db: &Db, root: &Path, rel: &str) -> Result<String> {
    local_free_conflict_rel_at(db, root, rel, chrono::Local::now())
}

fn local_free_conflict_rel_at(
    db: &Db,
    root: &Path,
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
        if !to_abs(root, &rel_candidate).exists() && db.node_by_path(&rel_candidate)?.is_none() {
            return Ok(rel_candidate);
        }
        candidate = with_suffix_before_ext(&stamped, &format!(" ({i})"));
    }
    unreachable!("unbounded dedupe loop")
}

/// Rename the local file at `rel` to its F7 conflict name (deduped — an
/// existing conflict copy is never overwritten); returns the new relative
/// path. Any tracked row is re-keyed to the new path as dirty (pending
/// upload) so the renamed copy uploads under its conflict name.
fn rename_for_conflict(db: &Db, root: &Path, rel: &str) -> Result<String> {
    let abs = to_abs(root, rel);
    let new_rel = local_free_conflict_rel(db, root, rel)?;
    std::fs::rename(&abs, to_abs(root, &new_rel))?;
    if let Some(mut row) = db.node_by_path(rel)? {
        // Free the old path for the incoming remote node, then track the
        // conflicted local copy under its new path as dirty (to-upload).
        // Refresh content hashes so the upload queue doesn't see staleness.
        db.remove_node(rel)?;
        let new_abs = to_abs(root, &new_rel);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SyncState;

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
            sync_state: SyncState::Synced,
            pinned: false,
            synced_sha1: sha1,
        }
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
}
