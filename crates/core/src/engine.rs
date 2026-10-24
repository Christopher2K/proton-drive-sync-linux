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

use anyhow::{Context, Result, anyhow};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{DebouncedEvent, Debouncer, new_debouncer};
use pds_sidecar_client::{
    DriveEvent, EventKind, Node, NodeKind, Notification, SidecarApi, UploadParams,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::conflict::conflict_name;
use crate::db::{Db, NodeRow, SyncState, Tombstone};
use crate::guard::DeleteGuard;

/// Name of the engine-private directory inside the sync root (DB, socket).
pub const STATE_DIR: &str = ".pds";
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
}

/// Control commands (CLI socket / UI).
pub enum Command {
    Pause,
    Resume,
    Shutdown,
    Status(oneshot::Sender<EngineStatus>),
}

/// Handle to a running engine.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<Command>,
}

impl EngineHandle {
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
}

pub struct EngineConfig {
    /// Local sync root folder.
    pub root: PathBuf,
    /// Path of the SQLite state DB (usually `<root>/.pds/state.db`).
    pub db_path: PathBuf,
    /// Base of the exponential upload retry backoff (normally
    /// `RETRY_BASE_DEFAULT`; tests shrink it).
    pub retry_base: Duration,
}

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
    /// Set when Shutdown arrived mid-transfer; the main loop exits ASAP.
    shutting_down: bool,
    /// Set once `run()` has been entered.
    started: AtomicBool,
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
        let engine = Self {
            sidecar,
            db,
            root: config.root,
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
            shutting_down: false,
            started: AtomicBool::new(false),
        };
        Ok((engine, EngineHandle { tx }))
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

        // 4. Main loop.
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
                    let abs = to_abs(&self.root, &rel);
                    if !abs.exists() {
                        std::fs::create_dir_all(&abs)?;
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
                if rel == STATE_DIR || rel.starts_with(&format!("{STATE_DIR}/")) {
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
        let mut seen = HashSet::new();
        for ev in events {
            let path = ev.path;
            let Some(rel) = to_rel(&self.root, &path) else {
                continue;
            };
            if rel.is_empty() || rel == STATE_DIR || rel.starts_with(&format!("{STATE_DIR}/")) {
                continue;
            }
            if !seen.insert(rel.clone()) {
                continue; // debounce coalescing within the batch
            }
            let abs = to_abs(&self.root, &rel);
            if rel.ends_with(".pds-part") {
                continue; // our own temp download files
            }
            if abs.is_dir() {
                self.ensure_remote_folder_async(&rel).await?;
            } else if abs.is_file() {
                self.upsert_local_file(&rel).await?;
            } else {
                self.handle_local_delete(&rel).await?;
            }
        }
        Ok(())
    }

    /// Earliest backoff expiry among scheduled retries, if any. Stale
    /// entries are harmless (the wake just finds nothing eligible).
    fn next_retry_wake(&self) -> Option<Instant> {
        self.retry_at.values().copied().min()
    }

    /// Create/write-close on a file: hash, then upload (new node or new
    /// revision) or mark dirty while paused (F4 buffering).
    async fn upsert_local_file(&mut self, rel: &str) -> Result<()> {
        let abs = to_abs(&self.root, rel);
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
        // Terminally errored or waiting out a retry backoff: leave it alone
        // (F2 retry policy — no hot loop).
        if self.db.upload_error(rel)?.is_some() {
            return Ok(());
        }
        if self.retry_at.get(rel).is_some_and(|t| *t > Instant::now()) {
            return Ok(());
        }
        let abs = to_abs(&self.root, rel);
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

    /// Pick a conflict path next to `rel` that is free locally, in the DB,
    /// and (best effort) remotely.
    async fn fresh_conflict_rel(&self, rel: &str, row: &NodeRow) -> Result<String> {
        let name = rel.rsplit('/').next().unwrap_or(rel);
        let stamped = conflict_name(name, chrono::Local::now());
        let with_name = |n: &str| match rel.rsplit_once('/') {
            Some((dir, _)) => format!("{dir}/{n}"),
            None => n.to_string(),
        };
        // Local/DB collision (same-minute double conflict): disambiguate.
        let mut candidate = stamped;
        for i in 2.. {
            let abs = to_abs(&self.root, &with_name(&candidate));
            if !abs.exists() && self.db.node_by_path(&with_name(&candidate))?.is_none() {
                break;
            }
            candidate = conflict_name(&format!("{name} ({i})"), chrono::Local::now());
        }
        // Remote collision: ask the Drive side for an available name.
        if let Some(parent) = row.parent_uid.clone() {
            let available = self.sidecar.get_available_name(&parent, &candidate).await;
            if let Ok(available) = available {
                let abs = to_abs(&self.root, &with_name(&available));
                if !abs.exists() && self.db.node_by_path(&with_name(&available))?.is_none() {
                    candidate = available;
                }
            }
        }
        Ok(with_name(&candidate))
    }

    /// Perform the actual upload transfer for a dirty row.
    async fn upload_row(&mut self, rel: &str, row: &NodeRow) -> Result<()> {
        let abs = to_abs(&self.root, rel);
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
        self.db.clear_upload_failures(rel)?;
        self.retry_at.remove(rel);
        info!(path = %rel, "uploaded");
        Ok(())
    }

    /// F2/F7 upload failure policy (e2e-m1 bug #1 follow-up):
    /// - 409 "revision no longer up to date" class → keep-both (download the
    ///   fresh remote head as a conflict copy) and retry ONCE atop it; a
    ///   second failure marks the node errored instead of looping;
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
            warn!(path = %rel, error = %message, "revision upload rejected (stale base); keeping both");
            match self.sidecar.get_node(&row.node_uid).await {
                Ok(node) => {
                    let conflict_rel = self
                        .download_remote_head_as_conflict(rel, row, &node)
                        .await?;
                    if let Some(mut r) = self.db.node_by_path(rel)? {
                        // Record the fresh head so later preconditions see it.
                        r.synced_sha1 = node.sha1.clone();
                        self.db.upsert_node(&r)?;
                    }
                    info!(path = %rel, conflict = %conflict_rel, "retrying upload atop fresh remote head");
                }
                Err(e) => {
                    // Node gone remotely: re-upload as a NEW file rather than
                    // reviving a trashed node — local content survives.
                    warn!(path = %rel, error = %e, "remote node vanished; re-uploading as new file");
                    if let Some(mut r) = self.db.node_by_path(rel)? {
                        r.node_uid = format!("pending:{rel}");
                        r.sync_state = SyncState::Dirty;
                        self.db.upsert_node(&r)?;
                    }
                }
            }
            return self.retry_upload_once(rel).await;
        }

        if !retryable {
            self.db.mark_upload_errored(rel, &message)?;
            self.retry_at.remove(rel);
            warn!(path = %rel, code, error = %message, "non-retryable upload error; node marked errored");
            return Ok(());
        }
        let count = self.db.record_retryable_failure(rel)?;
        if count >= MAX_UPLOAD_RETRIES {
            self.db.mark_upload_errored(rel, &message)?;
            self.retry_at.remove(rel);
            warn!(path = %rel, count, error = %message, "upload failed too many times; node marked errored");
        } else {
            let backoff = retry_backoff(self.retry_base, count);
            self.retry_at
                .insert(rel.to_string(), Instant::now() + backoff);
            info!(path = %rel, count, backoff = ?backoff, "retryable upload error; backing off");
        }
        Ok(())
    }

    /// The single retry granted after a keep-both conflict resolution. Any
    /// second failure marks the node errored — never loops.
    async fn retry_upload_once(&mut self, rel: &str) -> Result<()> {
        let Some(row) = self.db.node_by_path(rel)? else {
            return Ok(());
        };
        if row.sync_state != SyncState::Dirty {
            return Ok(());
        }
        let abs = to_abs(&self.root, rel);
        if !abs.is_file() {
            return Ok(());
        }
        let params = upload_params(&row, &abs);
        self.db.set_sync_state(rel, SyncState::Uploading)?;
        let sidecar = Arc::clone(&self.sidecar);
        let outcome = self
            .run_transfer("upload", Some(rel), sidecar.upload(&params))
            .await;
        match outcome {
            Ok(done) => self.finish_upload_success(rel, &row, done),
            Err(TransferFailure::Aborted(e)) => {
                if self.db.node_by_path(rel)?.is_some() {
                    self.db.set_sync_state(rel, SyncState::Dirty)?;
                }
                Err(e)
            }
            Err(TransferFailure::Remote {
                code,
                message,
                retryable,
            }) => {
                warn!(path = %rel, code, retryable, error = %message, "second upload attempt failed; node marked errored");
                if self.db.node_by_path(rel)?.is_some() {
                    self.db.set_sync_state(rel, SyncState::Dirty)?;
                    self.db.mark_upload_errored(rel, &message)?;
                }
                Ok(())
            }
        }
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
            return Ok(());
        }
        // Durable tombstone first; drained immediately if running (F3.4).
        self.db.add_tombstone(&Tombstone {
            local_path: rel.to_string(),
            node_uid: row.node_uid.clone(),
            deleted_at: now_unix(),
        })?;
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
                let files = self.remote_files_needing_download()?;
                for (rel, uid) in files {
                    self.download_file(&uid, &rel).await?;
                }
            }
        }
        // Persist the resume token after applying (protocol.md F6).
        self.db.set_event_cursor(&ev.scope_id, &ev.event_id)?;
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
            if !abs.exists() {
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
            }
        }

        if let Some(row) = self.db.node_by_path(&rel)?
            && row.sync_state == SyncState::Synced
            && row.sha1 == node.sha1
        {
            return Ok(()); // already current
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
            // re-upload under an available name.
            info!(path = %row.local_path, "remote delete of dirty local node: keeping local copy");
            return Ok(());
        }
        if abs.is_dir() {
            std::fs::remove_dir_all(&abs).ok();
        } else {
            std::fs::remove_file(&abs).ok();
        }
        self.remove_node_recursive(&row.local_path)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Transfers
    // ------------------------------------------------------------------

    /// Download `uid` to `rel` and mark the row synced.
    async fn download_file(&mut self, uid: &str, rel: &str) -> Result<()> {
        let abs = to_abs(&self.root, rel);
        let node = self.sidecar.get_node(uid).await.ok();
        let tmp_rel = format!("{rel}.pds-part");
        let tmp = to_abs(&self.root, &tmp_rel);
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
        info!(path = %rel, "downloaded");
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
                });
            }
            Command::Shutdown => {}
        }
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
            return Ok(true);
        }
        if let Some(ev) = self.buffered_events.pop_front() {
            self.apply_event(ev).await?;
            return Ok(true);
        }
        // Uploads: skip terminally errored nodes and rows waiting out a
        // retry backoff (F2 — no hot loop).
        let errored: HashSet<String> = self.db.errored_paths()?.into_iter().collect();
        let now = Instant::now();
        if let Some(row) = self
            .db
            .nodes_with_state(SyncState::Dirty)?
            .into_iter()
            .find(|r| {
                !errored.contains(&r.local_path)
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

/// Rename the local file at `rel` to its F7 conflict name; returns the new
/// relative path. Any tracked row is re-keyed to the new path as dirty
/// (pending upload) so the renamed copy uploads under its conflict name.
fn rename_for_conflict(db: &Db, root: &Path, rel: &str) -> Result<String> {
    let abs = to_abs(root, rel);
    let name = abs
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("bad file name for {rel}"))?;
    let new_name = conflict_name(name, chrono::Local::now());
    let new_rel = match rel.rsplit_once('/') {
        Some((dir, _)) => format!("{dir}/{new_name}"),
        None => new_name,
    };
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
}
