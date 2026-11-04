//! In-process, deterministic mock sidecar for tests.
//!
//! Speaks the real JSON-RPC protocol (`docs/protocol.md`) over any async
//! stream, so it exercises the exact framing/correlation code paths of the
//! production client — no real Proton account required.
//!
//! Determinism: node uids / transfer ids / event ids come from counters, the
//! clock is fixed, and transfers "complete" synchronously (response first,
//! then the `transfer.done` notification, per the protocol's ordering).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::client::SidecarClient;
use crate::jsonrpc::JsonRpcClient;
use crate::types::{DriveEvent, error_codes};

#[derive(Debug, Clone)]
struct MockNode {
    uid: String,
    parent_uid: Option<String>,
    name: String,
    kind: &'static str, // "file" | "folder"
    size: Option<i64>,
    mtime: Option<i64>,
    sha1: Option<String>,
    content: Option<Arc<Vec<u8>>>, // files only
    trashed: bool,
}

/// Scripted upload failures for a node uid: `(code, message, retryable,
/// retry_after)` tuples, popped one per `drive.upload` attempt.
type ScriptedFailures = VecDeque<(i64, String, bool, Option<Duration>)>;

/// A download parked mid-stream by [`MockHandle::hold_next_download`]: the
/// sidecar wrote only part of the content and waits for `transfer.resume`
/// before finishing and emitting `transfer.done` (F4 pause semantics).
#[derive(Debug)]
struct HeldDownload {
    path: String,
    content: Arc<Vec<u8>>,
    written: usize,
    done: Value,
}

#[derive(Debug)]
struct State {
    nodes: HashMap<String, MockNode>,
    next_node: u64,
    next_transfer: u64,
    next_event: u64,
    scope_id: String,
    /// Call log for assertions: ("trash", uids), ("upload", name), ...
    calls: Vec<(String, Vec<String>)>,
    /// `(name, mtime)` of every `drive.upload` request, in call order.
    upload_mtimes: Vec<(String, i64)>,
    /// Scripted `transfer.error`s for revision uploads, per node uid; popped
    /// one per `drive.upload` attempt (content is NOT applied on failure).
    upload_failures: HashMap<String, ScriptedFailures>,
    /// Scripted `transfer.error`s for downloads, per node uid; popped one per
    /// `drive.download` attempt (nothing is written on failure).
    download_failures: HashMap<String, ScriptedFailures>,
    /// Remaining `drive.getRoot` calls to reject with a transport error.
    get_root_failures: usize,
    /// Remaining `drive.listChildren` calls to reject with a transport error.
    list_children_failures: usize,
    /// Node uids whose next download stops after this many bytes until the
    /// matching `transfer.resume` arrives (or `transfer.cancel` drops it).
    download_holds: HashMap<String, usize>,
    /// Parked downloads by transfer id (see [`HeldDownload`]).
    held_downloads: HashMap<String, HeldDownload>,
    /// Outbound notification queue (drained by the server task).
    outbox: mpsc::UnboundedSender<String>,
}

impl State {
    fn node_json(&self, n: &MockNode) -> Value {
        json!({
            "uid": n.uid,
            "parentUid": n.parent_uid,
            "name": n.name,
            "type": n.kind,
            "size": n.size,
            "mtime": n.mtime,
            "sha1": n.sha1,
            "treeEventScopeId": self.scope_id,
        })
    }

    fn children_of(&self, uid: &str) -> Vec<&MockNode> {
        let mut v: Vec<_> = self
            .nodes
            .values()
            .filter(|n| !n.trashed && n.parent_uid.as_deref() == Some(uid))
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }
}

/// Test handle: inspect calls and inject remote events while a
/// `MockSidecar` server is running.
#[derive(Clone)]
pub struct MockHandle {
    state: Arc<Mutex<State>>,
}

impl MockHandle {
    /// Uids passed to `drive.trash`, in call order.
    pub fn trashed_uids(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == "trash")
            .flat_map(|(_, u)| u.clone())
            .collect()
    }

    /// Uids passed to `drive.restore`, in call order.
    pub fn restored_uids(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == "restore")
            .flat_map(|(_, u)| u.clone())
            .collect()
    }

    /// `(uid, newName)` pairs passed to `drive.rename`, in call order.
    pub fn renames(&self) -> Vec<(String, String)> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == "rename")
            .map(|(_, u)| {
                (
                    u.first().cloned().unwrap_or_default(),
                    u.get(1).cloned().unwrap_or_default(),
                )
            })
            .collect()
    }

    /// `(uid, newParentUid)` pairs passed to `drive.move`, in call order.
    pub fn moves(&self) -> Vec<(String, String)> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == "move")
            .map(|(_, u)| {
                (
                    u.first().cloned().unwrap_or_default(),
                    u.get(1).cloned().unwrap_or_default(),
                )
            })
            .collect()
    }

    /// Names passed to `drive.upload`, in call order.
    pub fn uploaded_names(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == "upload")
            .flat_map(|(_, u)| u.clone())
            .collect()
    }

    /// `(name, mtime)` of every `drive.upload` request, in call order.
    pub fn uploaded_mtimes(&self) -> Vec<(String, i64)> {
        self.state.lock().unwrap().upload_mtimes.clone()
    }

    /// Current mtime of a remote node (Unix seconds).
    pub fn node_mtime(&self, uid: &str) -> Option<i64> {
        self.state
            .lock()
            .unwrap()
            .nodes
            .get(uid)
            .and_then(|n| n.mtime)
    }

    /// Current (non-trashed) remote children names of a folder.
    pub fn child_names(&self, parent_uid: &str) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .children_of(parent_uid)
            .iter()
            .map(|n| n.name.clone())
            .collect()
    }

    /// Look up a node uid by (parent uid, name).
    pub fn find_uid(&self, parent_uid: &str, name: &str) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .children_of(parent_uid)
            .iter()
            .find(|n| n.name == name)
            .map(|n| n.uid.clone())
    }

    /// Inject a remote file under `parent_uid` and emit a `node_created`
    /// event, as if another client created it.
    pub fn remote_create_file(&self, parent_uid: &str, name: &str, content: &[u8]) -> String {
        let mut st = self.state.lock().unwrap();
        let uid = st.alloc_uid();
        let node = MockNode {
            uid: uid.clone(),
            parent_uid: Some(parent_uid.to_string()),
            name: name.to_string(),
            kind: "file",
            size: Some(content.len() as i64),
            mtime: Some(1_700_000_000),
            sha1: Some(hex_sha1(content)),
            content: Some(Arc::new(content.to_vec())),
            trashed: false,
        };
        st.nodes.insert(uid.clone(), node);
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_created",
            "nodeUid": uid,
            "parentNodeUid": parent_uid,
        }));
        uid
    }

    /// Change a remote file's content and emit a `node_updated` event.
    pub fn remote_update_file(&self, uid: &str, content: &[u8]) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.size = Some(content.len() as i64);
            n.sha1 = Some(hex_sha1(content));
            n.content = Some(Arc::new(content.to_vec()));
            n.trashed = false;
        }
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_updated",
            "nodeUid": uid,
        }));
    }

    /// Rename a remote node in place and emit a `node_updated` event, as if
    /// another client renamed it.
    pub fn remote_rename(&self, uid: &str, new_name: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.name = new_name.to_string();
        }
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_updated",
            "nodeUid": uid,
        }));
    }

    /// Move a remote node under `new_parent_uid` and emit a `node_updated`
    /// event, as if another client moved it.
    pub fn remote_move(&self, uid: &str, new_parent_uid: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.parent_uid = Some(new_parent_uid.to_string());
        }
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_updated",
            "nodeUid": uid,
        }));
    }

    /// Inject a remote folder under `parent_uid` and emit a `node_created`
    /// event, as if another client created it. Returns its uid.
    pub fn remote_create_folder(&self, parent_uid: &str, name: &str) -> String {
        let mut st = self.state.lock().unwrap();
        let uid = st.alloc_uid();
        st.nodes.insert(
            uid.clone(),
            MockNode {
                uid: uid.clone(),
                parent_uid: Some(parent_uid.to_string()),
                name: name.to_string(),
                kind: "folder",
                size: None,
                mtime: Some(1_700_000_000),
                sha1: None,
                content: None,
                trashed: false,
            },
        );
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_created",
            "nodeUid": uid,
            "parentNodeUid": parent_uid,
        }));
        uid
    }

    /// Move a remote node under `new_parent_uid` WITHOUT emitting an event —
    /// for tests that need to control event ordering (e.g. deliver the child
    /// update before the parent folder's).
    pub fn remote_move_silent(&self, uid: &str, new_parent_uid: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.parent_uid = Some(new_parent_uid.to_string());
        }
    }

    /// Inject a remote folder under `parent_uid` WITHOUT emitting an event.
    pub fn remote_create_folder_silent(&self, parent_uid: &str, name: &str) -> String {
        let mut st = self.state.lock().unwrap();
        let uid = st.alloc_uid();
        st.nodes.insert(
            uid.clone(),
            MockNode {
                uid: uid.clone(),
                parent_uid: Some(parent_uid.to_string()),
                name: name.to_string(),
                kind: "folder",
                size: None,
                mtime: Some(1_700_000_000),
                sha1: None,
                content: None,
                trashed: false,
            },
        );
        uid
    }

    /// Trash a remote node and emit the real event shape (`node_updated`
    /// with `isTrashed: true`), as Proton reports a trash from another
    /// client.
    pub fn remote_trash(&self, uid: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.trashed = true;
        }
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_updated",
            "nodeUid": uid,
            "isTrashed": true,
        }));
    }

    /// Restore a remote node and emit the real event shape (`node_updated`
    /// with `isTrashed: false`), the counterpart of [`MockHandle::remote_trash`].
    pub fn remote_restore(&self, uid: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.trashed = false;
        }
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_updated",
            "nodeUid": uid,
            "isTrashed": false,
        }));
    }

    /// Uids passed to `drive.download`, in call order.
    pub fn downloaded_uids(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == "download")
            .flat_map(|(_, u)| u.clone())
            .collect()
    }

    /// Freeze the next download of `node_uid` after `bytes` bytes until the
    /// engine sends `transfer.resume` (or `transfer.cancel`). Lets a test
    /// hold a transfer in flight without sleeps — the mock is the transfer
    /// authority, exactly like the real sidecar's pause controller.
    pub fn hold_next_download(&self, node_uid: &str, bytes: usize) {
        self.state
            .lock()
            .unwrap()
            .download_holds
            .insert(node_uid.to_string(), bytes);
    }

    /// Number of downloads currently parked waiting for `transfer.resume`.
    pub fn held_downloads(&self) -> usize {
        self.state.lock().unwrap().held_downloads.len()
    }

    /// Transfer-control methods the engine sent, in call order
    /// (`pause` / `resume` / `cancel`).
    pub fn transfer_controls(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| matches!(m.as_str(), "pause" | "resume" | "cancel"))
            .map(|(m, _)| m.clone())
            .collect()
    }

    /// Remote file size in bytes, if the uid is a file.
    pub fn file_size(&self, uid: &str) -> Option<i64> {
        self.state.lock().unwrap().nodes.get(uid).and_then(|n| n.size)
    }

    /// Hex SHA-1 of a remote file's current content.
    pub fn file_sha1(&self, uid: &str) -> Option<String> {
        self.state.lock().unwrap().nodes.get(uid).and_then(|n| n.sha1.clone())
    }

    /// Emit a `node_deleted` event for a uid and remove it remotely.
    pub fn remote_delete(&self, uid: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.trashed = true;
        }
        let event_id = st.alloc_event_id();
        let scope = st.scope_id.clone();
        st.emit(json!({
            "scopeId": scope,
            "eventId": event_id,
            "type": "node_deleted",
            "nodeUid": uid,
        }));
    }

    /// Change a remote file's content WITHOUT emitting an event — simulates
    /// an edit on another device whose event hasn't reached us yet (event
    /// stream lag, e2e-m1 bug #1).
    pub fn remote_update_file_silent(&self, uid: &str, content: &[u8]) {
        let mut st = self.state.lock().unwrap();
        if let Some(n) = st.nodes.get_mut(uid) {
            n.size = Some(content.len() as i64);
            n.sha1 = Some(hex_sha1(content));
            n.content = Some(Arc::new(content.to_vec()));
            n.trashed = false;
        }
    }

    /// Current remote content of a file node.
    pub fn file_content(&self, uid: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .unwrap()
            .nodes
            .get(uid)
            .and_then(|n| n.content.as_ref().map(|c| (**c).clone()))
    }

    /// Script failures for the next `n` revision-upload attempts of
    /// `node_uid`: each entry is `(code, message, retryable, retry_after)`
    /// as sent in the `transfer.error` notification. The upload attempt IS
    /// logged (so `upload_attempts` counts it) but content is not applied.
    pub fn inject_upload_failures(&self, node_uid: &str, failures: &[(i64, &str, bool)]) {
        self.inject_upload_failures_full(
            node_uid,
            &failures.iter().map(|(c, m, r)| (*c, *m, *r, None)).collect::<Vec<_>>(),
        );
    }

    /// Like [`MockSidecar::inject_upload_failures`] with a server
    /// `retryAfter` hint (seconds) per failure.
    pub fn inject_upload_failures_full(
        &self,
        node_uid: &str,
        failures: &[(i64, &str, bool, Option<u64>)],
    ) {
        let mut st = self.state.lock().unwrap();
        st.upload_failures.insert(
            node_uid.to_string(),
            failures
                .iter()
                .map(|(c, m, r, ra)| {
                    (
                        *c,
                        (*m).to_string(),
                        *r,
                        ra.map(std::time::Duration::from_secs),
                    )
                })
                .collect(),
        );
    }

    /// Script failures for the next download attempts of `node_uid`. Each
    /// entry is `(code, message, retryable)` sent as a `transfer.error`
    /// notification; the target file is NOT written on failure. Mirrors
    /// [`MockHandle::inject_upload_failures`] for the download direction.
    pub fn inject_download_failures(&self, node_uid: &str, failures: &[(i64, &str, bool)]) {
        self.inject_download_failures_full(
            node_uid,
            &failures
                .iter()
                .map(|(c, m, r)| (*c, *m, *r, None))
                .collect::<Vec<_>>(),
        );
    }

    /// Like [`MockHandle::inject_download_failures`] with a server
    /// `retryAfter` hint (seconds) per failure.
    pub fn inject_download_failures_full(
        &self,
        node_uid: &str,
        failures: &[(i64, &str, bool, Option<u64>)],
    ) {
        let mut st = self.state.lock().unwrap();
        st.download_failures.insert(
            node_uid.to_string(),
            failures
                .iter()
                .map(|(c, m, r, ra)| {
                    (
                        *c,
                        (*m).to_string(),
                        *r,
                        ra.map(std::time::Duration::from_secs),
                    )
                })
                .collect(),
        );
    }

    /// Reject the next `times` `drive.getRoot` calls with a transport error,
    /// simulating starting offline / while the sidecar cannot reach Proton.
    pub fn fail_get_root(&self, times: usize) {
        self.state.lock().unwrap().get_root_failures = times;
    }

    /// Reject the next `times` `drive.listChildren` calls with a transport
    /// error (initial enumeration cannot complete).
    pub fn fail_list_children(&self, times: usize) {
        self.state.lock().unwrap().list_children_failures = times;
    }

    /// How many `drive.upload` calls (any outcome) were made for `name`.
    pub fn upload_attempts(&self, name: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, a)| m == "upload" && a.iter().any(|n| n == name))
            .count()
    }

    /// Emit a raw drive event (for custom scenarios).
    pub fn emit_event(&self, event: &DriveEvent) {
        let st = self.state.lock().unwrap();
        st.emit(serde_json::to_value(event).expect("DriveEvent serializes"));
    }
}

impl State {
    fn alloc_uid(&mut self) -> String {
        self.next_node += 1;
        format!("node-{}", self.next_node)
    }
    fn alloc_event_id(&mut self) -> String {
        self.next_event += 1;
        format!("evt-{}", self.next_event)
    }
    fn enqueue(&self, msg: Value) {
        let _ = self.outbox.send(msg.to_string());
    }
    fn emit(&self, params: Value) {
        self.enqueue(json!({"jsonrpc": "2.0", "method": "drive.event", "params": params}));
    }
}

fn hex_sha1(bytes: &[u8]) -> String {
    use sha1::Digest;
    let mut h = sha1::Sha1::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// A deterministic in-process sidecar.
pub struct MockSidecar {
    state: Arc<Mutex<State>>,
}

impl Default for MockSidecar {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSidecar {
    /// Mock tree: root `root` containing folder `Documents` (empty) and file
    /// `hello.txt` ("hello drive").
    pub fn new() -> Self {
        let (outbox, _) = mpsc::unbounded_channel();
        let mut nodes = HashMap::new();
        let scope_id = "scope-1".to_string();
        nodes.insert(
            "root".to_string(),
            MockNode {
                uid: "root".into(),
                parent_uid: None,
                name: "".into(),
                kind: "folder",
                size: None,
                mtime: None,
                sha1: None,
                content: None,
                trashed: false,
            },
        );
        nodes.insert(
            "node-docs".into(),
            MockNode {
                uid: "node-docs".into(),
                parent_uid: Some("root".into()),
                name: "Documents".into(),
                kind: "folder",
                size: None,
                mtime: Some(1_700_000_000),
                sha1: None,
                content: None,
                trashed: false,
            },
        );
        let content = b"hello drive".to_vec();
        nodes.insert(
            "node-hello".into(),
            MockNode {
                uid: "node-hello".into(),
                parent_uid: Some("root".into()),
                name: "hello.txt".into(),
                kind: "file",
                size: Some(content.len() as i64),
                mtime: Some(1_700_000_000),
                sha1: Some(hex_sha1(&content)),
                content: Some(Arc::new(content)),
                trashed: false,
            },
        );
        Self {
            state: Arc::new(Mutex::new(State {
                nodes,
                next_node: 100,
                next_transfer: 0,
                next_event: 0,
                scope_id,
                calls: Vec::new(),
                upload_mtimes: Vec::new(),
                upload_failures: HashMap::new(),
                download_failures: HashMap::new(),
                get_root_failures: 0,
                list_children_failures: 0,
                download_holds: HashMap::new(),
                held_downloads: HashMap::new(),
                outbox,
            })),
        }
    }

    pub fn handle(&self) -> MockHandle {
        MockHandle {
            state: Arc::clone(&self.state),
        }
    }

    /// Connect a `SidecarClient` to this mock over an in-memory duplex stream
    /// and run the server in the background.
    pub fn connect(self) -> (SidecarClient, MockHandle) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (cr, cw) = tokio::io::split(client_io);
        let (sr, sw) = tokio::io::split(server_io);
        let handle = self.handle();
        tokio::spawn(self.serve(sr, sw));
        let (rpc, notifications) = JsonRpcClient::new(cr, cw);
        (SidecarClient::from_rpc(rpc, notifications), handle)
    }

    /// Run the JSON-RPC server loop until the peer closes.
    pub async fn serve<R, W>(self, reader: R, writer: W)
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        // Rebind the outbox to a channel we can drain here.
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        self.state.lock().unwrap().outbox = tx;

        let mut writer = writer;
        let mut lines = BufReader::new(reader).lines();
        loop {
            tokio::select! {
                line = lines.next_line() => {
                    match line {
                        Ok(Some(line)) if !line.trim().is_empty() => {
                            let responses = self.handle_line(&line);
                            for r in responses {
                                if writer.write_all(r.as_bytes()).await.is_err()
                                    || writer.write_all(b"\n").await.is_err()
                                {
                                    return;
                                }
                            }
                            let _ = writer.flush().await;
                        }
                        Ok(_) => return, // EOF
                        Err(_) => return,
                    }
                }
                Some(notif) = rx.recv() => {
                    if writer.write_all(notif.as_bytes()).await.is_err()
                        || writer.write_all(b"\n").await.is_err()
                    {
                        return;
                    }
                    let _ = writer.flush().await;
                }
            }
        }
    }

    /// Dispatch one request line; returns response lines (usually one; the
    /// fork login also emits the `auth.signInUrl` notification first).
    fn handle_line(&self, line: &str) -> Vec<String> {
        let Ok(msg) = serde_json::from_str::<Value>(line) else {
            return vec![];
        };
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let params = msg.get("params").cloned().unwrap_or(Value::Null);

        let ok = |result: Value| json!({"jsonrpc":"2.0","id":id,"result":result}).to_string();
        let err = |code: i64, message: &str| {
            json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}).to_string()
        };
        let pstr = |k: &str| {
            params
                .get(k)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };

        match method {
            "auth.loginFork" => {
                let notif = json!({
                    "jsonrpc": "2.0",
                    "method": "auth.signInUrl",
                    "params": {"url": "https://account.proton.me/desktop/login?mock=1", "userCode": "MOCK-CODE"},
                })
                .to_string();
                vec![notif, ok(json!({"email": "mock@proton.me"}))]
            }
            "auth.status" => vec![ok(json!({"loggedIn": true, "email": "mock@proton.me"}))],
            "auth.logout" => vec![ok(json!({}))],
            "drive.getRoot" => {
                let mut st = self.state.lock().unwrap();
                if st.get_root_failures > 0 {
                    st.get_root_failures -= 1;
                    return vec![err(error_codes::INTERNAL, "transport error: getRoot failed")];
                }
                vec![ok(st.node_json(st.nodes.get("root").unwrap()))]
            }
            "drive.listChildren" => {
                let mut st = self.state.lock().unwrap();
                if st.list_children_failures > 0 {
                    st.list_children_failures -= 1;
                    return vec![err(error_codes::INTERNAL, "transport error: listChildren failed")];
                }
                let nodes: Vec<Value> = st
                    .children_of(&pstr("parentUid"))
                    .iter()
                    .map(|n| st.node_json(n))
                    .collect();
                vec![ok(json!({"nodes": nodes}))]
            }
            "drive.getNode" => {
                let st = self.state.lock().unwrap();
                match st.nodes.get(&pstr("uid")).filter(|n| !n.trashed) {
                    Some(n) => vec![ok(st.node_json(n))],
                    None => vec![err(-32602, "no such node")],
                }
            }
            "drive.createFolder" => {
                let mut st = self.state.lock().unwrap();
                let parent = pstr("parentUid");
                let name = pstr("name");
                if st.children_of(&parent).iter().any(|n| n.name == name) {
                    return vec![err(error_codes::CONFLICT, "name already exists")];
                }
                let uid = st.alloc_uid();
                let node = MockNode {
                    uid: uid.clone(),
                    parent_uid: Some(parent),
                    name,
                    kind: "folder",
                    size: None,
                    mtime: Some(1_700_000_000),
                    sha1: None,
                    content: None,
                    trashed: false,
                };
                st.nodes.insert(uid.clone(), node);
                let json = st.node_json(st.nodes.get(&uid).unwrap());
                vec![ok(json)]
            }
            "drive.getAvailableName" => {
                let st = self.state.lock().unwrap();
                let parent = pstr("parentUid");
                let name = pstr("name");
                let taken: Vec<String> = st
                    .children_of(&parent)
                    .iter()
                    .map(|n| n.name.clone())
                    .collect();
                vec![ok(json!({"name": available_name(&taken, &name)}))]
            }
            "drive.rename" => {
                let mut st = self.state.lock().unwrap();
                let uid = pstr("nodeUid");
                let new_name = pstr("newName");
                st.calls.push(("rename".into(), vec![uid.clone(), new_name.clone()]));
                let (parent, trashed) = match st.nodes.get(&uid) {
                    Some(n) => (n.parent_uid.clone(), n.trashed),
                    None => return vec![err(-32602, "no such node")],
                };
                if trashed {
                    return vec![err(-32602, "no such node")];
                }
                // Same-name collision against non-trashed siblings.
                if let Some(parent) = parent {
                    let clash = st.nodes.values().any(|other| {
                        !other.trashed
                            && other.uid != uid
                            && other.parent_uid.as_deref() == Some(parent.as_str())
                            && other.name == new_name
                    });
                    if clash {
                        return vec![err(error_codes::CONFLICT, "name already exists")];
                    }
                }
                st.nodes.get_mut(&uid).unwrap().name = new_name;
                let json = st.node_json(st.nodes.get(&uid).unwrap());
                vec![ok(json!({"node": json}))]
            }
            "drive.move" => {
                let mut st = self.state.lock().unwrap();
                let uid = pstr("nodeUid");
                let new_parent = pstr("newParentUid");
                st.calls.push(("move".into(), vec![uid.clone(), new_parent.clone()]));
                let fail =
                    |e: &str| json!({"results": [{"uid": uid, "ok": false, "error": e}]});
                let (name, trashed, parent_known) = match st.nodes.get(&uid) {
                    Some(n) => (n.name.clone(), n.trashed, st.nodes.contains_key(&new_parent)),
                    None => return vec![ok(fail("no such node"))],
                };
                if trashed || !parent_known {
                    return vec![ok(fail("no such node"))];
                }
                let clash = st.nodes.values().any(|other| {
                    !other.trashed
                        && other.uid != uid
                        && other.parent_uid.as_deref() == Some(new_parent.as_str())
                        && other.name == name
                });
                if clash {
                    return vec![ok(fail("name already exists"))];
                }
                st.nodes.get_mut(&uid).unwrap().parent_uid = Some(new_parent);
                vec![ok(json!({"results": [{"uid": uid, "ok": true}]}))]
            }
            "drive.trash" => {
                let mut st = self.state.lock().unwrap();
                let uids: Vec<String> = params
                    .get("uids")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                st.calls.push(("trash".into(), uids.clone()));
                let results: Vec<Value> = uids
                    .iter()
                    .map(|u| match st.nodes.get_mut(u) {
                        Some(n) => {
                            n.trashed = true;
                            json!({"uid": u, "ok": true, "error": null})
                        }
                        None => json!({"uid": u, "ok": false, "error": "no such node"}),
                    })
                    .collect();
                vec![ok(json!({"results": results}))]
            }
            "drive.restore" => {
                let mut st = self.state.lock().unwrap();
                let uids: Vec<String> = params
                    .get("uids")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                st.calls.push(("restore".into(), uids.clone()));
                let results: Vec<Value> = uids
                    .iter()
                    .map(|u| match st.nodes.get_mut(u) {
                        Some(n) => {
                            n.trashed = false;
                            json!({"uid": u, "ok": true, "error": null})
                        }
                        None => json!({"uid": u, "ok": false, "error": "no such node"}),
                    })
                    .collect();
                vec![ok(json!({"results": results}))]
            }
            "drive.listTrashed" => {
                let st = self.state.lock().unwrap();
                let mut nodes: Vec<&MockNode> = st.nodes.values().filter(|n| n.trashed).collect();
                nodes.sort_by(|a, b| a.name.cmp(&b.name));
                vec![ok(json!({"nodes": nodes.iter().map(|n| st.node_json(n)).collect::<Vec<_>>()}))]
            }
            "drive.upload" => {
                let mut st = self.state.lock().unwrap();
                st.next_transfer += 1;
                let transfer_id = format!("transfer-{}", st.next_transfer);
                let name = pstr("name");
                let path = pstr("path");
                let node_uid = params
                    .get("nodeUid")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let parent_uid = params
                    .get("parentUid")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let mtime = params
                    .get("mtime")
                    .and_then(Value::as_i64)
                    .unwrap_or(1_700_000_000);
                st.calls.push(("upload".into(), vec![name.clone()]));
                st.upload_mtimes.push((name.clone(), mtime));

                // Scripted failure (e.g. the 409 stale-revision class):
                // respond with the transfer id, then a transfer.error —
                // content is NOT applied, like a server-side rejection.
                if let Some(existing) = &node_uid
                    && let Some((code, message, retryable, retry_after)) = st
                        .upload_failures
                        .get_mut(existing)
                        .and_then(VecDeque::pop_front)
                {
                    let mut failure = json!({
                        "transferId": transfer_id, "code": code, "message": message, "retryable": retryable,
                    });
                    if let Some(secs) = retry_after {
                        failure["retryAfter"] = json!(secs.as_secs_f64());
                    }
                    let err = json!({
                        "jsonrpc": "2.0",
                        "method": "transfer.error",
                        "params": failure,
                    })
                    .to_string();
                    return vec![ok(json!({"transferId": transfer_id})), err];
                }

                let content = std::fs::read(&path).unwrap_or_default();
                let uid = if let Some(existing) = node_uid {
                    // New revision of an existing node.
                    if let Some(n) = st.nodes.get_mut(&existing) {
                        n.size = Some(content.len() as i64);
                        n.sha1 = Some(hex_sha1(&content));
                        n.mtime = Some(mtime);
                        n.content = Some(Arc::new(content));
                        n.trashed = false;
                    }
                    existing
                } else {
                    let uid = st.alloc_uid();
                    st.nodes.insert(
                        uid.clone(),
                        MockNode {
                            uid: uid.clone(),
                            parent_uid,
                            name,
                            kind: "file",
                            size: Some(content.len() as i64),
                            mtime: Some(mtime),
                            sha1: Some(hex_sha1(&content)),
                            content: Some(Arc::new(content)),
                            trashed: false,
                        },
                    );
                    uid
                };
                // Response first, then completion notification (protocol order).
                let done = json!({
                    "jsonrpc": "2.0",
                    "method": "transfer.done",
                    "params": {"transferId": transfer_id, "nodeUid": uid, "nodeRevisionUid": format!("{uid}-rev-1")},
                })
                .to_string();
                vec![ok(json!({"transferId": transfer_id})), done]
            }
            "drive.download" => {
                let mut st = self.state.lock().unwrap();
                st.next_transfer += 1;
                let node_uid = pstr("nodeUid");
                let uid = node_uid.clone();
                let path = pstr("path");
                st.calls.push(("download".into(), vec![node_uid.clone()]));
                let transfer_id = format!("transfer-{}", st.next_transfer);
                // Scripted failure (local ENOSPC/EACCES reported by the
                // sidecar, or a transient transport error): respond with the
                // transfer id, then a transfer.error — no file is written.
                if let Some((code, message, retryable, retry_after)) =
                    st.download_failures.get_mut(&uid).and_then(VecDeque::pop_front)
                {
                    let mut failure = json!({
                        "transferId": transfer_id, "code": code, "message": message, "retryable": retryable,
                    });
                    if let Some(secs) = retry_after {
                        failure["retryAfter"] = json!(secs.as_secs_f64());
                    }
                    let err = json!({
                        "jsonrpc": "2.0",
                        "method": "transfer.error",
                        "params": failure,
                    })
                    .to_string();
                    return vec![ok(json!({"transferId": transfer_id})), err];
                }
                let content = st
                    .nodes
                    .get(&node_uid)
                    .and_then(|n| n.content.clone())
                    .unwrap_or_default();
                let done = json!({
                    "jsonrpc": "2.0",
                    "method": "transfer.done",
                    "params": {"transferId": transfer_id},
                });
                if let Some(hold_after) = st.download_holds.remove(&node_uid) {
                    // Park mid-stream: write the prefix, keep the completion
                    // for `transfer.resume`, like a paused real transfer.
                    let written = hold_after.min(content.len());
                    let _ = std::fs::write(&path, &content[..written]);
                    st.held_downloads.insert(
                        transfer_id.clone(),
                        HeldDownload {
                            path,
                            content,
                            written,
                            done,
                        },
                    );
                    return vec![ok(json!({"transferId": transfer_id}))];
                }
                let _ = std::fs::write(&path, &*content);
                st.enqueue(done);
                vec![ok(json!({"transferId": transfer_id}))]
            }
            "drive.events.start" => {
                let mut st = self.state.lock().unwrap();
                let event_id = st.alloc_event_id();
                let scope = st.scope_id.clone();
                let ff = json!({
                    "jsonrpc": "2.0",
                    "method": "drive.event",
                    "params": {"scopeId": scope, "eventId": event_id, "type": "fast_forward"},
                })
                .to_string();
                vec![ok(json!({})), ff]
            }
            "drive.events.stop" => vec![ok(json!({}))],
            "transfer.pause" => {
                let tid = pstr("transferId");
                self.state
                    .lock()
                    .unwrap()
                    .calls
                    .push(("pause".into(), vec![tid]));
                vec![ok(json!({}))]
            }
            "transfer.resume" => {
                let tid = pstr("transferId");
                let mut st = self.state.lock().unwrap();
                st.calls.push(("resume".into(), vec![tid.clone()]));
                if let Some(held) = st.held_downloads.remove(&tid) {
                    // Finish the parked transfer: write the remaining bytes,
                    // then emit completion (after the response, per protocol).
                    let mut content = Vec::with_capacity(held.content.len());
                    content.extend_from_slice(&held.content[..held.written]);
                    content.extend_from_slice(&held.content[held.written..]);
                    let _ = std::fs::write(&held.path, &content);
                    st.enqueue(held.done);
                }
                vec![ok(json!({}))]
            }
            "transfer.cancel" => {
                let tid = pstr("transferId");
                let mut st = self.state.lock().unwrap();
                st.calls.push(("cancel".into(), vec![tid.clone()]));
                st.held_downloads.remove(&tid);
                vec![ok(json!({}))]
            }
            _ => vec![err(-32601, "method not found")],
        }
    }
}

/// Deterministic "make it fit" naming: `name`, `name (1).ext`, `name (2).ext`, …
fn available_name(taken: &[String], wanted: &str) -> String {
    if !taken.iter().any(|t| t == wanted) {
        return wanted.to_string();
    }
    let (stem, ext) = match wanted.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s.to_string(), format!(".{e}")),
        _ => (wanted.to_string(), String::new()),
    };
    for i in 1..1000 {
        let candidate = format!("{stem} ({i}){ext}");
        if !taken.iter().any(|t| t == &candidate) {
            return candidate;
        }
    }
    format!("{stem} (x){ext}")
}
