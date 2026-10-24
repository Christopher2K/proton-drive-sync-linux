//! In-process, deterministic mock sidecar for tests.
//!
//! Speaks the real JSON-RPC protocol (`docs/protocol.md`) over any async
//! stream, so it exercises the exact framing/correlation code paths of the
//! production client — no real Proton account required.
//!
//! Determinism: node uids / transfer ids / event ids come from counters, the
//! clock is fixed, and transfers "complete" synchronously (response first,
//! then the `transfer.done` notification, per the protocol's ordering).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

#[derive(Debug)]
struct State {
    nodes: HashMap<String, MockNode>,
    next_node: u64,
    next_transfer: u64,
    next_event: u64,
    scope_id: String,
    /// Call log for assertions: ("trash", uids), ("upload", name), ...
    calls: Vec<(String, Vec<String>)>,
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
    fn emit(&self, params: Value) {
        let msg = json!({"jsonrpc": "2.0", "method": "drive.event", "params": params}).to_string();
        let _ = self.outbox.send(msg);
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
                let st = self.state.lock().unwrap();
                vec![ok(st.node_json(st.nodes.get("root").unwrap()))]
            }
            "drive.listChildren" => {
                let st = self.state.lock().unwrap();
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
                st.calls.push(("upload".into(), vec![name.clone()]));

                let content = std::fs::read(&path).unwrap_or_default();
                let uid = if let Some(existing) = node_uid {
                    // New revision of an existing node.
                    if let Some(n) = st.nodes.get_mut(&existing) {
                        n.size = Some(content.len() as i64);
                        n.sha1 = Some(hex_sha1(&content));
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
                            mtime: Some(1_700_000_000),
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
                let transfer_id = format!("transfer-{}", st.next_transfer);
                let content = st
                    .nodes
                    .get(&pstr("nodeUid"))
                    .and_then(|n| n.content.clone())
                    .unwrap_or_default();
                let _ = std::fs::write(pstr("path"), &*content);
                let done = json!({
                    "jsonrpc": "2.0",
                    "method": "transfer.done",
                    "params": {"transferId": transfer_id},
                })
                .to_string();
                vec![ok(json!({"transferId": transfer_id})), done]
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
            "transfer.pause" | "transfer.resume" | "transfer.cancel" => vec![ok(json!({}))],
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
