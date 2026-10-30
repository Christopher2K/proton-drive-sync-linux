//! Types mirroring `docs/protocol.md` (Sidecar IPC protocol v1).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// JSON-RPC error codes beyond the standard ones (protocol.md).
pub mod error_codes {
    pub const AUTH_REQUIRED: i64 = 1001;
    pub const SESSION_EXPIRED: i64 = 1002;
    pub const RATE_LIMITED: i64 = 1003;
    pub const INTEGRITY_ERROR: i64 = 1004;
    pub const CONFLICT: i64 = 1005;
    pub const INTERNAL: i64 = 1099;
}

/// A Drive node as returned by `drive.*` methods.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub uid: String,
    #[serde(rename = "parentUid")]
    pub parent_uid: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub kind: NodeKind,
    pub size: Option<i64>,
    /// Unix seconds.
    pub mtime: Option<i64>,
    /// Hex SHA-1, files only.
    pub sha1: Option<String>,
    #[serde(rename = "treeEventScopeId")]
    pub tree_event_scope_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    File,
    Folder,
}

impl Node {
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
    pub fn is_folder(&self) -> bool {
        self.kind == NodeKind::Folder
    }
}

/// Per-node result of `drive.trash`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrashResult {
    pub uid: String,
    pub ok: bool,
    pub error: Option<String>,
}

/// Params for `drive.upload`. Exactly one of `parent_uid` (new file) or
/// `node_uid` (new revision) must be set.
#[derive(Debug, Clone, Serialize)]
pub struct UploadParams {
    #[serde(rename = "parentUid", skip_serializing_if = "Option::is_none")]
    pub parent_uid: Option<String>,
    #[serde(rename = "nodeUid", skip_serializing_if = "Option::is_none")]
    pub node_uid: Option<String>,
    pub name: String,
    pub path: String,
    pub size: i64,
    pub sha1: String,
    pub mtime: i64,
}

/// Kinds of `drive.event` notifications.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventKind {
    #[serde(rename = "node_created")]
    NodeCreated,
    #[serde(rename = "node_updated")]
    NodeUpdated,
    #[serde(rename = "node_deleted")]
    NodeDeleted,
    #[serde(rename = "tree_refresh")]
    TreeRefresh,
    #[serde(rename = "tree_remove")]
    TreeRemove,
    #[serde(rename = "fast_forward")]
    FastForward,
}

/// Payload of a `drive.event` notification (F6).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriveEvent {
    #[serde(rename = "scopeId")]
    pub scope_id: String,
    #[serde(rename = "eventId")]
    pub event_id: String,
    #[serde(rename = "type")]
    pub kind: EventKind,
    #[serde(rename = "nodeUid")]
    pub node_uid: Option<String>,
    #[serde(rename = "parentNodeUid")]
    pub parent_node_uid: Option<String>,
    #[serde(rename = "isTrashed")]
    pub is_trashed: Option<bool>,
}

/// A server→client notification (no `id`), decoded.
#[derive(Debug, Clone, PartialEq)]
pub enum Notification {
    /// `auth.signInUrl` — display/open this URL to complete fork login.
    AuthSignInUrl { url: String, user_code: String },
    /// `transfer.progress`.
    TransferProgress {
        transfer_id: String,
        bytes: i64,
        total: i64,
    },
    /// `transfer.done` — upload adds node ids.
    TransferDone {
        transfer_id: String,
        node_uid: Option<String>,
        node_revision_uid: Option<String>,
    },
    /// `transfer.error`.
    TransferError {
        transfer_id: String,
        code: i64,
        message: String,
        retryable: bool,
        /// Server-driven rate-limit hint (`retryAfter`, code 1003); `None`
        /// when the server gave no hint.
        retry_after: Option<Duration>,
    },
    /// `drive.event` (F6).
    DriveEvent(Box<DriveEvent>),
    /// Anything else — preserved so callers can log/forward unknown methods.
    Other {
        method: String,
        params: serde_json::Value,
    },
}
