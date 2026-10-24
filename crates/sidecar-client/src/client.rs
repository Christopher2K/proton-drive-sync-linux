//! Typed sidecar API built on the JSON-RPC transport, plus child-process
//! spawning. Mirrors `docs/protocol.md` method-for-method.

use std::path::Path;
use std::process::Stdio;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::jsonrpc::{Error, JsonRpcClient};
use crate::types::{Node, Notification, TrashResult, UploadParams};

/// The full sidecar API as an object-safe trait so the sync engine can run
/// against either the real child process or the in-process `MockSidecar`.
#[async_trait]
pub trait SidecarApi: Send + Sync {
    // Auth
    async fn login_fork(&self) -> Result<String, Error>;
    async fn auth_status(&self) -> Result<AuthStatus, Error>;
    async fn logout(&self) -> Result<(), Error>;

    // Drive
    async fn get_root(&self) -> Result<Node, Error>;
    async fn list_children(&self, parent_uid: &str) -> Result<Vec<Node>, Error>;
    async fn get_node(&self, uid: &str) -> Result<Node, Error>;
    async fn create_folder(&self, parent_uid: &str, name: &str) -> Result<Node, Error>;
    async fn get_available_name(&self, parent_uid: &str, name: &str) -> Result<String, Error>;
    async fn trash(&self, uids: &[String]) -> Result<Vec<TrashResult>, Error>;

    // Transfers (async — completion arrives as notifications)
    async fn upload(&self, params: &UploadParams) -> Result<String, Error>;
    async fn download(&self, node_uid: &str, path: &Path) -> Result<String, Error>;
    async fn transfer_pause(&self, transfer_id: &str) -> Result<(), Error>;
    async fn transfer_resume(&self, transfer_id: &str) -> Result<(), Error>;
    async fn transfer_cancel(&self, transfer_id: &str) -> Result<(), Error>;

    // Events
    async fn events_start(&self, scope_id: &str, last_event_id: Option<&str>) -> Result<(), Error>;
    async fn events_stop(&self, scope_id: &str) -> Result<(), Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthStatus {
    pub logged_in: bool,
    pub email: Option<String>,
}

/// A client connected to a sidecar (real child process or in-process mock).
pub struct SidecarClient {
    rpc: JsonRpcClient,
    notifications: Option<mpsc::Receiver<Notification>>,
}

impl SidecarClient {
    pub fn from_rpc(rpc: JsonRpcClient, notifications: mpsc::Receiver<Notification>) -> Self {
        Self {
            rpc,
            notifications: Some(notifications),
        }
    }

    /// Take the notification stream (once).
    pub fn take_notifications(&mut self) -> mpsc::Receiver<Notification> {
        self.notifications
            .take()
            .expect("notifications already taken")
    }

    /// Spawn the Node.js sidecar as a child process and connect over stdio.
    /// `program` is e.g. `node` or `npx`; `args` e.g. `["dist/index.js"]` or
    /// `["tsx", "src/index.ts"]`. Stderr is inherited (human logs only).
    pub fn spawn(program: &str, args: &[&str], cwd: Option<&Path>) -> Result<(Self, Child), Error> {
        let mut cmd = Command::new(program);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            // Never let a stray stdin EOF from a terminal kill the protocol.
            .kill_on_drop(true);
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let (rpc, notifications) = JsonRpcClient::new(stdout, stdin);
        Ok((Self::from_rpc(rpc, notifications), child))
    }

    fn get<'a>(v: &'a Value, key: &str) -> Result<&'a Value, Error> {
        v.get(key)
            .ok_or_else(|| Error::Malformed(format!("missing field `{key}` in response")))
    }
}

#[async_trait]
impl SidecarApi for SidecarClient {
    async fn login_fork(&self) -> Result<String, Error> {
        let result = self.rpc.call("auth.loginFork", json!({})).await?;
        let email = Self::get(&result, "email")?
            .as_str()
            .ok_or_else(|| Error::Malformed("`email` is not a string".into()))?;
        Ok(email.to_string())
    }

    async fn auth_status(&self) -> Result<AuthStatus, Error> {
        let result = self.rpc.call("auth.status", json!({})).await?;
        Ok(AuthStatus {
            logged_in: Self::get(&result, "loggedIn")?
                .as_bool()
                .ok_or_else(|| Error::Malformed("`loggedIn` is not a bool".into()))?,
            email: result
                .get("email")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    async fn logout(&self) -> Result<(), Error> {
        self.rpc.call("auth.logout", json!({})).await?;
        Ok(())
    }

    async fn get_root(&self) -> Result<Node, Error> {
        let result = self.rpc.call("drive.getRoot", json!({})).await?;
        serde_json::from_value(result).map_err(|e| Error::Malformed(e.to_string()))
    }

    async fn list_children(&self, parent_uid: &str) -> Result<Vec<Node>, Error> {
        let result = self
            .rpc
            .call("drive.listChildren", json!({ "parentUid": parent_uid }))
            .await?;
        serde_json::from_value(Self::get(&result, "nodes")?.clone())
            .map_err(|e| Error::Malformed(e.to_string()))
    }

    async fn get_node(&self, uid: &str) -> Result<Node, Error> {
        let result = self
            .rpc
            .call("drive.getNode", json!({ "uid": uid }))
            .await?;
        serde_json::from_value(result).map_err(|e| Error::Malformed(e.to_string()))
    }

    async fn create_folder(&self, parent_uid: &str, name: &str) -> Result<Node, Error> {
        let result = self
            .rpc
            .call(
                "drive.createFolder",
                json!({ "parentUid": parent_uid, "name": name }),
            )
            .await?;
        serde_json::from_value(result).map_err(|e| Error::Malformed(e.to_string()))
    }

    async fn get_available_name(&self, parent_uid: &str, name: &str) -> Result<String, Error> {
        let result = self
            .rpc
            .call(
                "drive.getAvailableName",
                json!({ "parentUid": parent_uid, "name": name }),
            )
            .await?;
        Ok(Self::get(&result, "name")?
            .as_str()
            .ok_or_else(|| Error::Malformed("`name` is not a string".into()))?
            .to_string())
    }

    async fn trash(&self, uids: &[String]) -> Result<Vec<TrashResult>, Error> {
        let result = self
            .rpc
            .call("drive.trash", json!({ "uids": uids }))
            .await?;
        serde_json::from_value(Self::get(&result, "results")?.clone())
            .map_err(|e| Error::Malformed(e.to_string()))
    }

    async fn upload(&self, params: &UploadParams) -> Result<String, Error> {
        let result = self
            .rpc
            .call(
                "drive.upload",
                serde_json::to_value(params).expect("UploadParams serializes"),
            )
            .await?;
        Ok(Self::get(&result, "transferId")?
            .as_str()
            .ok_or_else(|| Error::Malformed("`transferId` is not a string".into()))?
            .to_string())
    }

    async fn download(&self, node_uid: &str, path: &Path) -> Result<String, Error> {
        let result = self
            .rpc
            .call(
                "drive.download",
                json!({ "nodeUid": node_uid, "path": path.to_string_lossy() }),
            )
            .await?;
        Ok(Self::get(&result, "transferId")?
            .as_str()
            .ok_or_else(|| Error::Malformed("`transferId` is not a string".into()))?
            .to_string())
    }

    async fn transfer_pause(&self, transfer_id: &str) -> Result<(), Error> {
        self.rpc
            .call("transfer.pause", json!({ "transferId": transfer_id }))
            .await?;
        Ok(())
    }

    async fn transfer_resume(&self, transfer_id: &str) -> Result<(), Error> {
        self.rpc
            .call("transfer.resume", json!({ "transferId": transfer_id }))
            .await?;
        Ok(())
    }

    async fn transfer_cancel(&self, transfer_id: &str) -> Result<(), Error> {
        self.rpc
            .call("transfer.cancel", json!({ "transferId": transfer_id }))
            .await?;
        Ok(())
    }

    async fn events_start(&self, scope_id: &str, last_event_id: Option<&str>) -> Result<(), Error> {
        let mut params = json!({ "scopeId": scope_id });
        if let Some(id) = last_event_id {
            params["lastEventId"] = json!(id);
        }
        self.rpc.call("drive.events.start", params).await?;
        Ok(())
    }

    async fn events_stop(&self, scope_id: &str) -> Result<(), Error> {
        self.rpc
            .call("drive.events.stop", json!({ "scopeId": scope_id }))
            .await?;
        Ok(())
    }
}

// Re-export for convenience in matching code.
pub use crate::types::DriveEvent as Event;
