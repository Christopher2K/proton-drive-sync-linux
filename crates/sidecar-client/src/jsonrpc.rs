//! Low-level JSON-RPC 2.0 client over NDJSON framing (one message per line).
//!
//! Generic over any async reader/writer pair, so it works over child-process
//! stdio as well as in-process duplex streams (tests, `MockSidecar`).

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, trace, warn};

use crate::types::Notification;

/// A protocol-level error object returned by the server.
#[derive(Debug, Clone, thiserror::Error)]
#[error("rpc error {code}: {message}")]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    /// `data.retryAfter` seconds for code 1003 (rate_limited), if present.
    pub fn retry_after(&self) -> Option<f64> {
        self.data
            .as_ref()
            .and_then(|d| d.get("retryAfter"))
            .and_then(Value::as_f64)
    }
}

/// Client-side errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("connection closed")]
    Closed,
    #[error("malformed message from server: {0}")]
    Malformed(String),
}

impl Error {
    /// The JSON-RPC error code if this was a protocol error.
    pub fn code(&self) -> Option<i64> {
        match self {
            Error::Rpc(e) => Some(e.code),
            _ => None,
        }
    }
}

struct Shared {
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>,
    next_id: AtomicU64,
}

/// An async JSON-RPC 2.0 client. Cheap to clone; calls may be made
/// concurrently and are correlated by `id`.
#[derive(Clone)]
pub struct JsonRpcClient {
    shared: Arc<Shared>,
    write_tx: mpsc::Sender<String>,
}

impl JsonRpcClient {
    /// Create a client over an arbitrary byte stream. Returns the client and
    /// a receiver for server notifications. Background tasks exit when the
    /// stream closes or the client is dropped.
    pub fn new<R, W>(reader: R, writer: W) -> (Self, mpsc::Receiver<Notification>)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        });
        let (write_tx, write_rx) = mpsc::channel::<String>(256);
        let (notif_tx, notif_rx) = mpsc::channel::<Notification>(256);

        tokio::spawn(write_loop(writer, write_rx));
        tokio::spawn(read_loop(reader, Arc::clone(&shared), notif_tx));

        (Self { shared, write_tx }, notif_rx)
    }

    /// Send a request and await the correlated response.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, Error> {
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.shared.pending.lock().await.insert(id, tx);

        let line = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        })
        .to_string();
        trace!(id, method, "rpc -> {line}");

        self.write_tx.send(line).await.map_err(|_| Error::Closed)?;

        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(Error::Rpc(err)),
            Err(_) => Err(Error::Closed),
        }
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(mut writer: W, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        let mut buf = line.into_bytes();
        buf.push(b'\n');
        if writer.write_all(&buf).await.is_err() || writer.flush().await.is_err() {
            debug!("rpc write loop: peer closed");
            return;
        }
    }
}

async fn read_loop<R: AsyncRead + Unpin>(
    reader: R,
    shared: Arc<Shared>,
    notif_tx: mpsc::Sender<Notification>,
) {
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                trace!("rpc <- {line}");
                match serde_json::from_str::<Value>(&line) {
                    Ok(msg) => dispatch(msg, &shared, &notif_tx).await,
                    Err(e) => warn!("rpc: skipping malformed line: {e}"),
                }
            }
            Ok(None) => {
                debug!("rpc read loop: EOF");
                break;
            }
            Err(e) => {
                debug!("rpc read loop: read error: {e}");
                break;
            }
        }
    }
    // Fail every pending request so callers don't hang.
    let mut pending = shared.pending.lock().await;
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(RpcError {
            code: -32000,
            message: "connection closed".into(),
            data: None,
        }));
    }
}

async fn dispatch(msg: Value, shared: &Arc<Shared>, notif_tx: &mpsc::Sender<Notification>) {
    if let Some(id) = msg.get("id").and_then(Value::as_u64) {
        let tx = shared.pending.lock().await.remove(&id);
        let Some(tx) = tx else {
            warn!(id, "rpc: response for unknown id");
            return;
        };
        if let Some(err) = msg.get("error") {
            let rpc_err = RpcError {
                code: err.get("code").and_then(Value::as_i64).unwrap_or(-32000),
                message: err
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
                    .to_string(),
                data: err.get("data").cloned(),
            };
            let _ = tx.send(Err(rpc_err));
        } else {
            let _ = tx.send(Ok(msg.get("result").cloned().unwrap_or(Value::Null)));
        }
    } else if let Some(method) = msg.get("method").and_then(Value::as_str) {
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        let notif = parse_notification(method, params);
        // Receiver gone: client is shutting down; drop silently.
        let _ = notif_tx.try_send(notif);
    } else {
        warn!("rpc: message with neither id nor method, ignoring");
    }
}

fn parse_notification(method: &str, params: Value) -> Notification {
    let s = |k: &str| {
        params
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let i = |k: &str| params.get(k).and_then(Value::as_i64).unwrap_or_default();
    match method {
        "auth.signInUrl" => Notification::AuthSignInUrl {
            url: s("url"),
            user_code: s("userCode"),
        },
        "transfer.progress" => Notification::TransferProgress {
            transfer_id: s("transferId"),
            bytes: i("bytes"),
            total: i("total"),
        },
        "transfer.done" => Notification::TransferDone {
            transfer_id: s("transferId"),
            node_uid: params
                .get("nodeUid")
                .and_then(Value::as_str)
                .map(str::to_string),
            node_revision_uid: params
                .get("nodeRevisionUid")
                .and_then(Value::as_str)
                .map(str::to_string),
        },
        "transfer.error" => Notification::TransferError {
            transfer_id: s("transferId"),
            code: i("code"),
            message: s("message"),
            retryable: params
                .get("retryable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        },
        "drive.event" => match serde_json::from_value::<crate::types::DriveEvent>(params.clone()) {
            Ok(ev) => Notification::DriveEvent(Box::new(ev)),
            Err(_) => Notification::Other {
                method: method.to_string(),
                params,
            },
        },
        other => Notification::Other {
            method: other.to_string(),
            params,
        },
    }
}
