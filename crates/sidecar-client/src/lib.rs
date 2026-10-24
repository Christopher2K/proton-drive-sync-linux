//! Rust client for the proton-drive-sync Node.js sidecar.
//!
//! JSON-RPC 2.0 over NDJSON (newline-delimited, one message per line) on the
//! sidecar's stdio, per `docs/protocol.md`. All Proton Drive traffic goes
//! through the sidecar — this crate never talks to Proton directly.

pub mod client;
pub mod jsonrpc;
pub mod mock;
pub mod spawn;
pub mod types;

pub use client::{AuthStatus, SidecarApi, SidecarClient};
pub use jsonrpc::{Error, JsonRpcClient, RpcError};
pub use mock::{MockHandle, MockSidecar};
pub use types::{DriveEvent, EventKind, Node, NodeKind, Notification, TrashResult, UploadParams};
