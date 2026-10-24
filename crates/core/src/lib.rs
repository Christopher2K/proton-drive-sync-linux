//! Sync engine, state DB, queues, and trash guards.
//!
//! See `SPEC.md` at the repo root. All Proton Drive API traffic goes through
//! the Node.js sidecar (`sidecar/`), never from Rust directly.

pub mod conflict;
pub mod db;
pub mod engine;
pub mod guard;

pub use db::{Db, NodeRow, SyncState, Tombstone};
pub use engine::{
    Command, Engine, EngineConfig, EngineHandle, EngineState, EngineStatus, RETRY_BASE_DEFAULT,
    STATE_DIR, sha1_file, to_abs, to_rel,
};
pub use guard::{DeleteGuard, GUARD_MAX_DELETES, GUARD_MAX_RATIO, GUARD_WINDOW};
