//! Sync engine, state DB, queues, and trash guards.
//!
//! See `SPEC.md` at the repo root. All Proton Drive API traffic goes through
//! the Node.js sidecar (`sidecar/`), never from Rust directly.

pub mod conflict;
pub mod db;
pub mod engine;
pub mod guard;
pub mod mode;
pub mod store;

pub use db::{Db, NodeRow, SyncState, Tombstone};
pub use engine::{
    ActivityItem, ActivityKind, Command, Engine, EngineConfig, EngineHandle, EngineState,
    EngineStatus, EVICTION_CAP_DEFAULT, RETRY_BASE_DEFAULT, STATE_DIR, VFS_BACKING_DIR, sha1_file,
    to_abs, to_rel,
};
pub use mode::{
    ENV_SYNC_MODE, META_SYNC_MODE, SyncMode, app_data_dir, apply_mode_setting, engine_state_dir,
    move_file, root_key, set_sync_mode, stored_root_of,
};
pub use guard::{DeleteGuard, GUARD_MAX_DELETES, GUARD_MAX_RATIO, GUARD_WINDOW};
pub use store::Store;
