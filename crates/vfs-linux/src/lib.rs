//! `pds-vfs-linux` — FUSE filesystem for Proton Drive placeholders (M3).
//!
//! See `fs.rs` (the filesystem) and `tree.rs` (path ↔ inode mapping). The
//! engine side lives in `pds_core::store` and the placeholder branches of
//! `pds_core::engine`.

pub mod fs;
pub mod tree;

pub use fs::PdsFs;
