//! Sync engine, state DB, queues, and trash guards.
//!
//! See `SPEC.md` at the repo root. All Proton Drive API traffic goes through
//! the Node.js sidecar (`sidecar/`), never from Rust directly.

/// Sync state machine placeholder — real implementation lands in M1.
pub fn spec_version() -> &'static str {
    "0.1"
}

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_builds() {
        assert_eq!(super::spec_version(), "0.1");
    }
}
