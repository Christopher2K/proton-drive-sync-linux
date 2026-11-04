//! F7 keep-both conflict naming: `name (conflict YYYY-MM-DD HHmm).ext`.
//!
//! The suffix is UTC. Local wall-clock time repeats during a DST fold and can
//! jump backwards when the system clock is corrected; using UTC keeps the
//! generated names monotonic-enough and stable for a given instant. Existing
//! local-time conflict copies are ordinary files: dedup only checks whether a
//! candidate name is taken, so both naming eras coexist without parsing.

use chrono::{DateTime, Utc};

/// Build the keep-both name for `name` at time `at` (SPEC.md F7).
///
/// The extension (everything after the last dot) is preserved; dotfiles and
/// extension-less names get the suffix appended whole.
pub fn conflict_name(name: &str, at: DateTime<Utc>) -> String {
    let stamp = at.format("%Y-%m-%d %H%M");
    match name.rsplit_once('.') {
        // Guard: dotfile like `.env` (empty stem) has no "extension".
        Some((stem, ext)) if !stem.is_empty() => format!("{stem} (conflict {stamp}).{ext}"),
        _ => format!("{name} (conflict {stamp})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 23, 14, 5, 0).unwrap()
    }

    #[test]
    fn plain_file() {
        assert_eq!(
            conflict_name("report.pdf", at()),
            "report (conflict 2026-10-23 1405).pdf"
        );
    }

    #[test]
    fn no_extension() {
        assert_eq!(
            conflict_name("README", at()),
            "README (conflict 2026-10-23 1405)"
        );
    }

    #[test]
    fn dotfile_has_no_extension() {
        assert_eq!(
            conflict_name(".env", at()),
            ".env (conflict 2026-10-23 1405)"
        );
    }

    #[test]
    fn multi_dot_keeps_last_extension() {
        assert_eq!(
            conflict_name("archive.tar.gz", at()),
            "archive.tar (conflict 2026-10-23 1405).gz"
        );
    }

    #[test]
    fn zero_padded_minutes() {
        let t = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 0).unwrap();
        assert_eq!(
            conflict_name("a.txt", t),
            "a (conflict 2026-01-02 0304).txt"
        );
    }

    /// A DST fold repeats the local wall clock, but the UTC instants — and
    /// therefore the generated names — are distinct. Comparing with the
    /// local representations proves local-time naming would have collided.
    #[test]
    fn dst_fold_instants_get_distinct_utc_names() {
        // US Eastern fall-back 2026-11-01: 01:30 EDT then 01:30 EST.
        let first = Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap();
        let second = Utc.with_ymd_and_hms(2026, 11, 1, 6, 30, 0).unwrap();
        let edt = FixedOffset::west_opt(4 * 3600).unwrap();
        let est = FixedOffset::west_opt(5 * 3600).unwrap();
        assert_eq!(
            first.with_timezone(&edt).format("%Y-%m-%d %H%M").to_string(),
            second
                .with_timezone(&est)
                .format("%Y-%m-%d %H%M")
                .to_string(),
            "the fold really does repeat the local wall clock"
        );
        assert_eq!(
            conflict_name("a.txt", first),
            "a (conflict 2026-11-01 0530).txt"
        );
        assert_eq!(
            conflict_name("a.txt", second),
            "a (conflict 2026-11-01 0630).txt"
        );
        assert_ne!(conflict_name("a.txt", first), conflict_name("a.txt", second));
    }
}
