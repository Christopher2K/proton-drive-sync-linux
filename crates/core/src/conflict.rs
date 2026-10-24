//! F7 keep-both conflict naming: `name (conflict YYYY-MM-DD HHmm).ext`.

use chrono::{DateTime, Local};

/// Build the keep-both name for `name` at time `at` (SPEC.md F7).
///
/// The extension (everything after the last dot) is preserved; dotfiles and
/// extension-less names get the suffix appended whole.
pub fn conflict_name(name: &str, at: DateTime<Local>) -> String {
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
    use chrono::TimeZone;

    fn at() -> DateTime<Local> {
        Local.with_ymd_and_hms(2026, 10, 23, 14, 5, 0).unwrap()
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
        let t = Local.with_ymd_and_hms(2026, 1, 2, 3, 4, 0).unwrap();
        assert_eq!(
            conflict_name("a.txt", t),
            "a (conflict 2026-01-02 0304).txt"
        );
    }
}
