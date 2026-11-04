//! Mountpoint inspection for the placeholder view (TG-30).
//!
//! A FUSE mount outlives the process that served it. After a crash or a
//! `SIGKILL`, the mountpoint is still attached in the kernel but its session
//! is gone: every access returns `ENOTCONN` (or blocks forever). The app must
//! detect that BEFORE it starts a sync session, otherwise it would silently
//! write into a dead mount and hang the UI.
//!
//! [`mountpoint_state`] combines `/proc/self/mountinfo` with a bounded
//! directory probe. A mount that answers is [`MountState::Live`] (another
//! session owns it — mounting a second session would be wrong); one that
//! times out or reports a stale-session errno is [`MountState::Stale`].
//! [`ensure_placeholder_mountpoint`] turns those states into the actionable
//! error the app shows instead of proceeding.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

/// What is attached at a path the app wants to use as its placeholder view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountState {
    /// Not a mountpoint (or the path does not exist yet): safe to mount.
    Free,
    /// A mountpoint that answered a directory probe: another live session.
    Live {
        mountpoint: PathBuf,
        fstype: String,
    },
    /// A mountpoint whose session is gone: accessing it blocks or fails.
    Stale {
        mountpoint: PathBuf,
        fstype: String,
        reason: String,
    },
    /// A mountpoint that exists but the probe failed for another reason.
    Unreachable {
        mountpoint: PathBuf,
        fstype: String,
        reason: String,
    },
}

impl MountState {
    /// True when anything is attached at the path (including a stale mount).
    pub fn is_mounted(&self) -> bool {
        !matches!(self, Self::Free)
    }
}

/// A refusal to start the placeholder session at a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountpointError {
    /// Another live session already owns the mountpoint.
    Busy {
        mountpoint: PathBuf,
        fstype: String,
    },
    /// The previous session died without unmounting; the mount is dead.
    Stale {
        mountpoint: PathBuf,
        fstype: String,
        reason: String,
    },
    /// The mountpoint could not be inspected (permission, transient error).
    Unreachable {
        mountpoint: PathBuf,
        fstype: String,
        reason: String,
    },
}

impl fmt::Display for MountpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy { mountpoint, fstype } => write!(
                f,
                "the sync root {} is already mounted as {fstype}; another sync session may be \
                 running. Close it, or unmount with `fusermount3 -uz {}` and retry.",
                mountpoint.display(),
                mountpoint.display()
            ),
            Self::Stale {
                mountpoint,
                fstype,
                reason,
            } => write!(
                f,
                "the sync root {} is covered by a stale {fstype} mount left by an earlier session \
                 that stopped without unmounting ({reason}). Refusing to sync into a dead mount. \
                 Unmount it with `fusermount3 -uz {}` (or reboot) and retry.",
                mountpoint.display(),
                mountpoint.display()
            ),
            Self::Unreachable {
                mountpoint,
                fstype,
                reason,
            } => write!(
                f,
                "the sync root {} is a {fstype} mountpoint that cannot be inspected ({reason}). \
                 Refusing to sync into it. Unmount it with `fusermount3 -uz {}` and retry.",
                mountpoint.display(),
                mountpoint.display()
            ),
        }
    }
}

impl std::error::Error for MountpointError {}

/// Inspect `root` and refuse to start a placeholder session if anything is
/// already attached there. This is the app's preflight: it must return
/// quickly even when the existing mount is wedged.
pub fn ensure_placeholder_mountpoint(
    root: &Path,
    timeout: Duration,
) -> Result<(), MountpointError> {
    refusal(mountpoint_state(root, timeout))
}

fn refusal(state: MountState) -> Result<(), MountpointError> {
    match state {
        MountState::Free => Ok(()),
        MountState::Live { mountpoint, fstype } => Err(MountpointError::Busy { mountpoint, fstype }),
        MountState::Stale {
            mountpoint,
            fstype,
            reason,
        } => Err(MountpointError::Stale {
            mountpoint,
            fstype,
            reason,
        }),
        MountState::Unreachable {
            mountpoint,
            fstype,
            reason,
        } => Err(MountpointError::Unreachable {
            mountpoint,
            fstype,
            reason,
        }),
    }
}

/// What is attached at `root`, if anything.
///
/// The probe runs on a helper thread so a wedged FUSE mount cannot block the
/// caller past `timeout` (the same pattern as the app's FUSE availability
/// probe). A timed-out helper thread is left behind; the process exits long
/// before it matters.
pub fn mountpoint_state(root: &Path, timeout: Duration) -> MountState {
    let target = normalize(root);
    let Some((mountpoint, fstype)) = find_mount(&target) else {
        return MountState::Free;
    };
    state_from_probe(mountpoint, fstype, probe(&target, timeout), timeout)
}

enum Probe {
    Served,
    TimedOut,
    Failed(io::Error),
}

fn probe(path: &Path, timeout: Duration) -> Probe {
    let (tx, rx) = mpsc::channel();
    let target = path.to_path_buf();
    std::thread::spawn(move || {
        // `read_dir` forces a real FUSE readdir; a dead session answers
        // ENOTCONN or blocks. A plain metadata call can be satisfied from a
        // cached dentry on some kernels.
        let result = std::fs::read_dir(&target).map(|mut it| {
            let _ = it.next();
        });
        let _ = tx.send(result);
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(())) => Probe::Served,
        Ok(Err(e)) => Probe::Failed(e),
        Err(_) => Probe::TimedOut,
    }
}

fn state_from_probe(
    mountpoint: PathBuf,
    fstype: String,
    probe: Probe,
    timeout: Duration,
) -> MountState {
    match probe {
        Probe::Served => MountState::Live {
            mountpoint,
            fstype,
        },
        Probe::TimedOut => MountState::Stale {
            mountpoint,
            fstype,
            reason: format!("no response within {timeout:?}"),
        },
        Probe::Failed(e) if is_stale_error(&e) => MountState::Stale {
            mountpoint,
            fstype,
            reason: e.to_string(),
        },
        Probe::Failed(e) => MountState::Unreachable {
            mountpoint,
            fstype,
            reason: e.to_string(),
        },
    }
}

/// Errnos a kernel returns for a FUSE mount whose serving process is gone.
fn is_stale_error(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code)
            if code == libc::ENOTCONN
                || code == libc::ENODEV
                || code == libc::EIO
                || code == libc::ESTALE
                || code == libc::ENOENT
    )
}

/// Every `(mountpoint, fstype)` in `/proc/self/mountinfo`, in file order.
/// Only FUSE mounts matter here: a sync root that happens to sit on its own
/// disk mount (e.g. `/mnt/data`) is perfectly mountable over, and refusing it
/// would make the app unusable there.
fn find_mount(target: &Path) -> Option<(PathBuf, String)> {
    let text = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    text.lines()
        .filter_map(parse_mountinfo_line)
        .find(|(mountpoint, fstype)| mountpoint == target && is_fuse(fstype))
}

fn is_fuse(fstype: &str) -> bool {
    fstype == "fuse" || fstype.starts_with("fuse.")
}

/// Parse one mountinfo line: the mountpoint is field 5, the filesystem type
/// follows the `-` separator. Paths are octal-escaped (`\040` = space).
fn parse_mountinfo_line(line: &str) -> Option<(PathBuf, String)> {
    let mut fields = line.split(' ');
    // 1 mount id, 2 parent id, 3 major:minor, 4 root, 5 mount point, 6 opts.
    let _id = fields.next()?;
    let _parent = fields.next()?;
    let _dev = fields.next()?;
    let _root = fields.next()?;
    let mountpoint = fields.next()?;
    let _options = fields.next()?;
    let rest: Vec<&str> = fields.collect();
    let separator = rest.iter().position(|f| *f == "-")?;
    let fstype = *rest.get(separator + 1)?;
    Some((PathBuf::from(unescape_mountinfo(mountpoint)), fstype.to_string()))
}

fn unescape_mountinfo(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let octal = &raw[i + 1..i + 4];
            if let Ok(value) = u8::from_str_radix(octal, 8) {
                out.push(value as char);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Absolute, symlink-resolved where possible. On a dead mount `canonicalize`
/// itself can fail, so fall back to resolving the parent and keeping the
/// final component.
fn normalize(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => parent
            .canonicalize()
            .map(|canonical| canonical.join(name))
            .unwrap_or(absolute),
        _ => absolute,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_failure(code: i32) -> Probe {
        Probe::Failed(io::Error::from_raw_os_error(code))
    }

    #[test]
    fn parses_mountinfo_escapes_and_fstype() {
        let line = "36 1 0:32 / /tmp/foo\\040bar rw,nosuid,nodev - fuse.pds pds rw,user_id=1000";
        let (mountpoint, fstype) = parse_mountinfo_line(line).expect("valid mountinfo line");
        assert_eq!(mountpoint, PathBuf::from("/tmp/foo bar"));
        assert_eq!(fstype, "fuse.pds");
    }

    #[test]
    fn only_fuse_mounts_are_treated_as_occupied() {
        assert!(is_fuse("fuse"));
        assert!(is_fuse("fuse.pds"));
        assert!(is_fuse("fuse.sshfs"));
        assert!(!is_fuse("ext4"));
        assert!(!is_fuse("tmpfs"));
    }

    #[test]
    fn ordinary_directory_is_free() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            mountpoint_state(dir.path(), Duration::from_millis(200)),
            MountState::Free
        );
        assert!(
            ensure_placeholder_mountpoint(dir.path(), Duration::from_millis(200)).is_ok()
        );
    }

    #[test]
    fn served_probe_is_live_and_probe_errors_are_classified() {
        let live = state_from_probe(
            PathBuf::from("/mnt/pds"),
            "fuse.pds".into(),
            Probe::Served,
            Duration::from_secs(1),
        );
        assert!(matches!(live, MountState::Live { .. }));
        let err = refusal(live);
        assert!(matches!(err, Err(MountpointError::Busy { .. })));
        assert!(err.unwrap_err().to_string().contains("already mounted"));

        for code in [libc::ENOTCONN, libc::ENODEV, libc::EIO, libc::ESTALE, libc::ENOENT] {
            let state = state_from_probe(
                PathBuf::from("/mnt/pds"),
                "fuse.pds".into(),
                probe_failure(code),
                Duration::from_secs(1),
            );
            assert!(
                matches!(state, MountState::Stale { .. }),
                "errno {code} should classify as stale: {state:?}"
            );
        }

        let timed_out = state_from_probe(
            PathBuf::from("/mnt/pds"),
            "fuse.pds".into(),
            Probe::TimedOut,
            Duration::from_millis(50),
        );
        assert!(matches!(timed_out, MountState::Stale { .. }));
        assert!(refusal(timed_out).unwrap_err().to_string().contains("stale"));

        let denied = state_from_probe(
            PathBuf::from("/mnt/pds"),
            "fuse.pds".into(),
            probe_failure(libc::EACCES),
            Duration::from_secs(1),
        );
        assert!(matches!(denied, MountState::Unreachable { .. }));
    }
}
