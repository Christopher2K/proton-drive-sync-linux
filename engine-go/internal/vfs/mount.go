//go:build linux

// Mountpoint inspection for the placeholder view (TG-30).
//
// A FUSE mount outlives the process that served it. After a crash or a
// SIGKILL, the mountpoint is still attached in the kernel but its session
// is gone: every access returns ENOTCONN (or blocks forever). The app must
// detect that BEFORE it starts a sync session, otherwise it would silently
// write into a dead mount and hang the UI.
//
// MountpointState combines /proc/self/mountinfo with a bounded directory
// probe. A mount that answers is Live (another session owns it — mounting
// a second session would be wrong); one that times out or reports a
// stale-session errno is Stale. EnsurePlaceholderMountpoint turns those
// states into the actionable error the app shows instead of proceeding.

package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// MountState is what is attached at a path the app wants to use as its
// placeholder view.
type MountState struct {
	// Kind: free, live, stale, or unreachable.
	Kind MountKind
	// Mountpoint and FSType describe the inspected FUSE mount (empty for
	// Free).
	Mountpoint string
	FSType     string
	// Reason carries the probe failure detail for Stale/Unreachable.
	Reason string
}

type MountKind int

const (
	// MountFree: not a mountpoint (or the path does not exist yet): safe
	// to mount.
	MountFree MountKind = iota
	// MountLive: a mountpoint that answered a directory probe: another
	// live session.
	MountLive
	// MountStale: a mountpoint whose session is gone: accessing it
	// blocks or fails.
	MountStale
	// MountUnreachable: a mountpoint that exists but the probe failed
	// for another reason.
	MountUnreachable
)

// IsMounted reports whether anything is attached at the path (including a
// stale mount).
func (s MountState) IsMounted() bool { return s.Kind != MountFree }

// MountpointError is a refusal to start the placeholder session at a path.
type MountpointError struct {
	Kind       MountKind // live | stale | unreachable
	Mountpoint string
	FSType     string
	Reason     string // stale | unreachable
}

func (e *MountpointError) Error() string {
	switch e.Kind {
	case MountLive:
		return fmt.Sprintf(
			"the sync root %s is already mounted as %s; another sync session may be running. "+
				"Close it, or unmount with `fusermount3 -uz %s` and retry.",
			e.Mountpoint, e.FSType, e.Mountpoint)
	case MountStale:
		return fmt.Sprintf(
			"the sync root %s is covered by a stale %s mount left by an earlier session "+
				"that stopped without unmounting (%s). Refusing to sync into a dead mount. "+
				"Unmount it with `fusermount3 -uz %s` (or reboot) and retry.",
			e.Mountpoint, e.FSType, e.Reason, e.Mountpoint)
	default:
		return fmt.Sprintf(
			"the sync root %s is a %s mountpoint that cannot be inspected (%s). "+
				"Refusing to sync into it. Unmount it with `fusermount3 -uz %s` and retry.",
			e.Mountpoint, e.FSType, e.Reason, e.Mountpoint)
	}
}

// EnsurePlaceholderMountpoint inspects root and refuses to start a
// placeholder session if anything is already attached there. This is the
// app's preflight: it must return quickly even when the existing mount is
// wedged.
func EnsurePlaceholderMountpoint(root string, timeout time.Duration) error {
	return refusal(MountpointState(root, timeout))
}

func refusal(state MountState) error {
	switch state.Kind {
	case MountFree:
		return nil
	case MountLive:
		return &MountpointError{Kind: MountLive, Mountpoint: state.Mountpoint, FSType: state.FSType}
	case MountStale:
		return &MountpointError{Kind: MountStale, Mountpoint: state.Mountpoint, FSType: state.FSType, Reason: state.Reason}
	default:
		return &MountpointError{Kind: MountUnreachable, Mountpoint: state.Mountpoint, FSType: state.FSType, Reason: state.Reason}
	}
}

// MountpointState reports what is attached at root, if anything.
//
// The probe runs on a helper goroutine so a wedged FUSE mount cannot block
// the caller past timeout. A timed-out helper goroutine is left behind;
// the process exits long before it matters.
func MountpointState(root string, timeout time.Duration) MountState {
	target := normalizePath(root)
	mountpoint, fstype, mounted := findMount(target, mountinfoPath)
	if !mounted {
		return MountState{Kind: MountFree}
	}
	return stateFromProbe(mountpoint, fstype, probe(target, timeout))
}

// mountinfoPath is overridable in tests.
var mountinfoPath = "/proc/self/mountinfo"

type probeKind int

const (
	probeServed probeKind = iota
	probeTimedOut
	probeFailed
)

type probeResult struct {
	kind  probeKind
	errno syscall.Errno
}

func probe(path string, timeout time.Duration) probeResult {
	type outcome struct {
		errno syscall.Errno
		ok    bool
	}
	done := make(chan outcome, 1)
	go func() {
		// ReadDir forces a real FUSE readdir; a dead session answers
		// ENOTCONN or blocks. A plain stat call can be satisfied from a
		// cached dentry on some kernels.
		_, err := os.ReadDir(path)
		if err != nil {
			done <- outcome{errno: errnoOf(err)}
			return
		}
		done <- outcome{ok: true}
	}()
	select {
	case o := <-done:
		if o.ok {
			return probeResult{kind: probeServed}
		}
		return probeResult{kind: probeFailed, errno: o.errno}
	case <-time.After(timeout):
		return probeResult{kind: probeTimedOut}
	}
}

func errnoOf(err error) syscall.Errno {
	errno, ok := err.(syscall.Errno)
	if !ok {
		// PathError and friends wrap the errno; unwrap once.
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			return errnoOf(u.Unwrap())
		}
		return syscall.EIO
	}
	return errno
}

func stateFromProbe(mountpoint, fstype string, p probeResult) MountState {
	switch p.kind {
	case probeServed:
		return MountState{Kind: MountLive, Mountpoint: mountpoint, FSType: fstype}
	case probeTimedOut:
		return MountState{Kind: MountStale, Mountpoint: mountpoint, FSType: fstype, Reason: "no response within the probe timeout"}
	default:
		if isStaleErrno(p.errno) {
			return MountState{Kind: MountStale, Mountpoint: mountpoint, FSType: fstype, Reason: p.errno.Error()}
		}
		return MountState{Kind: MountUnreachable, Mountpoint: mountpoint, FSType: fstype, Reason: p.errno.Error()}
	}
}

// isStaleErrno: errnos a kernel returns for a FUSE mount whose serving
// process is gone.
func isStaleErrno(errno syscall.Errno) bool {
	switch errno {
	case syscall.ENOTCONN, syscall.ENODEV, syscall.EIO, syscall.ESTALE, syscall.ENOENT:
		return true
	}
	return false
}

// findMount returns the (mountpoint, fstype) of the FUSE mount at target
// from the given mountinfo text source, if any. Only FUSE mounts matter
// here: a sync root that happens to sit on its own disk mount (e.g.
// /mnt/data) is perfectly mountable over, and refusing it would make the
// app unusable there.
func findMount(target, mountinfo string) (string, string, bool) {
	data, err := os.ReadFile(mountinfo)
	if err != nil {
		return "", "", false
	}
	for _, line := range strings.Split(string(data), "\n") {
		mountpoint, fstype, ok := ParseMountinfoLine(line)
		if !ok {
			continue
		}
		if mountpoint == target && IsFUSE(fstype) {
			return mountpoint, fstype, true
		}
	}
	return "", "", false
}

// IsFUSE reports whether fstype is a FUSE filesystem type.
func IsFUSE(fstype string) bool {
	return fstype == "fuse" || strings.HasPrefix(fstype, "fuse.")
}

// ParseMountinfoLine parses one mountinfo line: the mountpoint is field 5,
// the filesystem type follows the `-` separator. Paths are octal-escaped
// (\040 = space).
func ParseMountinfoLine(line string) (mountpoint, fstype string, ok bool) {
	// 1 mount id, 2 parent id, 3 major:minor, 4 root, 5 mount point, 6 opts.
	fields := strings.Split(line, " ")
	if len(fields) < 6 {
		return "", "", false
	}
	rest := fields[6:]
	sep := -1
	for i, f := range rest {
		if f == "-" {
			sep = i
			break
		}
	}
	if sep < 0 || sep+1 >= len(rest) {
		return "", "", false
	}
	return UnescapeMountinfo(fields[4]), rest[sep+1], true
}

// UnescapeMountinfo decodes a mountinfo path's octal escapes (\040 etc).
func UnescapeMountinfo(raw string) string {
	var out strings.Builder
	out.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' && i+3 < len(raw) {
			if v, err := parseOctal(raw[i+1 : i+4]); err == nil {
				out.WriteByte(v)
				i += 3
				continue
			}
		}
		out.WriteByte(raw[i])
	}
	return out.String()
}

func parseOctal(s string) (byte, error) {
	v := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '7' {
			return 0, syscall.EINVAL
		}
		v = v*8 + int(s[i]-'0')
	}
	return byte(v), nil
}

// normalizePath: absolute, symlink-resolved where possible. On a dead
// mount filepath.EvalSymlinks itself can fail, so fall back to resolving
// the parent and keeping the final component.
func normalizePath(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if canonical, err := filepath.EvalSymlinks(path); err == nil {
		return canonical
	}
	parent, name := filepath.Split(path)
	parent = strings.TrimSuffix(parent, string(filepath.Separator))
	if parent == "" || name == "" {
		return path
	}
	if canonical, err := filepath.EvalSymlinks(parent); err == nil {
		return canonical + "/" + strings.TrimSuffix(name, "/")
	}
	return path
}
