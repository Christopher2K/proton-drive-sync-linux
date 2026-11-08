package mode

import "syscall"

// syscallEXDEV is the errno class checked by MoveFile's cross-device
// fallback (Rust: ErrorKind::CrossesDevices).
const syscallEXDEV = syscall.EXDEV
