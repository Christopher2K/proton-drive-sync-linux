// Package conflict implements F7 keep-both conflict naming:
// `name (conflict YYYY-MM-DD HHmm).ext`.
//
// The suffix is UTC. Local wall-clock time repeats during a DST fold and can
// jump backwards when the system clock is corrected; using UTC keeps the
// generated names monotonic-enough and stable for a given instant. Existing
// local-time conflict copies are ordinary files: dedup only checks whether a
// candidate name is taken, so both naming eras coexist without parsing.
//
// Port of crates/core/src/conflict.rs.
package conflict

import (
	"fmt"
	"strings"
	"time"
)

// Name builds the keep-both name for name at time at (SPEC.md F7).
//
// The extension (everything after the last dot) is preserved; dotfiles and
// extension-less names get the suffix appended whole.
func Name(name string, at time.Time) string {
	stamp := at.UTC().Format("2006-01-02 1504")
	if i := strings.LastIndex(name, "."); i > 0 { // i > 0: dotfile like ".env" has no "extension"
		return fmt.Sprintf("%s (conflict %s).%s", name[:i], stamp, name[i+1:])
	}
	return fmt.Sprintf("%s (conflict %s)", name, stamp)
}
