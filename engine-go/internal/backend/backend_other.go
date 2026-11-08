//go:build !linux

package backend

import (
	"fmt"
	"time"

	storepkg "pds.dev/engine-go/internal/store"
)

// defaultHooks reports FUSE unavailable off Linux, so placeholder sessions
// fall back to mirror via MountUnavailable (SPEC F5: Linux-only product).
func defaultHooks() Hooks {
	return Hooks{
		Preflight: func(_ string, _ time.Duration) error { return nil },
		Probe:     func() bool { return false },
		Mount: func(_ *storepkg.Store, _, _, _ string) (func(), error) {
			return nil, fmt.Errorf("placeholder mounts require Linux FUSE")
		},
	}
}
