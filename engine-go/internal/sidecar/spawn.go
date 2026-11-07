package sidecar

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SpawnSidecar resolves and spawns the sidecar (port of spawn.rs
// spawn_sidecar).
//
// With an explicit cmd (CLI flag / app setting) or $PDS_SIDECAR_CMD, that
// command line is run verbatim from the current directory. Otherwise a
// sidecar directory is located (see DiscoverSidecarDir) and the right
// launch line is chosen inside it, with the child's working directory set
// to the sidecar directory so module resolution works regardless of where
// the host was started.
func SpawnSidecar(cmd *string) (*SidecarClient, *Child, error) {
	line := ""
	if cmd != nil {
		line = *cmd
	} else if v, ok := os.LookupEnv("PDS_SIDECAR_CMD"); ok {
		line = v
	}
	if line != "" {
		program, args, err := SplitCmd(line)
		if err != nil {
			return nil, nil, err
		}
		return SpawnSidecarProcess(program, args, nil)
	}
	dir, ok := DiscoverSidecarDir()
	if !ok {
		return nil, nil, errors.New(
			"could not locate the sidecar; install it (see packaging/linux) or set " +
				"$PDS_SIDECAR_CMD to a command that runs sidecar/src/index.ts")
	}
	return spawnIn(dir)
}

// DiscoverSidecarDir finds where the sidecar lives when no explicit
// command is given, in order:
//
//  1. $PDS_SIDECAR_DIR
//  2. <exe dir>/sidecar, then <exe dir>/../sidecar (flat layouts)
//  3. <exe dir>/../share/proton-drive-sync/sidecar (system installs:
//     packaging/linux/install.sh puts binaries in $PREFIX/bin and the
//     sidecar in $PREFIX/share/proton-drive-sync/sidecar)
//  4. $XDG_DATA_HOME/proton-drive-sync/sidecar (user-local data dir)
//  5. ./sidecar (development: running from the repository root)
//
// A directory counts if it can be launched: has dist/index.js or
// src/index.ts (plus node_modules for the TypeScript entry).
func DiscoverSidecarDir() (string, bool) {
	var exeDir *string
	if exe, err := os.Executable(); err == nil {
		d := filepath.Dir(exe)
		exeDir = &d
	}
	for _, dir := range CandidateDirs(exeDir) {
		if IsLaunchable(dir) {
			return dir, true
		}
	}
	return "", false
}

// CandidateDirs returns the discovery candidates given the directory of
// the running executable (nil when it cannot be determined). Split out
// for testing.
func CandidateDirs(exeDir *string) []string {
	var candidates []string
	if dir, ok := os.LookupEnv("PDS_SIDECAR_DIR"); ok {
		candidates = append(candidates, dir)
	}
	if exeDir != nil {
		candidates = append(candidates,
			filepath.Join(*exeDir, "sidecar"),
			filepath.Join(*exeDir, "../sidecar"),
			filepath.Join(*exeDir, "../share/proton-drive-sync/sidecar"),
		)
	}
	candidates = append(candidates,
		filepath.Join(xdgDataDir(), "sidecar"),
		"sidecar",
	)
	return candidates
}

// IsLaunchable reports whether dir has a runnable sidecar entry.
func IsLaunchable(dir string) bool {
	if isFile(filepath.Join(dir, "dist", "index.js")) {
		return true
	}
	return isFile(filepath.Join(dir, "src", "index.ts")) && isDir(filepath.Join(dir, "node_modules"))
}

func isFile(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular()
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

func spawnIn(dir string) (*SidecarClient, *Child, error) {
	// `@protontech/crypto` ships raw TypeScript, so the source entry needs
	// `tsx`; the compiled entry runs on plain node. See AGENT.md sidecar
	// gotchas.
	dirCopy := dir
	if isFile(filepath.Join(dir, "dist", "index.js")) {
		client, child, err := SpawnSidecarProcess("node", []string{"dist/index.js"}, &dirCopy)
		if err != nil {
			return nil, nil, fmt.Errorf("spawn sidecar from %s: %w", dir, err)
		}
		return client, child, nil
	}
	if isFile(filepath.Join(dir, "src", "index.ts")) {
		client, child, err := SpawnSidecarProcess("npx", []string{"tsx", "src/index.ts"}, &dirCopy)
		if err != nil {
			return nil, nil, fmt.Errorf("spawn sidecar from %s: %w", dir, err)
		}
		return client, child, nil
	}
	return nil, nil, fmt.Errorf(
		"no runnable sidecar in %s (need dist/index.js or src/index.ts + node_modules)", dir)
}

// xdgDataDir returns $XDG_DATA_HOME/proton-drive-sync, defaulting to
// ~/.local/share/... (port of pds_xdg::app_data_dir; the Go mode package
// will own the canonical copy — this local one keeps the sidecar package
// self-contained per the component-port rule).
func xdgDataDir() string {
	if v, ok := os.LookupEnv("XDG_DATA_HOME"); ok && v != "" {
		return filepath.Join(v, "proton-drive-sync")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "proton-drive-sync"
	}
	return filepath.Join(home, ".local", "share", "proton-drive-sync")
}

// SplitCmd splits a sidecar command line into program + args (port of
// spawn.rs split_cmd).
func SplitCmd(line string) (string, []string, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] == "" {
		return "", nil, errors.New("empty sidecar command")
	}
	return fields[0], fields[1:], nil
}
