---
name: Screenshots
description: How to capture and show screenshots of the app/desktop on this machine (Wayland/niri, grim) whenever a change needs visual verification
---

Take a real screenshot and embed it in the reply **every time** a UI change
should be shown or verified — never claim a visual fix without one. Never use
`browser.preview` or `html_preview` for native app work; they cannot show this
app.

## Tool: grim (Wayland)

The machine runs niri on Wayland. `grim` is not on `$PATH`; resolve it fresh
each time (nix store paths are garbage-collected):

```sh
GRIM=$(ls /nix/store/*/bin/grim 2>/dev/null | head -1)
```

If empty, fetch it: `nix shell nixpkgs#grim` then use
`$(ls /nix/store/*/bin/grim | head -1)`.

## Capture — prefer per-window, never steal focus (IMPORTANT)

The user works in parallel; do NOT focus or raise the app window to
screenshot it — that yanks their view to the app. niri 26.04 can capture a
window by ID directly, **without focusing it**, wherever it sits:

```sh
export XDG_RUNTIME_DIR=/run/user/1000 NIRI_SOCKET=/run/user/1000/niri.wayland-1.1777.sock
# Find the window id (test instance: App ID "proton-drive-sync")
niri msg windows | grep -B4 '"proton-drive-sync"'
# Capture just that window; focus stays wherever the user left it
niri msg action screenshot-window --id <ID>
# Newest file lands in ~/Pictures/Screenshots/ — copy it to /tmp/opencode
cp "$(ls -t "$HOME/Pictures/Screenshots"/Screenshot*.png | head -1)" /tmp/opencode/shot-name.png
```

**Never use `niri msg action focus-window` for screenshots.** Only reach for
`grim` full-screen captures when the user has explicitly asked to see the
whole desktop — and say so before doing it.

## Capture (full screen, only when asked)

Full screen (verify `$WAYLAND_DISPLAY`/`XDG_RUNTIME_DIR` match the session the
app runs in — the test app runs with `XDG_RUNTIME_DIR=/run/user/1000`,
`WAYLAND_DISPLAY=wayland-1`):

```sh
WAYLAND_DISPLAY=wayland-1 XDG_RUNTIME_DIR=/run/user/1000 $GRIM /tmp/opencode/shot-name.png && echo OK
```

A single output:

```sh
OUT=$(nix shell nixpkgs#sway -c swaymsg -s <ipc-sock> -t get_outputs -r | grep -o '"name": "[^"]*"' | head -1 | cut -d'"' -f4)
WAYLAND_DISPLAY=wayland-1 XDG_RUNTIME_DIR=/run/user/1000 $GRIM -o "$OUT" /tmp/opencode/shot.png
```

(Sway's `swaymsg` works against niri's IPC only if an IPC socket env var is
set; the plain full-screen form is usually enough.)

## Show the result

Embed the PNG in the reply with its absolute path:

```markdown
![screenshot](/tmp/opencode/shot-name.png)
```

Then **read the image back yourself** (the `read` tool renders images) and
actually verify the claim before finishing — a screenshot that wasn't
inspected proves nothing. Compare against the reference design, not against
"looks plausible".

## App-specific workflow (proton-drive-sync test instance)

1. Make sure the app is running (PID in `/tmp/pds-user-test-runtime/app.pid`;
   `ps aux | grep "[.]\/proton-drive-sync"`).
2. Rebuild first: `cd mygo-shell && go build -o proton-drive-sync .`
3. Restart it to pick up changes: kill the PID, kill the engine
   (`pds-engine-headless`), `fusermount3 -uz` stale mounts, remove stale
   sockets in `/run/user/1000/`, relaunch with the LD_LIBRARY_PATH block (see
   the runtime notes in this repo's session history or `mygo-shell/` scripts).
4. Wait a few seconds for the engine (status bar shows live counts when up).
5. Screenshot the window by ID (no focus — see above), read it, embed it.

Never screenshot or touch the user's production instance (PID ~2136, FUSE on
`~/ProtonDrive`) — only the test instance with `PDS_SYNC_ROOT=/tmp/pds-user-test-root`.
