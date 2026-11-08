---
name: Headless UI testing
description: Drive the mygo-shell UI headlessly — click buttons, type, assert states, and capture rendered frames with mygo's ui.NewTester from plain go test; plus what doesn't work (headless sway, wlrctl, agent-device)
---

Interact with the app's UI **without a display server and without touching
the user's session**: mygo ships an in-process Tester
(`github.com/egoist/mygo/ui`, `ui/headless.go`) that renders the real view
in memory and feeds it real pointer/keyboard events. Everything below is
proven in `mygo-shell/folder_ui_test.go` — read it for working examples.

## Setup

A test is a normal `go test` in `mygo-shell/` (package `main`, so the
private `app` struct and view are reachable):

```go
a := newApp()
a.mu.Lock()
a.phase = phaseRunning        // skip the splash; or phaseNeedsLogin etc.
a.root = "/tmp/pds-user-test-root"
a.mu.Unlock()
tt := ui.NewTester(a.view, 1080, 720)  // width, height in DIPs
a.setPage(pageSettings)
tt.Frame()                     // settle frames after direct state mutation
```

`ui.NewTester(view, w, h)` starts the app's real view function. Two
elements given the same key panic — the Tester is strict, which surfaces
key bugs tests would otherwise miss.

## Interacting (clicking, typing, asserting)

```go
tt.HasText("Change…")          // bool: any element shows this text
tt.Texts()                     // every visible string — use in failure messages
tt.Click("Change…")            // click center of the element showing that
                               // label; error if absent or off-window
tt.ClickAt(x, y)               // click at DIP coordinates
tt.Type("text")                // type into the focused text input
tt.Key(ui.Mods(0), ui.KeyTab)  // press a key; TypeKey for key+text combos
tt.Frame()                     // force a settled frame after mutating state
```

Gotchas learned here:

- Handlers that end in `a.invalidate()` are **no-ops in the Tester** —
  `a.win` is nil there, and `invalidate` drops the redraw. After calling
  such a handler (e.g. `a.beginFolderChange()`), call `tt.Frame()` yourself.
- For flows where the modal field is driven through app state (cleaner than
  simulating caret movement), set the state, `tt.Frame()`, then click the
  confirm button — see `TestFolderChangeConfirm`.
- Time stands still in the Tester; `tt.Advance(d)` moves it for anything
  time-based (animations, debounces, double-click windows).

## Visual checks without a compositor

`tt.Image()` returns the settled frame as `*image.RGBA` — encode it to PNG
and read it back to verify rendering pixel-for-pixel:

```go
f, _ := os.Create("/tmp/opencode/render.png")
png.Encode(f, tt.Image())
```

This is a genuine render of the app (in-memory software renderer). Use it
for layout/spacing verification; use the real-window screenshot
(`niri msg action screenshot-window --id <ID>`, see the screenshots skill)
only when GTK/font/tray fidelity matters.

## What does NOT work (tested 2026-11-07 — don't repeat)

- **Headless sway** (`WLR_BACKENDS=headless`): runs and screenshots fine
  (grim against its `wayland-2` socket), but interaction is impossible:
  `swaymsg seat seat0 cursor …` silently no-ops (seat has no input devices,
  capabilities = 0); `wlrctl` pointer events connect but never reach the
  client; `wlrctl keyboard type` / `wtype` send without error but with no
  observable effect on this app.
- **agent-device MCP**: its Linux backend is X11-only (xdotool + AT-SPI);
  errors immediately on Wayland.
- **ydotool on the user's session**: kernel-level injection lands on
  whatever the user has focused. Forbidden — it steals their input.
- **VM**: works but heavyweight; the Tester covers everything it would.

Decision rule: interaction or assertions → Tester; static screenshot of
the real app → `screenshot-window`; never focus, never ydotool.
