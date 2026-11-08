---
name: MyGo UI
description: Build native desktop interfaces and components with the mygo Go framework (package ui) — views, layout, styling, scrolling, state, threading, and debugging; distilled from building the proton-drive-sync mygo-shell
---

Build and debug native MyGo interfaces (`github.com/egoist/mygo/ui`). All
lessons below were learned building `mygo-shell/` in this repo; the module
source is the API reference:

```
~/go/pkg/mod/github.com/egoist/mygo@v0.7.4/
├── AGENTS.md            # framework hard rules (no cgo, threading)
└── ui/
    ├── doc.go           # mental model of views/elements — read this first
    ├── element.go       # all styling/layout chain methods
    ├── layout.go        # flexbox semantics incl. the stretch rules
    ├── scroll.go        # ScrollState / TrackScroll
    ├── input.go         # Clicked/Hovered/Changed queries
    └── api_gen.go       # widget constructors (Button, Text, Scroll, …)
```

## Mental model

- A **view** is `func(c *ui.Context)` that builds the whole frame from app
  state, Dear ImGui / egui style. MyGo re-calls it after input, after
  `win.Invalidate()`, and while anything animates.
- **Never keep elements in app state.** Elements expire at the end of the
  build pass. Store only plain data; rebuild everything each frame.
- Controls report input **inline as they are built**:
  ```go
  if ui.PrimaryButton(c, "Sign in").Clicked() { ... }
  ```
- State that survives frames (scroll position, text edits, focus) follows the
  element's **position among siblings**, or its `.Key(...)` if the list can
  reorder.

## File layout for an app shell

One file per concern, same package `main` (see `mygo-shell/`):

| File | Contents |
|---|---|
| `main.go` | `main()`, window/tray lifecycle, `//go:embed` icon |
| `state.go` | domain types, enums, formatting helpers |
| `app.go` | app struct, constructor, derived-state helpers |
| `engine.go` | background process/socket work, `applyUpdate` |
| `theme.go` | design tokens + shared component constructors |
| `shell.go` | root view, chrome (sidebar/header/status bar), routing |
| `page_*.go` | one file per page |
| `modals.go` | overlay + dialogs |
| `actions.go` | user actions that mutate state and invalidate |

## Threading — the #1 source of deadlocks

The UI runs on the **main thread**. Background goroutines own the work, then
hand results to the UI:

```go
// engine goroutine
a.mu.Lock()
...mutate plain state...
a.mu.Unlock()
a.invalidate() // win.Invalidate() is safe from any goroutine
```

**Never call a mygo API that marshals to the main thread while holding your
state mutex.** `tray.SetToolTip`, dialogs, clipboard — they post to main,
which may be inside `view()` waiting for that same mutex → deadlock. Pattern:

```go
tooltip := computeUnderLock()
a.mu.Unlock()
a.tray.SetToolTip(tooltip) // outside the lock
a.mu.Lock()                // or restructure without defer Unlock
```

Avoid `defer a.mu.Unlock()` in functions that must call UI APIs partway;
unlock explicitly. Keep lock scopes small: snapshot fields, unlock, then
build UI from the snapshots (see `renderSettings` in `page_settings.go`).

## Layout semantics (differs from web/GPUI!)

Read `ui/layout.go` before fighting layout:

1. **Flexbox everywhere.** `ui.Row`, `ui.Column`; `.Gap(v)`, `.Padding(...)`,
   `.AlignItems(a)` (cross axis), `.Justify(a)` (main axis), `.Center()`.
2. **Column children STRETCH horizontally by default** (`alignAuto` →
   `Stretch`). A button inside a column becomes a full-width bar. To hug:
   `.AlignSelf(ui.Start)` on the child or set `.AlignItems(ui.Start)` on the
   column.
3. **`.Fill()` overrides an explicit `Width`** — it sets width AND height to
   100%. Sidebar: `.Width(sidebarWidth).FillHeight()`, never `.Fill()`.
4. **Nothing stretches on the main axis without `.Grow(1)`** — and `Grow` on
   a card that should hug its content will squeeze it below content height;
   children then paint past the card edges. Prefer `.FillWidth()` + explicit
   `.MinHeight(0)` for scroll areas inside a sized parent.
5. Sizes are **DIPs** (float32). `WidthPercent/FillWidth` resolve against the
   parent.

## Scrolling

```go
// app state
var settingsScroll ui.ScrollState

// per frame — do NOT reset ScrollState in the render path
ui.Scroll(c).FillWidth().Grow(1).MinHeight(0).TrackScroll(&s.settingsScroll).Children(...)
```

- **`TrackScroll` owns the state while rendering.** Setting
  `scrollState.Y = 0` inside the view fights the user's mouse wheel every
  frame → scrolling appears dead. Reset positions only on page *entry*
  (navigation action), never in the view function.
- Keep one `ScrollState` per page so each page remembers its position.

## Styling

```go
ui.Box(c).Background(col).Border(1, col).Radius(12).Padding(16).Children(...)
ui.Text(c, "…").FontSize(t.FontSize*0.85).TextColor(t.TextMuted).Bold()
```

- Colors: `ui.RGB(r, g, b)`, `ui.RGBA(r, g, b, alpha)`. Define design tokens
  once in `theme.go` as package vars; mirror the reference design's values
  exactly when porting a design (see `crates/app/src/theme.rs` ↔
  `mygo-shell/theme.go`).
- `c.Theme()` gives themed spacing (`t.Space(n)`), font size, radius, colors.
  Scale text with `t.FontSize * factor` rather than hardcoding.
- Build small shared components as plain functions returning `ui.Element`:
  `appCard`, `pageCard`, `statusDot`, `pill`, `fieldRow`, `banner`, …
- **Port a design 1:1**: transcribe paddings, radii, gaps and widths from the
  reference component for component, not by eye. GPUI `p_6` = 24 DIPs,
  `gap_4` = 16, `p_3` = 12, `px_3 py_1p5` = hug-width button. A "card" in the
  reference with fixed width (e.g. a 460px login card) must NOT reuse the
  full-width `pageCard` helper.

## Modals

Absolute-positioned overlay + centered content:

```go
func modalOverlay(c *ui.Context, content func()) {
	ui.Box(c).Absolute().Top(0).Left(0).Right(0).Bottom(0).
		Background(ui.RGBA(0x05, 0x06, 0x0a, 0.65)).
		AlignItems(ui.Center).Justify(ui.Center).Children(content)
}
```

Gate it in the root view on a state field; render it last.

## State & actions

- State lives in one app struct guarded by `sync.Mutex`; views snapshot under
  the lock then build.
- Actions are methods: mutate state, then `a.invalidate()`.
- Engine updates arrive on background goroutines as typed update structs
  (`applyUpdate`); normalize external data there (e.g. enum strings) so UI
  comparisons stay in one canonical form. **Beware wire-format casing**: the
  engine sent `"running"` but the UI compared `"Running"` — normalize at the
  parse boundary (`normalizeEngineState`), not at 15 call sites.

## Debugging

- **Freeze/hang**: `kill -QUIT <pid>` dumps all goroutine stacks to stderr;
  look for `[sync.Mutex.Lock, locked to thread]` on the UI goroutine and find
  the lock holder in the other stack.
- **Silent no-updates**: check the socket path the app dials vs the one the
  engine actually bound (`ss -xl`, `/proc/<pid>/fd`); test the protocol with
  `nc -U /run/user/1000/<sock>` and a JSON line.
- **Element stretches wrong**: re-read rule list above; check `AlignSelf`,
  `Fill` vs `Width`, `Grow` on hug-containers.
- Verify visually with a screenshot before claiming a fix; iterate.
- Unit-test the wire protocol separately (dial the socket, assert parses) —
  keep parsing/normalize logic in plain functions.
- **UI logic CAN be tested headlessly**: `ui.NewTester` drives the real view
  with clicks/typing and asserts via `HasText`/`Texts` — see
  `mygo-shell/folder_ui_test.go` and the headless-testing skill.

## Verify loop

1. `go build ./... && go vet ./... && go test ./...`
2. Restart the app instance (see runtime notes in repo docs); kill engine and
   `fusermount3 -uz` stale mounts first — stale mounts make the engine refuse
   to start.
3. Screenshot the affected screen; compare against the reference design.
