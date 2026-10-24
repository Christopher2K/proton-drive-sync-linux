# GPUI integration guide (for the M2 shell)

Research date: 2026-10-23. Every API referenced below was verified against the
published `gpui-0.2.2` crate source (downloaded from crates.io) and the
official examples in `zed-industries/zed` at tag `v0.209.7` (the last tag whose
`crates/gpui` matches the 0.2.x line). Serves SPEC.md §6 (menu-bar icon,
status window, pause/resume, conflict inbox, transfer list).

## TL;DR — decisions for M2

| Question | Answer |
|---|---|
| Which gpui? | crates.io `gpui = "0.2"` (currently 0.2.2). Do **not** track the zed git repo. |
| Which ecosystem? | **Upstream** (`zed-industries`), not the `gpui-kit`/`gpui-ce` forks — types don't mix across them. |
| Tray / NSStatusItem? | Not in gpui. Use [`gpui-tray`](https://github.com/domenkozar/gpui-tray) `0.3` (default features only). |
| App shape | Windowless menu-bar app; open/destroy the status window on demand (gpui has no window *hide*). |
| Engine → UI bridge | `Context::spawn` task looping over a channel receiver, `AsyncApp::update` + `WeakEntity::update` + `cx.notify()`. |
| Widgets | Lists (`uniform_list`), scrolling, clicks, text, images out of the box; buttons/inputs/checkboxes are DIY styled divs. |
| Packaging | Real `.app` bundle with `LSUIElement=YES` (cargo-bundle or a shell script). `cargo run` works for dev but shows a dock icon. |

---

## 1. Which crate to depend on today

**`gpui = "0.2"` from crates.io.** Facts:

- crates.io `gpui` is at **0.2.2** (published 2025-10-22, Apache-2.0, edition
  2024 — needs a recent rustc). The old `0.1.0` placeholder is yanked.
- Zed's own `main` branch still versions `crates/gpui` as **0.2.2** — the
  crates.io release *is* current; there is nothing newer to track in git.
  Tracking git would only buy you churn.
- Historical context: GPUI was git-only until the 0.2.0 crates.io release in
  2025-10. In 2026 Zed publicly deprioritized all GPUI work not directly
  serving Zed (HN thread on the blade→wgpu switch), so expect slow upstream
  responses to non-Zed issues; be prepared to `[patch.crates-io]` if we hit a
  blocking bug.
- **Fork fragmentation is the one real trap.** Three incompatible lines exist:
  - upstream `gpui` 0.2.2 (zed-industries) — what we use;
  - `gpui-pre` 0.3.x ("GPUI Kit", longbridge) — the fork that
    `gpui-component` 0.7.1 depends on (`gpui = { package = "gpui-pre", version = "=0.3.8" }`),
    so **gpui-component cannot be combined with upstream gpui**;
  - `gpui-ce` 0.3 (community edition, started by an ex-Zed employee) — nearly
    inactive (one merged PR, then stalled).
  Rust types (`Entity`, `App`, …) don't unify across these; an app and all its
  gpui-adjacent libraries must share one source. We pick upstream: our widget
  needs are modest and `gpui-tray` (below) supports upstream by default.

```toml
[dependencies]
gpui = "0.2"
```

## 2. Minimal app skeleton + dashboard layout

### 2.1 Compilable hello world (verified against the official example)

```rust
use gpui::{
    App, Application, Bounds, Context, SharedString, Window, WindowBounds,
    WindowOptions, div, prelude::*, px, rgb, size,
};

struct HelloWorld {
    text: SharedString,
}

impl Render for HelloWorld {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .bg(rgb(0x505050))
            .size_full()
            .justify_center()
            .items_center()
            .text_xl()
            .text_color(rgb(0xffffff))
            .child(format!("Hello, {}!", &self.text))
    }
}

fn main() {
    Application::new().run(|cx: &mut App| {
        let bounds = Bounds::centered(None, size(px(800.), px(600.)), cx);
        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |_, cx| cx.new(|_| HelloWorld { text: "Proton Drive".into() }),
        )
        .unwrap();
    });
}
```

Key facts: `Application::new().run(|cx: &mut App| …)` is the entry point;
`cx.open_window(WindowOptions, FnOnce(&mut Window, &mut App) -> Entity<V>)`
creates a window whose root is an `Entity<V: Render>`;
`Render::render(&mut self, &mut Window, &mut Context<Self>) -> impl IntoElement`
is called on the main thread whenever the entity is notified. `div()` is the
workhorse element; styling is tailwind-ish chained methods (`flex`, `flex_col`,
`items_center`, `justify_between`, `gap_2`, `px_2`, `p_4`, `size_full`,
`w_full`, `h_full`, `bg`, `text_color`, `text_sm`/`base`/`lg`/`xl`,
`font_weight(FontWeight::SEMIBOLD)`, `border_1`, `border_r`, `rounded_md`,
`shadow_lg`, `hover(|s| s.bg(...))`). Units: `px(f32)`, `size(w, h)`,
`Bounds::centered(None, size, cx)`; colors: `rgb(0xRRGGBB)`, `rgba()`,
`hsla()`, plus constants `gpui::red()`, `gpui::white()`, etc. Strings and
`SharedString` are `IntoElement`, so `.child(format!("…"))` just works.

### 2.2 Dashboard-ish layout (sidebar + list + toolbar)

This is the shape the M2 status window wants. All methods used here are
verified against 0.2.2.

```rust
use gpui::{div, prelude::*, px, rgb, uniform_list, Context, ScrollHandle, Window};

struct Dashboard {
    status: pds_core::EngineStatus,          // mirrored from the engine (§3)
    roots: Vec<SyncRootRow>,                 // our own view data
    activity: Vec<ActivityRow>,
    activity_scroll: ScrollHandle,           // for the non-virtualized column
}

impl Render for Dashboard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .size_full()
            .bg(rgb(0x1e1e1e))
            .text_color(rgb(0xd4d4d4))
            .text_sm()
            // ---- sidebar ----
            .child(
                div()
                    .id("sidebar")              // needed for scroll/stateful interactivity
                    .flex()
                    .flex_col()
                    .gap_1()
                    .w(px(220.))
                    .h_full()
                    .p_3()
                    .border_r_1()
                    .border_color(rgb(0x333333))
                    .overflow_y_scroll()
                    .track_scroll(&self.activity_scroll)
                    .children(self.roots.iter().map(|r| {
                        div()
                            .id(("root", r.id))   // ElementId: &str / usize / tuples
                            .px_2()
                            .py_1()
                            .rounded_md()
                            .cursor_pointer()
                            .hover(|s| s.bg(rgb(0x2d2d2d)))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.select_root(r.id);
                                cx.notify();
                            }))
                            .child(r.name.clone())
                    })),
            )
            // ---- main column ----
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .h_full()
                    // toolbar
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px_3()
                            .py_2()
                            .border_b_1()
                            .border_color(rgb(0x333333))
                            .child(div().text_base().child("Proton Drive Sync"))
                            .child(self.pause_button(cx)),
                    )
                    // virtualized activity list (only visible rows are laid out)
                    .child(
                        uniform_list(
                            "activity",
                            self.activity.len(),
                            cx.processor(|this, range, _window, _cx| {
                                range
                                    .map(|ix| {
                                        div()
                                            .id(ix)
                                            .px_3()
                                            .py_1()
                                            .child(this.activity[ix].summary.clone())
                                    })
                                    .collect()
                            }),
                        )
                        .flex_1(),
                    ),
            )
    }
}
```

Notes:

- `cx.listener(|this, event, window, cx| …)` adapts the usual
  `Fn(&E, &mut Window, &mut App)` callback into one that also gives you the
  view — the standard way to mutate state from clicks.
- `cx.processor(|this, range, window, cx| …)` does the same for
  `uniform_list`'s row renderer.
- `uniform_list(id, item_count, f)` requires **uniform row height**; for
  variable-height rows there is `list(&ListState, f)` (heavier). For our
  activity log and conflict inbox, `uniform_list` is the right tool.
- Plain scrollable div: give it an `.id(…)` (scrolling lives on
  `StatefulInteractiveElement`), then `.overflow_y_scroll()`; hold a
  `ScrollHandle` only if you need programmatic scrolling.

## 3. Reactivity model + bridging the tokio sync engine

### 3.1 The model

- State lives in **entities**: structs created with `cx.new(|cx| MyView { … })`,
  handed around as `Entity<T>` handles. All mutation happens through
  `&mut Context<T>` (or `App`) — always on the main thread.
- **Re-render trigger:** `cx.notify()` marks the entity dirty; GPUI re-renders
  any view backed by it next frame. There is no fine-grained reactivity —
  notify is per-entity, so keep entities small enough that re-rendering them is
  cheap, and use `uniform_list` for large collections.
- **Cross-entity:** `cx.observe(&other_entity, |this, observed, cx| …)` (fires
  when the observed entity notifies), and `cx.subscribe(&emitter, |this,
  emitter, event, cx| …)` for typed events via `impl EventEmitter<MyEvent> for
  T` + `cx.emit(event)`. Both return a `Subscription` that **cancels on drop** —
  store it in your view struct or call `.detach()`.
- **Async:** `Context::spawn(|this: WeakEntity<T>, cx: &mut AsyncApp| async
  move { … })` runs a future on GPUI's foreground executor; the returned
  `Task` must be `.detach()`ed or held. `cx.background_executor()` runs `Send`
  futures on a thread pool and has `.timer(Duration)`; `cx.foreground_executor()`
  is the main-thread executor. The executor is smol-based (`gpui::Timer` is
  re-exported from smol) — **not tokio**.

### 3.2 Tokio bridge (the canonical pattern)

The M1 engine runs on its own tokio runtime and produces `EngineStatus`
snapshots. Bridge it with a channel at the boundary and one long-lived GPUI
task that pumps it into the entity:

```rust
// Engine side (tokio): keep a tokio::sync::watch::Sender<EngineStatus> or an
// mpsc::UnboundedSender. tokio's channel futures (recv/changed) do NOT need a
// tokio runtime context, so they can be awaited on GPUI's smol executor —
// what panics off-runtime is tokio::time::*, tokio::spawn, etc.

struct Dashboard {
    status: EngineStatus,
    _pump: Task<()>,
}

impl Dashboard {
    fn new(engine: EngineHandle, cx: &mut Context<Self>) -> Self {
        let mut rx = engine.subscribe_status(); // tokio watch/mpsc receiver
        let pump = cx.spawn(|this, mut cx| async move {
            while rx.changed().await.is_ok() {
                let status = rx.borrow().clone();
                // Hop back to the main-thread context; fails during shutdown.
                let Ok(()) = cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.status = status;
                        cx.notify();               // triggers re-render
                    })
                }) else { break };
            }
        });
        Self { status: engine.current_status(), _pump: pump }
    }
}
```

Rules that bite if ignored:

- `WeakEntity::update(&self, cx, f) -> Result<…>` errors once the entity is
  released (window closed) — treat `Err` as "stop the task".
- `AsyncApp::update` likewise fails while the app is shutting down; break the
  loop instead of unwrapping.
- Never hold `&mut Context<T>` across an `.await`; capture the `WeakEntity`
  and re-acquire through `cx.update`.
- Need a periodic poller instead of push (e.g. `engine.status().await` every
  second)? `cx.spawn` + `cx.background_executor().timer(dur).await` in a loop;
  do not use `tokio::time::sleep` — it panics without a tokio runtime.
- Commands the other way (UI → engine, e.g. the pause/resume toggle) are just
  method calls on the cloneable `EngineHandle` from a click handler; if the
  call is async, fire it with `cx.spawn` (or straight onto the engine's tokio
  runtime handle) and let the status channel deliver the resulting state —
  don't block the main thread.

## 4. Widgets: out of the box vs build-your-own

**In `gpui` 0.2.2 (`gpui::elements`):** `div` (layout + interactivity),
`text`, `img`, `svg`, `canvas` (custom painting), `list` + `ListState`
(variable-height virtualized), `uniform_list` + `UniformListScrollHandle`
(fixed-height virtualized — verified example `uniform_list.rs`), `anchored`,
`deferred` (overlay/popover positioning primitives), `surface`. Scrolling via
`.overflow_y_scroll()` + `ScrollHandle`. Clicks via `.on_click`,
`.on_mouse_down`, hover styling, `.cursor_pointer()`. That's the whole kit.

**Not included — DIY or ecosystem:**

| Need | Status | Plan for M2 |
|---|---|---|
| Button | None. A styled `div` with `.id`, `.cursor_pointer`, `.hover`, `.on_click` is the idiom. | Tiny internal `button(label, cx, on_click)` helper. |
| Checkbox / toggle | None. Same idiom, flip a bool + re-render. | DIY. |
| Text input | No widget. There is a full IME-capable *trait stack* (`EntityInputHandler`, `InputHandler`) but you implement and wire it yourself; the official `input.rs` example (~24 KB) is the reference. | Avoid in M2 (only use-case would be cache-cap editing; prefer a stepper of buttons). |
| Dropdown / context menu | DIY from `deferred`/`anchored`. | Probably unneeded; the tray menu covers global actions. |
| Menu bar menus (macOS) | Yes: `cx.set_menus(Vec<Menu>)`, `cx.set_dock_menu(Vec<MenuItem>)`; `MenuItem::action("Name", MyAction)` dispatches a GPUI action registered with `actions!(…)` + `cx.bind_keys`/`.on_action`. | Minimal app menu (Quit). |
| **System tray / NSStatusItem** | **No.** `src/platform/mac/status_item.rs` exists in the crate but is dead legacy code — not compiled into the module tree (absent from `platform/mac.rs`), using pre-0.1 APIs (`cocoa`, `RectF`). `WindowKind` is only `Normal`/`PopUp`/`Floating`. | Use `gpui-tray` (below). |

**Tray verdict — [`gpui-tray`](https://github.com/domenkozar/gpui-tray) 0.3:**
native tray built for GPUI's event loop (macOS via `objc2` AppKit, Linux
StatusNotifierItem over `zbus`, Windows `Shell_NotifyIconW`). Its default
backend is crates.io `gpui 0.2.2` — exactly our dependency. Menus are built
from `gpui::MenuItem` and dispatch ordinary GPUI actions; its example app is
**windowless**, i.e. precisely our menu-bar shape:

```rust
use gpui::{actions, App, MenuItem};
use gpui_tray::{Icon, Tray};

actions!(pds, [OpenStatus, TogglePause, QuitApp]);

fn build_tray(cx: &mut App, rgba: Vec<u8>) -> gpui_tray::Result<Tray> {
    let icon = Icon::from_rgba(rgba, 36, 36)
        .scale_factor(2.0)
        .template(true)        // adapts to light/dark menu bar
        .build()?;
    Tray::builder()
        .icon(icon)
        .tooltip("Proton Drive Sync")
        .menu(|_| vec![
            MenuItem::action("Open Status", OpenStatus),
            MenuItem::action("Pause Syncing", TogglePause),
            MenuItem::separator(),
            MenuItem::action("Quit", QuitApp),
        ])
        .build(cx)             // keep the returned Tray alive
}
```

Caveats: enable **default features only** — its `menu-state` feature (checked/
disabled items) requires `[patch.crates-io]`-ing gpui to a git rev, which we
don't want. Checked-state for the pause toggle can be faked by rebuilding the
menu (`Tray::refresh_menu`) with a "Resume Syncing" label. SF Symbol icons are
supported (`Icon::system_symbol("externaldrive")`).

## 5. Multi-window and close behavior for a menu-bar app

- Multiple windows are just repeated `cx.open_window(...)` calls; each returns
  a `WindowHandle<V>` you can keep to focus/activate later
  (`window.activate_window()`).
- **There is no window-level hide in 0.2.2** (no `Window::hide`/`set_visible`;
  `WindowOptions.show` is initial-only). The two lifecycle levers:
  - `window.remove_window()` — destroy now;
  - `window.on_window_should_close(cx, |window, cx| bool)` — intercept the
    traffic-light close; returning `false` vetoes it.
- `cx.on_window_closed(|cx| …)` fires after any window closes. The official
  `on_window_close_quit.rs` example shows the conventional desktop pattern:
  `if cx.windows().is_empty() { cx.quit() }`. **We must not do that** — a
  menu-bar app lives with zero windows (this works fine; the gpui-tray example
  never opens one, and upstream zed discussion #40318 confirms background
  running is the intended usage even if some folks hit rough edges).
- Recommended M2 lifecycle:

```rust
fn main() {
    Application::new().run(|cx: &mut App| {
        let _tray = build_tray(cx).expect("tray");      // keeps app alive
        cx.set_menus(vec![Menu { name: "Proton Drive".into(),
                               items: vec![MenuItem::action("Quit", QuitApp)] }]);

        // Tray action → open (or re-open) the status window.
        cx.on_action(|_: &OpenStatus, cx| { open_status_window(cx); });
        // Never quit when the status window closes:
        //   - no cx.on_window_closed(...quit...) handler at all.
        // Dock-click reopen (only relevant if we ever ship without LSUIElement):
        cx.on_reopen(|cx| { if cx.windows().is_empty() { open_status_window(cx); } });
    });
}

fn open_status_window(cx: &mut App) {
    let bounds = Bounds::centered(None, size(px(860.), px(560.)), cx);
    cx.open_window(
        WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            titlebar: Some(TitlebarOptions {
                title: Some("Proton Drive Sync".into()),
                appears_transparent: false,
                traffic_light_position: None,
            }),
            ..Default::default()
        },
        |_, cx| cx.new(|cx| Dashboard::new(engine_handle(), cx)),
    )
    .ok();
}
```

Closing the window destroys the entity; the status-pump task from §3 notices
(`WeakEntity::update` starts erroring) and exits. Re-opening builds fresh
state from the engine handle — cheap and simple. Also useful: `cx.hide()` /
`cx.hide_other_apps()` (NSApplication-level hide), `cx.activate(true)` before
opening from the tray so the window comes to the front,
`window.minimize_window()`.

## 6. Packaging on macOS

**Dev loop:** plain `cargo run` works — all official gpui examples run
unbundled. Caveats unbundled: the app name in the menu bar is wrong/empty,
activation policy defaults to Regular (dock icon appears), and `LSUIElement`
can only come from a bundle. Fine while iterating on layout.

**Shipping a menu-bar app requires a real `.app` bundle** — `LSUIElement=YES`
(agent app: no dock icon) exists only in `Info.plist`, and gpui exposes no
activation-policy API. Minimal bundle:

```
ProtonDriveSync.app/
  Contents/
    Info.plist
    MacOS/proton-drive-sync      # the release binary
    Resources/AppIcon.icns       # optional but expected
```

Minimum `Info.plist` keys that matter for us:

```xml
<dict>
  <key>CFBundleName</key>               <string>Proton Drive Sync</string>
  <key>CFBundleExecutable</key>         <string>proton-drive-sync</string>
  <key>CFBundleIdentifier</key>         <string>ch.drive-sync.proton</string>
  <key>CFBundlePackageType</key>        <string>APPL</string>
  <key>CFBundleShortVersionString</key> <string>0.1.0</string>
  <key>CFBundleVersion</key>            <string>1</string>
  <key>LSMinimumSystemVersion</key>     <string>13.0</string>
  <key>LSUIElement</key>                <true/>   <!-- menu-bar agent: no Dock icon -->
  <key>NSHighResolutionCapable</key>    <true/>
</dict>
```

(`NSPrincipalClass` can be omitted — default `NSApplication` is what gpui
drives. With `LSUIElement`, the app menu still appears in the menu bar while
one of our windows is focused, so `cx.set_menus` remains worth doing.)

Tooling: Zed itself bundles with a **zed fork of cargo-bundle**
(`cargo install cargo-bundle --git https://github.com/zed-industries/cargo-bundle --branch zed-deploy`,
metadata in `[package.metadata.bundle]`, `osx_info_plist_exts` partial
plists). We don't need the fork — upstream `cargo-bundle` or a 20-line shell
script (`mkdir -p …/Contents/MacOS`, copy binary + plist, `codesign --sign -`)
is enough. Apple-Silicon binaries are ad-hoc signed by the linker already;
real distribution later needs Developer-ID signing + notarization, same as
any macOS app. Nothing gpui-specific is required at runtime (Metal is
always present on supported macOS).

## 7. Sharp edges and where the living examples are

1. **Docs gap.** crates.io lists `documentation: null`; docs.rs has raw API
   docs only. The actual documentation is the source plus the examples in
   `crates/gpui/examples/` — use tag **`v0.209.7`** (matches 0.2.2-era API).
   Anything older on the web (blogs, gpui.rs snippets, AI training data) is
   usually pre-0.2 and won't compile: the `Render::render` signature
   (window/context params), `open_window` closure shape, and the view/entity
   split all changed repeatedly before 0.2.
2. **Fork fragmentation (biggest ecosystem risk).** upstream `gpui` 0.2.2 vs
   `gpui-pre` 0.3.x (gpui-kit, required by `gpui-component` 0.7.1) vs `gpui-ce`
   0.3 — mutually incompatible. If we ever want gpui-component's 60+ widgets
   (real text input, tables, date pickers…), that's an all-in switch to the
   gpui-kit line, not a dependency we add. Staying upstream + DIY is right for
   M2's scope.
3. **Upstream maintenance is Zed-focused.** Zed publicly deprioritized
   non-Zed GPUI work in 2026. Expect slow merges for external fixes; keep the
   option of `[patch.crates-io] gpui = { git = …, rev = … }` warm.
4. **Dead code that looks alive.** `platform/mac/status_item.rs` (tray) and
   other pre-0.2 leftovers are shipped in the crate but not compiled. Grep the
   module tree, not just filenames, before trusting a capability.
5. **Executor mismatch.** GPUI's executor is smol. `tokio::spawn` /
   `tokio::time::*` inside gpui tasks panic; engine calls must stay on the
   engine's runtime (or be channel-based). Channel recv/notify primitives are
   executor-agnostic and safe (§3.2).
6. **Lifetime discipline.** `Subscription` cancels on drop; `Task` cancels on
   drop — store or `.detach()` both. Async closures get a `WeakEntity` that
   fails after the view dies; handle the `Result`.
7. **Compile times.** gpui pulls blade/wgpu, metal bindings, font stack —
   debug builds are heavy; expect slow first builds and sluggish debug-mode
   rendering on large lists (release-profile the UI crate if it hurts:
   `[profile.dev.package.gpui] opt-level = 2`).
8. **macOS-only notes for us:** no window hide (§5), no tray (§4),
   `LSUIElement` only via bundle (§6).

**Best living examples, in order of relevance to M2:**

- `zed-industries/zed` @ `v0.209.7`, `crates/gpui/examples/`:
  `hello_world.rs` (skeleton), `uniform_list.rs` (virtualized list +
  `cx.processor`), `on_window_close_quit.rs` (window lifecycle, actions,
  key bindings), `set_menus.rs` (menu bar), `input.rs` (the text-input
  reference), `window.rs`, `data_table.rs`, `tree.rs`.
- `domenkozar/gpui-tray` → `examples/tray.rs`: windowless menu-bar app with
  action-dispatching tray menus — closest existing thing to our M2 shape.
- `longbridge/gpui-component` (gpui-kit line): a large gallery of real
  application UIs; useful as *design* reference even though we can't link it.
- Zed itself (`crates/workspace`, `crates/ui`) for production-grade patterns
  at scale.

## Appendix: proposed dependency sketch for the M2 shell crate

```toml
[dependencies]
gpui = "0.2"
gpui-tray = "0.3"          # default features only (menu-state needs a git patch — avoid)
pds-core = { path = "../core" }
tokio = { version = "1", features = ["sync", "rt-multi-thread", "macros"] }
tracing = "0.1"

# Optional, if debug-mode UI feels heavy:
# [profile.dev.package.gpui]
# opt-level = 2
```
