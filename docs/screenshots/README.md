# Screenshots

Canonical runtime screenshots of the MyGo shell. They are used by the
top-level [README.md](../../README.md) and by PR descriptions; keep the
filenames stable so those links don't break.

| File | What it shows |
|---|---|
| `mygo-shell-login-runtime.png` | Sign-in screen (no Proton session). |
| `mygo-shell-overview-runtime.png` | Overview: sync counters, attention items, recent activity. |
| `mygo-shell-activity-runtime.png` | Activity: filterable event log. |
| `mygo-shell-conflicts-runtime.png` | Conflicts: keep-both copies and failed uploads. |
| `mygo-shell-settings-runtime.png` | Settings: account, sync mode, cache cap, sync folder. |
| `mygo-shell-runtime.png` | Engine failure card. |

## How they were made

Rendered with the shell's own headless Tester (`ui.NewTester`) — no
compositor, no real account, no live sync. Each shot drives the app into
the phase/page being captured (populated demo state, placeholder email)
and dumps the frame as PNG.

> **Text rendering needs the system libraries.** The Tester rasterizes
> glyphs with Pango, which mygo `dlopen`s. Without the GTK library path it
> silently renders with no text at all ("text will not show"). On this
> machine, run it with every Nix store lib on `LD_LIBRARY_PATH`:
>
> ```sh
> LD_LIBRARY_PATH="$(ls -d /nix/store/*/lib | tr '\n' ':')" \
>   go test -count=1 -run TestPRScreenshots .
> ```
>
> (Inside `devenv shell` the path is already set via `LD_LIBRARY_PATH`.)
> Always verify a shot has text before committing — file size jumps from
> ~20 KB to 30–80 KB once glyphs render.
