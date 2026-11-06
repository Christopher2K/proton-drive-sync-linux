# Screenshots

These images are used by the top-level [README.md](../../README.md) to make the
project more approachable.

| File | What it shows | How it was made |
|---|---|---|
| `login.png` | The sign-in screen shown when no Proton session exists. | Real screenshot captured from the app running under a headless Wayland compositor. |
| `overview.png` | The main dashboard: sync status, counters, recent activity, and attention items. | Representative HTML mockup rendered with Chromium, matching the app's GPUI theme and layout. |
| `settings.png` | The Settings page: account, sync mode, cache cap, and sync folder. | Representative HTML mockup rendered with Chromium. |
| `activity.png` | The Activity page: filterable event log. | Representative HTML mockup rendered with Chromium. |
| `conflicts.png` | The Conflicts page: keep-both copies and failed uploads. | Representative HTML mockup rendered with Chromium. |

## Regenerating mock screenshots

The mock screenshots are generated from the HTML files in this repository's
rendering scratch area. To rebuild them:

```sh
nix shell nixpkgs#chromium --command \
  chromium --headless --disable-gpu --no-sandbox \
    --window-size=1152,648 --hide-scrollbars \
    --screenshot=<output.png> \
    file://<path-to-html>
```

## Capturing real app screenshots

To capture a real screenshot of the login screen (or any other screen that does
not require a long-running sync):

```sh
# 1. Enter the pinned dev environment and install the sidecar
devenv shell
sidecar-install

# 2. Install a headless Wayland compositor + screenshot tool
nix shell nixpkgs#sway nixpkgs#grim

# 3. Run the app and capture
mkdir -p /tmp/pds-runtime && chmod 700 /tmp/pds-runtime
WLR_BACKENDS=headless WLR_LIBINPUT_NO_DEVICES=1 WLR_RENDERER=pixman \
  XDG_RUNTIME_DIR=/tmp/pds-runtime sway -c /dev/null &
export XDG_RUNTIME_DIR=/tmp/pds-runtime WAYLAND_DISPLAY=wayland-1

# For the login screen, use a fresh data dir so no session is restored.
export XDG_DATA_HOME=/tmp/pds-data XDG_CONFIG_HOME=/tmp/pds-config
rm -rf "$XDG_DATA_HOME" "$XDG_CONFIG_HOME"
mkdir -p "$XDG_DATA_HOME" "$XDG_CONFIG_HOME"

proton-drive-sync &
sleep 10
grim -t png docs/screenshots/login.png
```

> The Overview and other signed-in screens require the first remote sync to
> complete. On accounts with many files this can take minutes, so the README
> currently uses representative mockups for those pages.
