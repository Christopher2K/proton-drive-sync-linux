# proton-drive-sync

Cross-platform (macOS + Linux) Proton Drive sync client with on-demand placeholder files.

- **Product spec:** [SPEC.md](SPEC.md) — source of truth
- **Agent notes:** [AGENT.md](AGENT.md)

Stack: Rust + GPUI host app · Node.js sidecar running the official [Proton Drive SDK](https://github.com/ProtonDriveApps/sdk) · FUSE (Linux) / File Provider (macOS).

> This is a third-party application not officially supported by Proton.
