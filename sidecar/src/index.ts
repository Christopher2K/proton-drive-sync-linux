/**
 * proton-drive-sync sidecar entry point.
 *
 * Wraps the official Proton Drive SDK (ProtonDriveApps/sdk) and exposes a
 * JSON-RPC interface over stdio to the Rust host app. All Proton Drive API
 * traffic must go through this process (see SPEC.md §3).
 *
 * M0 spike: see src/spike.ts. The persistent JSON-RPC loop lands after the
 * spike validates auth + SDK operations.
 */

const APP_VERSION = "external-drive-proton_drive_sync@0.1.0-alpha";

export function appVersion(): string {
  return APP_VERSION;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  console.error("sidecar: JSON-RPC loop not implemented yet — run `npm run spike` for the M0 spike");
  process.exit(1);
}
