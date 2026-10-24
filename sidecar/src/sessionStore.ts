/**
 * Session persistence (docs/protocol.md §Session persistence, M1 edition).
 *
 * `session.json` (mode 0600) in the platform data dir, written atomically
 * (temp file + fsync + rename). Loaded once at startup; re-persisted after
 * every token refresh (refresh tokens rotate — losing one strands the
 * session). Keychain storage replaces this in M2.
 *
 * Data dir: `$PDS_DATA_DIR` override (tests), else
 * `~/Library/Application Support/proton-drive-sync` (macOS) or
 * `$XDG_DATA_HOME/proton-drive-sync` / `~/.local/share/proton-drive-sync`
 * (Linux).
 *
 * RefreshToken note (verified against the SDK repo, 2026-10-23):
 * `SessionForkStatusResponse.RefreshToken` is **optional**
 * (`incubating/account/js/src/accountApi.ts`) — fork sessions are NOT
 * guaranteed to be refreshable. When absent we persist `refreshToken: ""`;
 * the first 401 then fails fast with session_expired (1002) instead of a
 * doomed /auth/v4/refresh call, and the host must re-run auth.loginFork.
 */
import { constants } from "node:fs";
import { mkdir, open, readFile, rename, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { Session } from "./auth";

const APP_DIR = "proton-drive-sync";
const SESSION_FILE = "session.json";
const CLIENT_FILE = "client.json";

export interface PersistedSession extends Session {
  /** Primary address email, resolved after login (fork response lacks it). */
  email?: string;
}

export function dataDir(): string {
  if (process.env.PDS_DATA_DIR) return process.env.PDS_DATA_DIR;
  if (process.platform === "darwin") {
    return join(homedir(), "Library", "Application Support", APP_DIR);
  }
  return join(process.env.XDG_DATA_HOME ?? join(homedir(), ".local", "share"), APP_DIR);
}

async function writeAtomic0600(path: string, contents: string): Promise<void> {
  const tmp = `${path}.tmp-${process.pid}`;
  const fh = await open(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC, 0o600);
  try {
    await fh.writeFile(contents, "utf8");
    await fh.sync();
  } finally {
    await fh.close();
  }
  await rename(tmp, path);
}

export class SessionStore {
  readonly dir = dataDir();
  private readonly sessionPath = join(this.dir, SESSION_FILE);
  private readonly clientPath = join(this.dir, CLIENT_FILE);

  async init(): Promise<void> {
    await mkdir(this.dir, { recursive: true, mode: 0o700 });
  }

  /** Returns undefined when no (readable) session exists. */
  async load(): Promise<PersistedSession | undefined> {
    try {
      const raw = await readFile(this.sessionPath, "utf8");
      const s = JSON.parse(raw) as PersistedSession;
      if (!s.uid || !s.accessToken) return undefined;
      return s;
    } catch {
      return undefined;
    }
  }

  async save(session: PersistedSession): Promise<void> {
    await writeAtomic0600(this.sessionPath, JSON.stringify(session, null, 2));
  }

  async clear(): Promise<void> {
    await rm(this.sessionPath, { force: true });
  }

  /**
   * The SDK clientUid identifies which client owns an upload draft. It must
   * be stable across restarts AND across logout/login, so it lives in its
   * own file, never wiped by auth.logout.
   */
  async getOrCreateClientUid(): Promise<string> {
    try {
      const raw = await readFile(this.clientPath, "utf8");
      const parsed = JSON.parse(raw);
      if (typeof parsed.clientUid === "string" && parsed.clientUid.length > 0) {
        return parsed.clientUid;
      }
    } catch {
      /* fall through to generate */
    }
    const clientUid = `pds-${crypto.randomUUID()}`;
    await writeAtomic0600(this.clientPath, JSON.stringify({ clientUid }));
    return clientUid;
  }
}
