/**
 * ProtonDriveHTTPClient implementation: every SDK request flows through here.
 * Attaches Authorization/x-pm-uid/x-pm-appversion and performs a single-flight
 * 401 → refresh → retry. See docs/research/sdk-client.md §3.1.
 *
 * The session object is mutated in place on refresh so all holders of the
 * reference (account, persistence layer) see fresh tokens; callers must
 * persist after `onSessionRefreshed`.
 */
import { APP_VERSION } from "./config";
import { ApiError, refresh, type Session } from "./auth";

interface HttpRequest {
  url: string;
  method: string;
  headers: Headers;
  timeoutMs: number;
  signal?: AbortSignal;
  json?: object;
  body?: any;
  onProgress?: (progress: number) => void;
}

export class DriveHttpClient {
  private refreshPromise: Promise<void> | null = null;

  constructor(
    private readonly session: Session,
    private readonly onSessionRefreshed?: (s: Session) => void | Promise<void>,
  ) {}

  private authHeaders(extra?: Headers): Record<string, string> {
    const out: Record<string, string> = {
      "x-pm-appversion": APP_VERSION,
      "x-pm-uid": this.session.uid,
      authorization: `Bearer ${this.session.accessToken}`,
    };
    extra?.forEach((value, key) => {
      out[key] = value;
    });
    return out;
  }

  private async rawFetch(req: HttpRequest): Promise<Response> {
    const signal = AbortSignal.any(
      [AbortSignal.timeout(req.timeoutMs || 60_000), ...(req.signal ? [req.signal] : [])],
    );
    return fetch(req.url, {
      method: req.method,
      headers: this.authHeaders(req.headers),
      body: req.json !== undefined ? JSON.stringify(req.json) : req.body,
      signal,
    });
  }

  /** Single-flight refresh: a 401 stampede produces exactly one refresh call. */
  private async refreshOnce(): Promise<boolean> {
    this.refreshPromise ??= (async () => {
      try {
        const refreshed = await refresh(this.session);
        Object.assign(this.session, refreshed);
        await this.onSessionRefreshed?.(this.session);
      } finally {
        // Allow the next 401 to trigger a new refresh.
        setTimeout(() => (this.refreshPromise = null), 0);
      }
    })();
    try {
      await this.refreshPromise;
      return true;
    } catch (e) {
      // 400/401/422 from /refresh means the session is dead — surface it.
      if (e instanceof ApiError && [400, 401, 422].includes(e.status)) {
        throw new Error("Session expired — re-login required", { cause: e });
      }
      return false; // transient refresh failure: do not retry the original call now
    }
  }

  private async doFetch(req: HttpRequest): Promise<Response> {
    let res = await this.rawFetch(req);
    if (res.status === 401 && !req.url.includes("/auth/")) {
      if (await this.refreshOnce()) {
        res = await this.rawFetch(req);
      }
    }
    return res;
  }

  async fetchJson(req: HttpRequest): Promise<Response> {
    return this.doFetch(req);
  }

  async fetchBlob(req: HttpRequest): Promise<Response> {
    return this.doFetch(req);
  }
}
