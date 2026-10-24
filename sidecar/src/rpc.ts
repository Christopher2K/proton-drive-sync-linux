/**
 * Minimal JSON-RPC 2.0 server over newline-delimited stdio (docs/protocol.md).
 *
 * - One message per line, UTF-8. Requests are dispatched concurrently
 *   (handlers are async; `auth.loginFork` intentionally stays open for
 *   minutes while the user signs in).
 * - stdout is protocol-only; anything human-readable goes to stderr via log().
 * - Error codes: JSON-RPC standard (-32700/-32600/-32601/-32602) for protocol
 *   problems; 1001..1099 (see docs/protocol.md) for domain errors raised by
 *   handlers. Handler exceptions that don't map to a domain code become 1099
 *   "internal" rather than -32603, so the host can rely on the documented
 *   code range for all method-level failures.
 */

export const RPC = {
  parseError: -32700,
  invalidRequest: -32600,
  methodNotFound: -32601,
  invalidParams: -32602,
  authRequired: 1001,
  sessionExpired: 1002,
  rateLimited: 1003,
  integrityError: 1004,
  conflict: 1005,
  internal: 1099,
} as const;

export class RpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
    readonly data?: unknown,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

export type Handler = (params: any) => unknown | Promise<unknown>;

interface JsonRpcRequest {
  jsonrpc: string;
  id?: number | string | null;
  method?: unknown;
  params?: unknown;
}

export class RpcServer {
  private handlers = new Map<string, Handler>();
  private buffer = "";

  constructor(
    private readonly input: NodeJS.ReadableStream = process.stdin,
    private readonly output: NodeJS.WritableStream = process.stdout,
  ) {}

  method(name: string, handler: Handler): void {
    this.handlers.set(name, handler);
  }

  /** Server→client notification (no id). */
  notify(method: string, params: unknown): void {
    this.write({ jsonrpc: "2.0", method, params });
  }

  /** Run the read loop until stdin closes. */
  run(): void {
    this.input.setEncoding("utf8");
    this.input.on("data", (chunk: string) => {
      this.buffer += chunk;
      for (;;) {
        const nl = this.buffer.indexOf("\n");
        if (nl < 0) break;
        const line = this.buffer.slice(0, nl);
        this.buffer = this.buffer.slice(nl + 1);
        if (line.trim().length > 0) void this.handleLine(line);
      }
    });
    this.input.on("end", () => log("stdin closed — shutting down"));
  }

  private async handleLine(line: string): Promise<void> {
    let msg: JsonRpcRequest;
    try {
      msg = JSON.parse(line);
    } catch {
      this.writeError(null, RPC.parseError, "Parse error");
      return;
    }
    if (msg.jsonrpc !== "2.0" || typeof msg.method !== "string") {
      this.writeError(msg.id ?? null, RPC.invalidRequest, "Invalid JSON-RPC request");
      return;
    }
    if (msg.id === undefined || msg.id === null) {
      // Client→server notification. The protocol defines none; ignore.
      log(`ignoring notification ${msg.method}`);
      return;
    }
    const handler = this.handlers.get(msg.method);
    if (!handler) {
      this.writeError(msg.id, RPC.methodNotFound, `Unknown method ${msg.method}`);
      return;
    }
    try {
      const result = (await handler(msg.params)) ?? {};
      this.write({ jsonrpc: "2.0", id: msg.id, result });
    } catch (e) {
      const err = toRpcError(e);
      this.writeError(msg.id, err.code, err.message, err.data);
    }
  }

  private write(msg: unknown): void {
    this.output.write(JSON.stringify(msg) + "\n");
  }

  private writeError(id: number | string | null, code: number, message: string, data?: unknown) {
    this.write({ jsonrpc: "2.0", id, error: { code, message, ...(data !== undefined ? { data } : {}) } });
  }
}

/** Maps any thrown error to a protocol error code (docs/protocol.md §errors). */
export function toRpcError(e: unknown): { code: number; message: string; data?: unknown } {
  if (e instanceof RpcError) return { code: e.code, message: e.message, data: e.data };
  const name = (e as any)?.name ?? "";
  // SDK error classes (@protontech/drive-sdk) — matched by name to keep this
  // module free of SDK imports.
  if (name === "RateLimitedError") return { code: RPC.rateLimited, message: msg(e) };
  if (name === "IntegrityError" || name === "DecryptionError") {
    return { code: RPC.integrityError, message: msg(e) };
  }
  if (name === "NodeWithSameNameExistsValidationError") {
    return {
      code: RPC.conflict,
      message: msg(e),
      data: { existingNodeUid: (e as any).existingNodeUid },
    };
  }
  // Our own auth/HTTP layer (src/auth.ts ApiError, src/httpClient.ts).
  if (name === "SessionExpiredError") return { code: RPC.sessionExpired, message: msg(e) };
  if (name === "ApiError") {
    const status = (e as any).status as number;
    if (status === 429) {
      const retryAfter = Number((e as any).retryAfter) || undefined;
      return { code: RPC.rateLimited, message: msg(e), data: { retryAfter } };
    }
    if (status === 400 || status === 401) return { code: RPC.sessionExpired, message: msg(e) };
    return { code: RPC.internal, message: msg(e), data: { status, apiCode: (e as any).code } };
  }
  return { code: RPC.internal, message: msg(e) };
}

/** retryable flag for transfer.error notifications. */
export function isRetryable(e: unknown): boolean {
  const name = (e as any)?.name ?? "";
  if (name === "RateLimitedError" || name === "ConnectionError") return true;
  if (name === "ApiError") {
    const status = (e as any).status as number;
    return status === 429 || status >= 500;
  }
  return false;
}

function msg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function log(...args: unknown[]): void {
  console.error("[sidecar]", ...args);
}
