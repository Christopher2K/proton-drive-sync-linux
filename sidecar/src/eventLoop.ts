/**
 * Polling loop around the SDK's pull-based event generator.
 *
 * `client.iterateEvents(scope, cursor)` drains the events currently
 * available and RETURNS (docs/research/sdk-client.md §4.5) — calling it
 * once drops the scope after the initial drain and no live remote event
 * ever arrives. This loop re-invokes it after each drain with the latest
 * forwarded eventId as the new cursor:
 *
 * - Cadence matches the SDK EventScheduler's own-volume interval: 30 s
 *   plus a small jitter (event-endpoint polling is the sanctioned model).
 * - Stream errors back off exponentially (5 s → 5 min cap) and keep the
 *   scope alive.
 * - fast_forward events adopt their eventId as cursor; every forwarded
 *   event advances the cursor.
 * - tree_remove means the scope is gone (its eventId is the literal
 *   'none', not a cursor): forward it and stop polling.
 * - The AbortSignal is honored during sleeps (stop / restart-replaces).
 */
import type { DriveEvent } from "@protontech/drive-sdk";

export const EVENT_POLL_INTERVAL_MS = 30_000;
export const EVENT_POLL_JITTER_MS = 1_000;
export const EVENT_BACKOFF_MIN_MS = 5_000;
export const EVENT_BACKOFF_MAX_MS = 5 * 60_000;

export interface EventLoopOptions {
  /** Invoke the SDK generator for one drain; must return after it. */
  iterate: (cursor: string | undefined, signal: AbortSignal) => AsyncIterable<DriveEvent>;
  /** Forward one event to the host. */
  forward: (event: DriveEvent) => void;
  /** Abort to stop the loop (events.stop, scope replacement, shutdown). */
  signal: AbortSignal;
  /** Resume token from the host; omit to fast-forward to latest. */
  initialCursor?: string;
  /** Called on stream errors (loop keeps running). */
  onError?: (error: unknown) => void;
  /** Test seams — defaults match the SDK EventScheduler. */
  pollIntervalMs?: number;
  jitterMs?: number;
  backoffMinMs?: number;
  backoffMaxMs?: number;
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException("Sleep aborted", "AbortError"));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(new DOMException("Sleep aborted", "AbortError"));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

export async function runEventLoop(opts: EventLoopOptions): Promise<void> {
  const poll = opts.pollIntervalMs ?? EVENT_POLL_INTERVAL_MS;
  const jitter = opts.jitterMs ?? EVENT_POLL_JITTER_MS;
  const backoffMin = opts.backoffMinMs ?? EVENT_BACKOFF_MIN_MS;
  const backoffMax = opts.backoffMaxMs ?? EVENT_BACKOFF_MAX_MS;
  let cursor = opts.initialCursor;
  let backoff = backoffMin;
  while (!opts.signal.aborted) {
    try {
      for await (const event of opts.iterate(cursor, opts.signal)) {
        if (event.type === "tree_remove") {
          opts.forward(event);
          return;
        }
        opts.forward(event);
        cursor = event.eventId;
      }
      backoff = backoffMin; // clean drain — reset error backoff
      await abortableSleep(poll + Math.random() * jitter, opts.signal);
    } catch (e) {
      // Abort (during drain or poll sleep) exits quietly.
      if (opts.signal.aborted) return;
      opts.onError?.(e);
      try {
        await abortableSleep(backoff, opts.signal);
      } catch {
        return; // aborted mid-backoff
      }
      backoff = Math.min(backoff * 2, backoffMax);
    }
  }
}
