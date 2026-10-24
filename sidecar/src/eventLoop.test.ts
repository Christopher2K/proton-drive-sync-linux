/**
 * Focused tests for the event polling loop (bug: iterateEvents returns
 * after the initial drain, so a single call silently dropped the scope).
 * Runs entirely on fakes — no network, no Proton account.
 *
 * Run: npm test   (npx tsx src/eventLoop.test.ts)
 */
import type { DriveEvent } from "@protontech/drive-sdk";
import { runEventLoop } from "./eventLoop";
import { SidecarService } from "./service";

let failures = 0;
function check(name: string, cond: boolean, actual: unknown): void {
  console.log(`${cond ? "PASS" : "FAIL"}  ${name}`);
  if (!cond) console.log(`      ${JSON.stringify(actual)}`);
  if (!cond) failures++;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function nodeEvent(eventId: string): DriveEvent {
  return {
    type: "node_updated",
    nodeUid: "n1",
    parentNodeUid: "p1",
    isTrashed: false,
    isShared: false,
    treeEventScopeId: "scope",
    eventId,
  } as DriveEvent;
}

/** Fake iterate: records calls, yields the scripted events per call. */
function fakeIterate(script: DriveEvent[][] | ((call: number) => DriveEvent[])) {
  const calls: { cursor: string | undefined; signal: AbortSignal }[] = [];
  const iterate = (cursor: string | undefined, signal: AbortSignal): AsyncIterable<DriveEvent> => {
    calls.push({ cursor, signal });
    const events = typeof script === "function" ? script(calls.length) : (script[calls.length - 1] ?? []);
    return (async function* () {
      for (const e of events) yield e;
    })();
  };
  return { calls, iterate };
}

async function main(): Promise<void> {
  // 1. After the drain the loop re-polls with the latest eventId as cursor.
  {
    const ac = new AbortController();
    const forwarded: string[] = [];
    const { calls, iterate } = fakeIterate([[nodeEvent("e1"), nodeEvent("e2")]]);
    const done = runEventLoop({
      iterate,
      forward: (e) => forwarded.push(e.eventId),
      signal: ac.signal,
      pollIntervalMs: 10,
      jitterMs: 0,
    });
    await sleep(60); // ~5 poll cycles
    ac.abort();
    await done;
    check("re-polls after drain", calls.length >= 3, calls.length);
    check(
      "re-poll uses latest eventId as cursor",
      calls.slice(1).every((c) => c.cursor === "e2"),
      calls.map((c) => c.cursor),
    );
    check("both events forwarded", forwarded.join(",") === "e1,e2", forwarded);
  }

  // 2. fast_forward eventId is adopted as the cursor.
  {
    const ac = new AbortController();
    const { calls, iterate } = fakeIterate([[{ type: "fast_forward", treeEventScopeId: "scope", eventId: "ff1" } as DriveEvent]]);
    const done = runEventLoop({
      iterate,
      forward: () => {},
      signal: ac.signal,
      pollIntervalMs: 10,
      jitterMs: 0,
    });
    await sleep(35);
    ac.abort();
    await done;
    check("fast_forward adopted as cursor", calls.length >= 2 && calls[1].cursor === "ff1", calls.map((c) => c.cursor));
  }

  // 3. Stream error → exponential backoff, scope stays alive, loop recovers.
  {
    const ac = new AbortController();
    const errors: unknown[] = [];
    const forwarded: string[] = [];
    const { calls, iterate } = fakeIterate((call) => {
      if (call <= 2) throw new Error(`boom ${call}`);
      return [nodeEvent("ok")];
    });
    const t0 = Date.now();
    const done = runEventLoop({
      iterate,
      forward: (e) => forwarded.push(e.eventId),
      signal: ac.signal,
      onError: (e) => errors.push(e),
      pollIntervalMs: 10,
      jitterMs: 0,
      backoffMinMs: 20,
      backoffMaxMs: 40,
    });
    await sleep(200);
    ac.abort();
    await done;
    check("errors reported, loop kept scope alive", errors.length >= 2 && calls.length >= 3, { errors: errors.length, calls: calls.length });
    check("loop recovered after errors", forwarded.includes("ok"), forwarded);
    // First retry waits >= backoffMin (20ms) instead of the 10ms poll interval.
    check("backoff delays retry", Date.now() - t0 >= 20, Date.now() - t0);
  }

  // 4. Abort during the poll sleep exits promptly.
  {
    const ac = new AbortController();
    const { iterate } = fakeIterate([[]]);
    const t0 = Date.now();
    const done = runEventLoop({
      iterate,
      forward: () => {},
      signal: ac.signal,
      pollIntervalMs: 60_000,
      jitterMs: 0,
    });
    await sleep(20); // first drain done, loop is now sleeping
    ac.abort();
    await done;
    check("abort during sleep exits promptly", Date.now() - t0 < 1_000, Date.now() - t0);
  }

  // 5. tree_remove (eventId 'none') is forwarded and ends the loop.
  {
    const ac = new AbortController();
    const forwarded: DriveEvent[] = [];
    const { calls, iterate } = fakeIterate([[{ type: "tree_remove", treeEventScopeId: "scope", eventId: "none" } as DriveEvent]]);
    await runEventLoop({
      iterate,
      forward: (e) => forwarded.push(e),
      signal: ac.signal,
      pollIntervalMs: 10,
      jitterMs: 0,
    });
    await sleep(30);
    check("tree_remove forwarded", forwarded.length === 1 && forwarded[0].type === "tree_remove", forwarded);
    check("tree_remove ends polling", calls.length === 1, calls.length);
  }

  // 6. Service level: events.start twice on the same scope replaces the
  //    first loop; events.stop aborts and is idempotent. (Fake client — no
  //    network.)
  {
    const notifications: { method: string; params: any }[] = [];
    const rpc = { method: () => {}, notify: (m: string, p: unknown) => notifications.push({ method: m, params: p }) };
    const svc = new SidecarService(rpc as any, {} as any);
    const { calls, iterate } = fakeIterate([[nodeEvent("n1")]]);
    (svc as any).session = { uid: "fake" };
    (svc as any).client = {
      iterateEvents: (_scope: string, cursor: string | undefined, signal: AbortSignal) => iterate(cursor, signal),
    };

    (svc as any).eventsStart({ scopeId: "s1" });
    await sleep(30); // first loop drains, then sleeps 30s (default cadence)
    check("service forwarded drained event", notifications.some((n) => n.method === "drive.event" && n.params.eventId === "n1"), notifications);

    (svc as any).eventsStart({ scopeId: "s1" }); // restart replaces
    await sleep(30);
    check("restart aborts previous loop", calls[0].signal.aborted === true, calls[0].signal.aborted);
    check("restart starts a fresh drain", calls.length === 2 && calls[1].signal.aborted === false, calls.map((c) => c.signal.aborted));

    (svc as any).eventsStop({ scopeId: "s1" });
    await sleep(10);
    check("events.stop aborts the loop", calls[1].signal.aborted === true, calls[1].signal.aborted);
    (svc as any).eventsStop({ scopeId: "s1" }); // idempotent no-op — must not throw
    check("events.stop on unknown scope is a no-op", true, null);
    check("scope registry emptied", (svc as any).eventScopes.size === 0, (svc as any).eventScopes.size);
  }

  if (failures > 0) {
    console.error(`\n${failures} check(s) FAILED`);
    process.exit(1);
  }
  console.log("\nevent loop tests PASSED");
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
