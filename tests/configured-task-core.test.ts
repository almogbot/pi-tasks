import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createConfiguredTaskCore } from "../src/configured-task-core";
import { createConfiguredCoreLoader } from "../src/extension";
import { createTaskStore } from "../src/task-store";
import type { TaskCore } from "../src/task-core";

const profile = "volatile-v1", epoch = "00000000-0000-4000-8000-000000000001";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };
function fixture(handler?: (body: any, init: RequestInit) => Promise<Response> | Response) {
  const root = mkdtempSync(join(tmpdir(), "tasks-configured-"));
  const path = join(root, "tasks.sqlite"), calls: any[] = [];
  const fetcher = Object.assign(async (url: unknown, init?: RequestInit) => {
    expect(String(url)).toBe("http://127.0.0.1:1/api/task-relay/volatile-v1");
    const body = JSON.parse(String(init?.body)); calls.push(body);
    if (handler) return handler(body, init!);
    return Response.json({ ok: true, profile, epoch, value: body.operation === "connect"
      ? { kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() }
      : { kind: "page", deliveries: [], nextCursor: "0", hasMore: false } });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  return { path, calls, options: { path, sessionName: "fixture", baseUrl: "http://127.0.0.1:1", fetch: fetcher }, cleanup: () => rmSync(root, { recursive: true, force: true }) };
}

test("configured normal transport negotiates volatile, closes SQLite and resumes the same lifetime without replay", async () => {
  const f = fixture();
  try {
    const first = await createConfiguredTaskCore(f.options);
    await first.receive(); await first.close(); await first.close();
    expect(() => first.listTasks()).toThrow("task session is closed");
    const store = createTaskStore({ path: f.path });
    const binding = store.getRelayTransportBinding(); store.close();
    expect(binding).toMatchObject({ profile, epoch, endpoint });
    const second = await createConfiguredTaskCore(f.options);
    expect(second.endpoint).toEqual(first.endpoint); await second.close();
    expect(f.calls.filter(c => c.operation === "connect")).toHaveLength(2);
    expect(f.calls.at(-1)).toMatchObject({ epoch, generation: binding!.generation });
  } finally { f.cleanup(); }
});

test("legacy history requires explicit rebind, quarantines old pending work and never calls durable routes", async () => {
  const f = fixture();
  try {
    const store = createTaskStore({ path: f.path });
    const prior = { ...endpoint, id: "old-endpoint" };
    store.setEndpointBinding(prior);
    store.putOutbox({ envelopeId: "old-pending", taskId: "old-task", source: prior, target: endpoint, kind: "assignment", protocolVersion: "pi-tasks/v2", payload: "{}", createdAt: new Date().toISOString() });
    store.close();
    await expect(createConfiguredTaskCore(f.options)).rejects.toMatchObject({ code: "RELAY_REBIND_REQUIRED" });
    expect(f.calls).toEqual([]);
    const rebound = await createConfiguredTaskCore({ ...f.options, rebind: true }); await rebound.close();
    const reopened = createTaskStore({ path: f.path });
    expect(reopened.outbox("pending")).toEqual([]);
    expect(reopened.quarantinedOutbox()).toHaveLength(1);
    expect(reopened.quarantinedOutbox()[0]!.envelope.source).toEqual(prior);
    expect(reopened.getRelayTransportBinding()).toMatchObject({ profile, epoch }); reopened.close();
  } finally { f.cleanup(); }
});

test("profile refusal does not trigger downgrade or bind an unbound store", async () => {
  const f = fixture(() => Response.json({ ok: false, profile: "durable-v2", error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }));
  try {
    await expect(createConfiguredTaskCore(f.options)).rejects.toMatchObject({ code: "RELAY_PROFILE_REQUIRED" });
    expect(f.calls).toHaveLength(1);
    const store = createTaskStore({ path: f.path }); expect(store.getRelayTransportBinding()).toBeUndefined(); store.close();
  } finally { f.cleanup(); }
});

test("owned close aborts pending receive before closing SQLite and fences new work", async () => {
  let started!: () => void;
  const receiving = new Promise<void>(resolve => { started = resolve; });
  const f = fixture(body => {
    if (body.operation === "connect") return Response.json({ ok: true, profile, epoch, value: { kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() } });
    started(); return new Promise<Response>(() => {}); // Even a noncooperative fetch must be fenced.
  });
  try {
    const core = await createConfiguredTaskCore(f.options);
    const result = core.receive().then(() => "unexpected", error => error.code);
    await receiving; await core.close();
    expect(await result).toBe("RELAY_RESET");
    expect(() => core.receive()).toThrow("task session is closed");
    const reopened = createTaskStore({ path: f.path }); expect(reopened.getReceiveCursor()).toBe("0"); reopened.close();
  } finally { f.cleanup(); }
});

test("loader cancels setup and disposes a late core without resurrecting it", async () => {
  let release!: (core: TaskCore) => void, signal: AbortSignal | undefined, closed = 0;
  const loader = createConfiguredCoreLoader(async s => { signal = s; return new Promise<TaskCore>(resolve => { release = resolve; }); });
  const pending = loader().then(() => "unexpected", e => e.code);
  await Promise.resolve();
  const closing = loader.close(); expect(signal!.aborted).toBe(true);
  release({ close: async () => { closed++; } } as unknown as TaskCore);
  await closing; expect(await pending).toBe("RELAY_CLOSED"); expect(closed).toBe(1);
  await expect(loader()).rejects.toMatchObject({ code: "RELAY_CLOSED" });
});

test("loader closes cached ownership once per lifecycle and starts a fresh instance", async () => {
  let opens = 0, closes = 0;
  const loader = createConfiguredCoreLoader(async () => ({ instance: ++opens, close: async () => { closes++; } }) as unknown as TaskCore);
  const first = await loader(); expect(await loader()).toBe(first);
  await loader.close(); await loader.close(); expect(closes).toBe(1);
  await loader.start(); expect(await loader()).not.toBe(first); expect(opens).toBe(2);
  await loader.close(); expect(closes).toBe(2);
});

test("loader startup waits for SQLite ownership disposal and a new shutdown fences that startup", async () => {
  let release!: () => void;
  const loader = createConfiguredCoreLoader(async () => ({ close: () => new Promise<void>(resolve => { release = resolve; }) }) as unknown as TaskCore);
  await loader();
  const closing = loader.close();
  await Promise.resolve();
  let started = false;
  const starting = loader.start().then(() => { started = true; return "unexpected"; }, error => error.code);
  await Promise.resolve(); expect(started).toBe(false);
  const stoppedAgain = loader.close(); release();
  await Promise.all([closing, stoppedAgain]);
  expect(await starting).toBe("RELAY_CLOSED");
  await expect(loader()).rejects.toMatchObject({ code: "RELAY_CLOSED" });
});
