import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createWolfpackTaskRelay } from "../src/wolfpack-task-relay";
import type { TaskRelay } from "../src/task-protocol";

const endpoint = { relay: "memory", id: "a" };

test("later automatic intent ACK cannot skip an assignment, including SQLite reopen", async () => {
  const root = mkdtempSync(join(tmpdir(), "tasks-checkpoint-"));
  const relay = createInMemoryTaskRelay("memory");
  const endpoints = ["a", "b", "c"].map(id => ({ relay: "memory", id }));
  const stores = endpoints.map(e => createTaskStore({ path: join(root, `${e.id}.sqlite`) }));
  const cores = endpoints.map((endpoint, i) => createTaskCore({ endpoint, relay, store: stores[i]! }));
  try {
    const [a, b, c] = cores;
    await Promise.all(cores.map(core => core.connect()));
    const parent = await a!.createTask({ target: endpoints[1]!, task: "a-to-b", timeoutMs: 60_000 });
    await b!.receive();
    const incoming = await c!.createTask({ target: endpoints[0]!, task: "c-to-a", timeoutMs: 60_000 });
    await b!.submitIntent({ taskId: parent.taskId, type: "task.information", payload: { message: "later intent" } });
    const visible = await a!.receive();
    const assignment = visible.find(d => d.envelope.taskId === incoming.taskId)!;
    expect(stores[0]!.getReceiveCursor()).toBe("0");
    stores[0]!.close(); stores[0] = createTaskStore({ path: join(root, "a.sqlite") });
    const resumed = createTaskCore({ endpoint: endpoints[0]!, relay, store: stores[0]! });
    const again = await resumed.receive();
    expect(again.some(d => d.envelope.envelopeId === assignment.envelope.envelopeId)).toBe(true);
    // Completing a later visible delivery first still cannot skip assignment.
    const later = again.filter(d => d.cursor !== assignment.cursor);
    for (const delivery of later) await resumed.acknowledgeRelayDelivery(delivery.cursor);
    expect(stores[0]!.getReceiveCursor()).toBe("0");
    await resumed.acknowledgeRelayDelivery(assignment.cursor);
    expect(BigInt(stores[0]!.getReceiveCursor())).toBeGreaterThanOrEqual(BigInt(assignment.cursor));
    expect(await resumed.receive()).toEqual([]);
  } finally { for (const store of stores) store.close(); rmSync(root, { recursive: true, force: true }); }
});

test("checkpoint preserves sparse bigint cursors, ignores ACKed rereads, and fences retired bindings", () => {
  const store = createTaskStore({ path: ":memory:" });
  const first = "9007199254740993", last = "99999999999999999999999999999999";
  try {
    store.setEndpointBinding(endpoint);
    store.transaction(() => store.trackRelayDeliveries(endpoint, [{ cursor: first, envelopeId: "first" }, { cursor: last, envelopeId: "last" }]));
    store.transaction(() => store.acknowledgeRelayCursor(endpoint, last));
    expect(store.getReceiveCursor()).toBe("0");
    store.transaction(() => store.trackRelayDeliveries(endpoint, [{ cursor: first, envelopeId: "first" }, { cursor: last, envelopeId: "last" }]));
    expect(store.pendingRelayEnvelopeId(endpoint, last)).toBeUndefined();
    expect(() => store.trackRelayDeliveries(endpoint, [{ cursor: first, envelopeId: "conflict" }])).toThrow("changed envelope identity");
    store.transaction(() => store.acknowledgeRelayCursor(endpoint, first));
    expect(store.getReceiveCursor()).toBe(last);
    store.setEndpointBinding({ ...endpoint, id: "new-lifetime" }); store.setReceiveCursor("0");
    expect(() => store.acknowledgeRelayCursor(endpoint, first)).toThrow("retired endpoint");
    expect(store.getReceiveCursor()).toBe("0");
  } finally { store.close(); }
});

test("ACK response loss keeps durable ID and checkpoint pending for retry after reopen", async () => {
  const root = mkdtempSync(join(tmpdir(), "tasks-ack-reopen-"));
  const path = join(root, "tasks.sqlite");
  let store = createTaskStore({ path });
  const attempts: unknown[] = [];
  let lose = true;
  const relay: TaskRelay = {
    id: "memory", async connect(input) { return { endpoint: input.endpoint, receiveCursor: input.receiveCursor }; },
    async resolve() { throw new Error("not used"); }, async send() { throw new Error("not used"); }, async receive() { throw new Error("not used"); },
    async acknowledgeDelivery(input) { attempts.push(input); if (lose) { lose = false; throw new Error("accepted ACK response lost"); } },
  };
  try {
    store.setEndpointBinding(endpoint);
    store.transaction(() => store.trackRelayDeliveries(endpoint, [{ cursor: "7", envelopeId: "immutable-id" }]));
    const core = createTaskCore({ endpoint, relay, store });
    await expect(core.acknowledgeRelayDelivery("7")).rejects.toThrow("response lost");
    expect(store.getReceiveCursor()).toBe("0");
    store.close(); store = createTaskStore({ path });
    await createTaskCore({ endpoint, relay, store }).connect(); // Automatically retries the persisted ACK intent.
    expect(attempts).toEqual([{ endpoint, cursor: "7", envelopeId: "immutable-id" }, { endpoint, cursor: "7", envelopeId: "immutable-id" }]);
    expect(store.getReceiveCursor()).toBe("7");
  } finally { store.close(); rmSync(root, { recursive: true, force: true }); }
});

test("completed checkpoint metadata does not retain per-ACK history", () => {
  const root = mkdtempSync(join(tmpdir(), "tasks-checkpoint-size-"));
  const path = join(root, "tasks.sqlite");
  const store = createTaskStore({ path });
  try {
    store.transaction(() => {
      for (let i = 1; i <= 2000; i++) {
        const cursor = String(i * 3);
        store.trackRelayDeliveries(endpoint, [{ cursor, envelopeId: `delivery-${i}` }]);
        store.requestRelayAcknowledgement(endpoint, cursor);
        store.acknowledgeRelayCursor(endpoint, cursor);
      }
    });
    const db = new Database(path, { readonly: true });
    try {
      const rows = db.query("SELECT value FROM relay_state WHERE name LIKE 'delivery_checkpoint:%'").all() as Array<{ value: string }>;
      expect(rows).toHaveLength(1);
      expect(JSON.parse(rows[0]!.value)).toEqual({ frontier: "6000", highWater: "6000", pending: {}, requested: {} });
      expect(rows[0]!.value.length).toBeLessThan(100);
    } finally { db.close(); }
  } finally { store.close(); rmSync(root, { recursive: true, force: true }); }
});

test("fresh Wolfpack adapter can use durable ACK binding without a receive-time RAM cache", async () => {
  const bound = { relay: "wolfpack-pi-tasks-v2", id: "bound" };
  const bodies: unknown[] = [];
  const requestFetch = Object.assign(async (_input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    if (body.generation) return Response.json({ ok: true, endpoint: bound, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() });
    bodies.push(body); return Response.json({ ok: true, kind: "duplicate" });
  }, { preconnect: fetch.preconnect }) as typeof fetch;
  const relay = createWolfpackTaskRelay({ baseUrl: "http://127.0.0.1:1", sessionName: "fixture", fetch: requestFetch });
  await relay.acknowledgeDelivery({ endpoint: bound, cursor: "9007199254740993", envelopeId: "saved" });
  expect(bodies).toEqual([{ callerSession: "fixture", envelopeId: "saved" }]);
  await expect(relay.acknowledgeDelivery({ endpoint: { ...bound, id: "retired" }, cursor: "1", envelopeId: "old" })).rejects.toThrow("different endpoint");
  expect(bodies).toHaveLength(1);
});
