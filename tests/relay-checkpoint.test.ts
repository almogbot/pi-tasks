import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import type { TaskRelay } from "../src/task-protocol";

const endpoint = { relay: "memory", id: "a" };

test("later automatic intent ACK cannot skip an assignment across same-lifetime core recreation", async () => {
  const root = mkdtempSync(join(tmpdir(), "tasks-checkpoint-"));
  const relay = createInMemoryTaskRelay("memory");
  const endpoints = ["a", "b", "c"].map(id => ({ relay: "memory", id }));
  const stores = endpoints.map(() => createTaskStore());
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
  const store = createTaskStore();
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

test("ACK response loss keeps identity pending for retry in the same endpoint lifetime", async () => {
  const root = mkdtempSync(join(tmpdir(), "tasks-ack-reopen-"));
  const path = join(root, "tasks.sqlite");
  const store = createTaskStore();
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
    await createTaskCore({ endpoint, relay, store }).connect(); // Retry the same RAM-owned ACK intent.
    expect(attempts).toEqual([{ endpoint, cursor: "7", envelopeId: "immutable-id" }, { endpoint, cursor: "7", envelopeId: "immutable-id" }]);
    expect(store.getReceiveCursor()).toBe("7");
  } finally { store.close(); rmSync(root, { recursive: true, force: true }); }
});

test("completed checkpoint metadata stays within two bounded RAM records over many ACKs", () => {
  const store = createTaskStore({ maxEntries: 2, maxBytes: 512 });
  try {
    store.transaction(() => {
      for (let i = 1; i <= 2000; i++) {
        const cursor = String(i * 3);
        store.trackRelayDeliveries(endpoint, [{ cursor, envelopeId: `delivery-${i}` }]);
        store.requestRelayAcknowledgement(endpoint, cursor);
        store.acknowledgeRelayCursor(endpoint, cursor);
      }
    });
    expect(store.getReceiveCursor()).toBe("6000");
    expect(store.requestedRelayAcknowledgements(endpoint)).toEqual([]);
    expect(store.pendingRelayEnvelopeId(endpoint, "6000")).toBeUndefined();
  } finally { store.close(); }
});
