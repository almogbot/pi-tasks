import { expect, test } from "bun:test";
import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { createWolfpackTaskRelay } from "../src/wolfpack-task-relay";
import { INVALID_RELAY_METADATA, TASK_PROTOCOL_VERSION } from "../src/task-protocol";
import type { RelayEnvelope } from "../src/task-protocol";

const origin = { relay: "memory", id: "origin" }, target = { relay: "memory", id: "target" };
const iso = (now: number) => new Date(now).toISOString();
const forbiddenFetch = (called: () => void): typeof fetch => Object.assign(async () => {
	called(); throw new Error("unexpected network");
}, { preconnect() { throw new Error("unexpected preconnect"); } });

test("assignment, intent and canonical envelopes persist their creation time without changing task event authority", async () => {
	const relay = createInMemoryTaskRelay("memory"), originStore = createTaskStore({ path: ":memory:" }), targetStore = createTaskStore({ path: ":memory:" });
	let now = 1000;
	const clock = { now: () => now };
	const a = createTaskCore({ endpoint: origin, relay, store: originStore, clock });
	const b = createTaskCore({ endpoint: target, relay, store: targetStore, clock });
	try {
		await a.connect(); await b.connect();
		const { taskId } = await a.createTask({ target, task: "metadata", timeoutMs: 60_000 });
		const assignment = originStore.outbox("accepted").find(row => row.envelope.kind === "assignment")!.envelope;
		expect(assignment.createdAt).toBe(iso(1000));
		now = 2000;
		await b.receive();
		await b.submitIntent({ taskId, type: "task.information", payload: { message: "progress" } });
		const intents = targetStore.outbox("accepted");
		expect(intents.length).toBeGreaterThan(0);
		expect(intents.every(row => row.envelope.kind === "intent" && row.envelope.createdAt === iso(2000))).toBe(true);
		now = 3000;
		await a.receive();
		const canonical = originStore.outbox("accepted").filter(row => row.envelope.kind === "canonical_event");
		expect(canonical.length).toBeGreaterThan(0);
		expect(canonical.every(row => row.envelope.createdAt === iso(3000))).toBe(true);
		expect(originStore.outbox("accepted").find(row => row.envelope.envelopeId === assignment.envelopeId)!.envelope.createdAt).toBe(iso(1000));
		expect(a.getTask(taskId)?.status).toBe("active");
		expect(a.getTask(taskId)?.events[0]?.occurredAt).toBe(1000);
	} finally { originStore.close(); targetStore.close(); }
});

test("missing and invalid legacy transport metadata is rejected before any relay request", async () => {
	let requests = 0;
	const relay = createWolfpackTaskRelay({ sessionName: "private-test", baseUrl: "https://must-not-contact.invalid", fetch: forbiddenFetch(() => { requests++; }) });
	const envelope: RelayEnvelope = { envelopeId: "old", protocolVersion: TASK_PROTOCOL_VERSION, source: origin, target, taskId: "task", kind: "assignment", payload: "{}" };
	for (const createdAt of [undefined, "invalid", "2026-09-08", "2026-09-08T00:00:00+00:00"]) {
		await expect(relay.send({ ...envelope, createdAt })).rejects.toMatchObject({ code: INVALID_RELAY_METADATA, retryable: false });
	}
	expect(requests).toBe(0);
});

test("legacy pending outbox is quarantined unchanged rather than stamped and blindly retried", async () => {
	let requests = 0;
	const relay = createWolfpackTaskRelay({ sessionName: "private-test", baseUrl: "https://must-not-contact.invalid", fetch: forbiddenFetch(() => { requests++; }) });
	const store = createTaskStore({ path: ":memory:" });
	const envelope: RelayEnvelope = { envelopeId: "legacy", protocolVersion: TASK_PROTOCOL_VERSION, source: origin, target, taskId: "task", kind: "assignment", payload: "{}" };
	store.putOutbox(envelope);
	const alreadyAccepted = { ...envelope, envelopeId: "already-accepted" };
	store.putOutbox(alreadyAccepted); store.markOutboxAccepted(alreadyAccepted.envelopeId);
	const core = createTaskCore({ endpoint: origin, relay, store });
	try {
		await expect(core.flushOutbox()).rejects.toMatchObject({ code: INVALID_RELAY_METADATA, retryable: false });
		expect(store.outbox("pending")).toEqual([]);
		expect(store.outbox("accepted").map(row => row.envelope)).toEqual([alreadyAccepted]);
		expect(store.quarantinedOutbox()).toEqual([expect.objectContaining({ envelope, errorCode: INVALID_RELAY_METADATA })]);
		await core.flushOutbox();
		expect(requests).toBe(0);
		expect(store.quarantinedOutbox()[0]!.envelope).toEqual(envelope);
	} finally { store.close(); }
});
