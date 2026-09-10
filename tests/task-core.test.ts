import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";

import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { TaskProtocolError } from "../src/task-protocol";
import type { TaskEndpoint, TaskRelay } from "../src/task-protocol";

const ORIGIN: TaskEndpoint = { relay: "memory", id: "origin" };
const RECEIVER: TaskEndpoint = { relay: "memory", id: "receiver" };

interface Fixture {
	readonly relay: ReturnType<typeof createInMemoryTaskRelay>;
	readonly origin: ReturnType<typeof createTaskCore>;
	readonly receiver: ReturnType<typeof createTaskCore>;
	readonly originStore: ReturnType<typeof createTaskStore>;
	readonly receiverStore: ReturnType<typeof createTaskStore>;
	advance(now: number): void;
}

function fixture(now = 1_000): Fixture {
	const relay = createInMemoryTaskRelay("memory");
	const originStore = createTaskStore();
	const receiverStore = createTaskStore();
	const current = { value: now };
	const clock = { now: (): number => current.value };
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: originStore, clock, ids: sequenceIds("origin") });
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, clock, ids: sequenceIds("receiver") });
	return { relay, origin, receiver, originStore, receiverStore, advance(value) { current.value = value; } };
}

function sequenceIds(prefix: string): () => string {
	let next = 0;
	return (): string => `${prefix}-${++next}`;
}

function blockOriginDelivery(backing: ReturnType<typeof createInMemoryTaskRelay>, blocked: { readonly value: boolean }): TaskRelay {
	return {
		id: backing.id,
		connect: (input) => backing.connect(input),
		resolve: (input) => backing.resolve(input),
		send: async (input) => {
			if (blocked.value && input.target.id === ORIGIN.id) {
				throw new TaskProtocolError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false, details: { targetId: input.target.id } });
			}
			return backing.send(input);
		},
		receive: (input) => backing.receive(input),
		acknowledgeDelivery: (input) => backing.acknowledgeDelivery(input),
	};
}

afterEach(() => undefined);

describe("endpoint-owned task core", () => {
	test("persists an origin task and assignment outbox before relay acceptance", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		value.relay.failNextSend();

		await expect(value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 })).rejects.toThrow("in-memory relay send failed");

		const task = value.origin.getTask("origin-1");
		expect(task?.status).toBe("active");
		expect(value.originStore.outbox("pending")).toHaveLength(1);
		expect(value.relay.envelopesFor(RECEIVER)).toHaveLength(0);

		await value.origin.flushOutbox();
		expect(value.originStore.outbox("pending")).toHaveLength(0);
		expect(value.relay.envelopesFor(RECEIVER)).toHaveLength(1);
	});

	test("origin canonically sequences receiver intents and rejects a conflicting receiver terminal retry", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await value.receiver.receive();

		await value.receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
		const receiverOutboxIds = value.receiverStore.outbox("accepted").map((record) => record.envelope.envelopeId);
		await expect(value.receiver.submitIntent({ taskId: created.taskId, type: "task.failed", payload: { summary: "late" } })).rejects.toMatchObject({ code: "TERMINAL_INTENT_CONFLICT", retryable: false });
		await value.origin.receive();

		const task = value.origin.getTask(created.taskId);
		expect(task?.status).toBe("completed");
		expect(task?.events.map((event) => event.type)).toEqual(["task.created", "task.completed"]);
		expect(value.receiverStore.outbox("accepted").map((record) => record.envelope.envelopeId)).toEqual(receiverOutboxIds);
		expect(task?.events.map((event) => event.sequence)).toEqual(["1", "2"]);

		await value.origin.receive();
		expect(value.origin.getTask(created.taskId)?.events).toHaveLength(2);
	});

	test("returns the structured canonical outcome when an origin operation is reused", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "cancel", timeoutMs: 500 });

		const first = await value.origin.submitIntentWithOutcome!({ taskId: created.taskId, type: "task.cancelled", payload: {} });
		const retry = await value.origin.submitIntentWithOutcome!({ taskId: created.taskId, type: "task.cancelled", payload: {} });

		expect(first).toEqual({ authority: "origin", canonicalEvent: { type: "task.cancelled", reused: false } });
		expect(retry).toEqual({ authority: "origin", canonicalEvent: { type: "task.cancelled", reused: true } });
		expect(value.origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.cancelled"]);
	});

	test("receiver persists an intent before an uncertain send and recovers it with its stable envelope id", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await value.receiver.receive();
		value.relay.failNextSend();

		await expect(value.receiver.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "progress" } })).rejects.toThrow("in-memory relay send failed");
		const pending = value.receiverStore.outbox("pending");
		expect(pending).toHaveLength(1);
		const envelopeId = pending[0]?.envelope.envelopeId;

		await value.receiver.flushOutbox();
		expect(value.receiverStore.outbox("pending")).toHaveLength(0);
		expect(value.relay.envelopesFor(ORIGIN)[0]?.envelopeId).toBe(envelopeId);
	});

	test("keeps quarantined delivery evidence blocked with its stable identity after same-lifetime core replacement", async () => {
		const directory = mkdtempSync("/tmp/pi-task-evidence-quarantine-");
		const path = join(directory, "receiver.sqlite");
		const backing = createInMemoryTaskRelay("memory");
		const blocked = { value: false };
		const relay = blockOriginDelivery(backing, blocked);
		let receiverStore = createTaskStore();
		const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequenceIds("origin") });
		let receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequenceIds("receiver") });
		try {
			await origin.connect();
			await receiver.connect();
			const created = await origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
			await receiver.receive();
			blocked.value = true;
			const evidence = { taskId: created.taskId, type: "task.delivery_receipt", payload: { eventId: "origin-2", stage: "receiver_recorded", state: "confirmed" } } as const;

			await expect(receiver.submitIntent(evidence)).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: false });
			const quarantinedEnvelopeId = receiverStore.quarantinedOutbox()[0]?.envelope.envelopeId;
			if (quarantinedEnvelopeId === undefined) throw new Error("delivery evidence was not quarantined");
			// Replace the core object, retaining this lifetime's RAM state.
			receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequenceIds("resumed-receiver") });
			await receiver.connect();

			await expect(receiver.submitIntent(evidence)).rejects.toMatchObject({
				code: "TARGET_NOT_REGISTERED",
				retryable: false,
				details: expect.objectContaining({ taskId: created.taskId, eventId: "origin-2", stage: "receiver_recorded", state: "confirmed" }),
			});
			expect(receiverStore.quarantinedOutbox().map((record) => record.envelope.envelopeId)).toEqual([quarantinedEnvelopeId]);
			expect(backing.envelopesFor(ORIGIN)).toEqual([]);
		} finally {
			receiverStore.close();
			rmSync(directory, { recursive: true, force: true });
		}
	});

	test("keeps a quarantined Pi insertion receipt blocked on retry", async () => {
		const backing = createInMemoryTaskRelay("memory");
		const blocked = { value: false };
		const relay = blockOriginDelivery(backing, blocked);
		const receiverStore = createTaskStore();
		const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequenceIds("origin") });
		const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequenceIds("receiver") });
		await origin.connect();
		await receiver.connect();
		const created = await origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await receiver.receive();
		blocked.value = true;
		const insertion = { taskId: created.taskId, eventId: "origin-2" };

		await expect(receiver.recordInsertion(insertion)).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: false });
		const quarantinedEnvelopeId = receiverStore.quarantinedOutbox()[0]?.envelope.envelopeId;
		if (quarantinedEnvelopeId === undefined) throw new Error("Pi insertion receipt was not quarantined");
		await expect(receiver.recordInsertion(insertion)).rejects.toMatchObject({
			code: "TARGET_NOT_REGISTERED",
			retryable: false,
			details: expect.objectContaining({ taskId: created.taskId, eventId: "origin-2", stage: "pi_inserted", state: "confirmed" }),
		});
		expect(receiverStore.quarantinedOutbox().map((record) => record.envelope.envelopeId)).toEqual([quarantinedEnvelopeId]);
	});

	test("records one insertion receipt when the same Pi insertion is retried", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await value.receiver.receive();

		await value.receiver.recordInsertion({ taskId: created.taskId, eventId: "origin-2" });
		await value.receiver.recordInsertion({ taskId: created.taskId, eventId: "origin-2" });
		await value.origin.receive();

		expect(value.origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.delivery_receipt"]);
	});

	test("rejects parent acknowledgment before the task is terminal without mutating durable state", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		const acceptedBefore = value.originStore.outbox("accepted").map((record) => record.envelope.envelopeId);

		await expect(value.origin.acknowledgeParent(created.taskId)).rejects.toMatchObject({ code: "TASK_NOT_TERMINAL" });

		expect(value.origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created"]);
		expect(value.originStore.outbox("accepted").map((record) => record.envelope.envelopeId)).toEqual(acceptedBefore);
		expect(value.originStore.outbox("pending")).toEqual([]);
	});

	test("reuses one parent acknowledgment event and its fan-out envelopes across sequential retries", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await value.receiver.receive();
		await value.receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
		await value.origin.receive();

		await value.origin.acknowledgeParent(created.taskId);
		const eventIds = value.origin.getTask(created.taskId)?.events.filter((event) => event.type === "task.parent_acknowledged").map((event) => event.eventId);
		const outboxIds = value.originStore.outbox("accepted").map((record) => record.envelope.envelopeId);
		await value.origin.acknowledgeParent(created.taskId);

		expect(value.origin.getTask(created.taskId)?.events.filter((event) => event.type === "task.parent_acknowledged").map((event) => event.eventId)).toEqual(eventIds);
		expect(value.originStore.outbox("accepted").map((record) => record.envelope.envelopeId)).toEqual(outboxIds);
	});

	test("reserves one parent acknowledgment across concurrent retries", async () => {
		const value = fixture();
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await value.receiver.receive();
		await value.receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
		await value.origin.receive();

		const outboxIdsBefore = new Set(value.originStore.outbox("accepted").map((record) => record.envelope.envelopeId));
		await Promise.all([
			value.origin.acknowledgeParent(created.taskId),
			value.origin.acknowledgeParent(created.taskId),
		]);

		expect(value.origin.getTask(created.taskId)?.events.filter((event) => event.type === "task.parent_acknowledged")).toHaveLength(1);
		expect(value.originStore.outbox("accepted").filter((record) => !outboxIdsBefore.has(record.envelope.envelopeId))).toHaveLength(2);
	});

	test("only the resumed origin evaluates timeout and acknowledgement plus insertion receipts remain logical events", async () => {
		const value = fixture(2_000);
		await value.origin.connect();
		await value.receiver.connect();
		const created = await value.origin.createTask({ target: RECEIVER, task: "implement narrowly", timeoutMs: 500 });
		await value.receiver.receive();

		await value.receiver.recordInsertion({ taskId: created.taskId, eventId: "origin-2" });
		await value.origin.receive();
		expect(value.origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.delivery_receipt"]);

		value.advance(2_501);
		await value.receiver.evaluateTimeouts();
		expect(value.receiver.getTask(created.taskId)?.status).toBe("active");
		await value.origin.evaluateTimeouts();
		expect(value.origin.getTask(created.taskId)?.status).toBe("timed_out");

		await value.origin.acknowledgeParent(created.taskId);
		expect(value.origin.getTask(created.taskId)?.events.at(-1)?.type).toBe("task.parent_acknowledged");
	});
});
