import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";

import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { TASK_PROTOCOL_VERSION, TaskEnvelopeKind, TaskProtocolError } from "../src/task-protocol";
import type { RelayEnvelope, TaskEndpoint, TaskRelay } from "../src/task-protocol";

const ORIGIN: TaskEndpoint = { relay: "memory", id: "origin" };
const RECEIVER: TaskEndpoint = { relay: "memory", id: "receiver" };
const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

test("rejects hostile receiver timeout intents before durable receipt or task history mutation", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const originStore = createTaskStore();
	const receiverStore = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: originStore, ids: sequence("origin") });
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "implement", timeoutMs: 1_000 });
	await receiver.receive();
	await receiver.acknowledgeRelayDelivery("1");

	await relay.send({
		envelopeId: "hostile-timeout",
		protocolVersion: TASK_PROTOCOL_VERSION,
		source: RECEIVER,
		target: ORIGIN,
		taskId: created.taskId,
		kind: "intent",
		payload: JSON.stringify({ intentId: "hostile-intent", taskId: created.taskId, type: "task.timed_out", payload: {} }),
	});

	await expect(origin.receive()).rejects.toThrow("intent envelope headers or payload are invalid");
	expect(origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created"]);
	expect(originStore.getReceiveCursor()).toBe("0");
	await expect(origin.receive()).rejects.toThrow("intent envelope headers or payload are invalid");
	expect(origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created"]);
});

test("rejects mismatched assignment headers before durable receipt and preserves that failure after same-lifetime core replacement", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-core-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const relay = createInMemoryTaskRelay("memory");
	const store = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store, ids: sequence("receiver") });
	await receiver.connect();
	await relay.connect({ endpoint: ORIGIN, protocolVersion: TASK_PROTOCOL_VERSION, receiveCursor: "0" });
	await relay.send({
		envelopeId: "mismatched-assignment",
		protocolVersion: TASK_PROTOCOL_VERSION,
		source: ORIGIN,
		target: RECEIVER,
		taskId: "header-task",
		kind: "assignment",
		payload: JSON.stringify({
			task: { taskId: "payload-task", protocolVersion: TASK_PROTOCOL_VERSION, origin: ORIGIN, target: RECEIVER, task: "hostile", createdAt: 1, expiresAt: 2, status: "active" },
			event: { eventId: "created", taskId: "payload-task", type: "task.created", sequence: "1", source: ORIGIN, target: RECEIVER, occurredAt: 1, payload: { task: "hostile" } },
		}),
	});

	await expect(receiver.receive()).rejects.toThrow("assignment envelope headers do not match its payload");
	expect(receiver.listTasks()).toEqual([]);
	// Replace the core object, retaining this lifetime's RAM state.
	const restartedStore = store;
	const restarted = createTaskCore({ endpoint: RECEIVER, relay, store: restartedStore, ids: sequence("restart") });
	await expect(restarted.receive()).rejects.toThrow("assignment envelope headers do not match its payload");
	expect(restarted.listTasks()).toEqual([]);
	restartedStore.close();
});

test("rejects assignments whose created event payload disagrees with the task header", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const store = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store, ids: sequence("receiver") });
	await receiver.connect();
	await relay.connect({ endpoint: ORIGIN, protocolVersion: TASK_PROTOCOL_VERSION, receiveCursor: "0" });
	await relay.send({
		envelopeId: "mismatched-created-payload",
		protocolVersion: TASK_PROTOCOL_VERSION,
		source: ORIGIN,
		target: RECEIVER,
		taskId: "task-1",
		kind: "assignment",
		payload: JSON.stringify({
			task: { taskId: "task-1", protocolVersion: TASK_PROTOCOL_VERSION, origin: ORIGIN, target: RECEIVER, task: "trusted instructions", createdAt: 1, expiresAt: 2, status: "active" },
			event: { eventId: "created", taskId: "task-1", type: "task.created", sequence: "1", source: ORIGIN, target: RECEIVER, occurredAt: 1, payload: { task: "hostile instructions" } },
		}),
	});

	await expect(receiver.receive()).rejects.toThrow("assignment envelope headers do not match its payload");
	expect(receiver.listTasks()).toEqual([]);
});

test("retries a durable canonical outbox before acknowledging a replayed receiver intent", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const originStore = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: originStore, ids: sequence("origin") });
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: createTaskStore(), ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "implement", timeoutMs: 1_000 });
	await receiver.receive();
	await receiver.acknowledgeRelayDelivery("1");
	await receiver.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "durable before acknowledgement" } });

	relay.failNextSend();
	await expect(origin.receive()).rejects.toThrow("in-memory relay send failed");
	expect(originStore.outbox("pending")).toHaveLength(1);
	expect(originStore.quarantinedOutbox()).toEqual([]);
	expect(originStore.getReceiveCursor()).toBe("0");

	const replayed = await origin.receive();
	expect(replayed).toHaveLength(1);
	expect(originStore.outbox("pending")).toHaveLength(0);
	expect(Number(originStore.getReceiveCursor())).toBeGreaterThan(0);
});

test("quarantines a permanent rejection, reports it to its initiating operation, and does not retry it", async () => {
	const state = { blockedTargetId: "stale" as string | undefined, sent: [] as string[], receiveCalls: 0 };
	const relay = selectiveRelay(state);
	const origin = { relay: relay.id, id: "origin" };
	const store = createTaskStore();
	const core = createTaskCore({ endpoint: origin, relay, store, ids: sequence("isolated") });
	await core.connect();

	await expect(core.createTask({ target: { relay: relay.id, id: "stale" }, task: "stale", timeoutMs: 1_000 })).rejects.toMatchObject({
		code: "TARGET_NOT_REGISTERED",
		retryable: false,
		details: { taskId: "isolated-1" },
	});
	const created = await core.createTask({ target: { relay: relay.id, id: "live" }, task: "deliver", timeoutMs: 1_000 });

	expect(created.taskId).toBe("isolated-4");
	expect(state.sent).toEqual(["isolated-3", "isolated-6"]);
	expect(store.outbox("pending")).toEqual([]);
	expect(store.quarantinedOutbox()).toEqual([expect.objectContaining({
		errorCode: "TARGET_NOT_REGISTERED",
		reason: "target is inactive",
		details: { targetId: "stale" },
		priorState: "pending",
		envelope: expect.objectContaining({ envelopeId: "isolated-3" }),
	})]);
});

test("keeps retryable target registration failures pending and reports them on later flushes", async () => {
	const state = { blockedTargetId: "stale" as string | undefined, sent: [] as string[], receiveCalls: 0, retryable: true };
	const relay = selectiveRelay(state);
	const origin = { relay: relay.id, id: "origin" };
	const store = createTaskStore();
	const core = createTaskCore({ endpoint: origin, relay, store, ids: sequence("transient") });
	await core.connect();

	await expect(core.createTask({ target: { relay: relay.id, id: "stale" }, task: "retry", timeoutMs: 1_000 })).rejects.toMatchObject({
		code: "TARGET_NOT_REGISTERED",
		retryable: true,
		details: { taskId: "transient-1" },
	});
	await expect(core.flushOutbox()).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: true });

	expect(state.sent).toEqual(["transient-3", "transient-3"]);
	expect(store.outbox("pending").map((record) => record.envelope.envelopeId)).toEqual(["transient-3"]);
	expect(store.quarantinedOutbox()).toEqual([]);
});

test("exposes a permanently blocked receiver terminal delivery without changing canonical task status", async () => {
	const state = { blocked: false, retryable: false };
	const relay = expiringOriginRelay(state);
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const receiverStore = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "complete after origin expiry", timeoutMs: 1_000 });
	await receiver.receive();
	state.blocked = true;

	await expect(receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } })).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: false });
	const blocked = receiver.getTask(created.taskId)?.terminalDelivery;

	expect(receiver.getTask(created.taskId)?.status).toBe("active");
	expect(blocked).toEqual({
		state: "delivery_blocked",
		intentId: expect.any(String),
		intentType: "task.completed",
		envelopeId: expect.any(String),
		origin: ORIGIN,
		blockedAt: expect.any(Number),
		error: { code: "TARGET_NOT_REGISTERED", retryable: false, details: { targetId: ORIGIN.id } },
	});
	await expect(receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } })).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: false });
	expect(receiver.getTask(created.taskId)?.terminalDelivery).toEqual(blocked);
	expect(receiverStore.quarantinedOutbox()).toHaveLength(1);
});

test("reuses one pending receiver terminal intent across concurrent retryable failures", async () => {
	const state = { blocked: false, retryable: true };
	const relay = expiringOriginRelay(state);
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const receiverStore = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "retry terminal", timeoutMs: 1_000 });
	await receiver.receive();
	state.blocked = true;

	const attempts = await Promise.allSettled([
		receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } }),
		receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } }),
	]);

	expect(attempts.every((attempt) => attempt.status === "rejected")).toBe(true);
	expect(receiverStore.outbox("pending")).toHaveLength(1);
	const pendingEnvelopeId = receiverStore.outbox("pending")[0]?.envelope.envelopeId;
	if (pendingEnvelopeId === undefined) throw new Error("expected one pending terminal envelope");
	expect(receiver.getTask(created.taskId)?.terminalDelivery).toEqual({
		state: "pending",
		intentId: expect.any(String),
		intentType: "task.completed",
		envelopeId: pendingEnvelopeId,
		origin: ORIGIN,
	});
});

test("reserves one receiver terminal action across conflicting concurrent calls", async () => {
	const state = { blocked: false, retryable: true };
	const relay = expiringOriginRelay(state);
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const receiverStore = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "conflicting terminal", timeoutMs: 1_000 });
	await receiver.receive();
	state.blocked = true;

	const attempts = await Promise.allSettled([
		receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } }),
		receiver.submitIntent({ taskId: created.taskId, type: "task.failed", payload: { summary: "failed" } }),
	]);

	expect(attempts[0]).toMatchObject({ status: "rejected", reason: { code: "TARGET_NOT_REGISTERED", retryable: true } });
	expect(attempts[1]).toMatchObject({ status: "rejected", reason: { code: "TERMINAL_INTENT_CONFLICT", retryable: false } });
	expect(receiverStore.outbox("pending")).toHaveLength(1);
	expect(receiver.getTask(created.taskId)?.terminalDelivery).toMatchObject({ state: "pending", intentType: "task.completed" });
});

test("keeps one accepted receiver terminal identity across sequential retries", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const receiverStore = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "accept terminal", timeoutMs: 1_000 });
	await receiver.receive();

	await receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
	const accepted = receiver.getTask(created.taskId)?.terminalDelivery;
	await receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });

	expect(accepted).toEqual({
		state: "accepted",
		intentId: expect.any(String),
		intentType: "task.completed",
		envelopeId: expect.any(String),
		origin: ORIGIN,
	});
	expect(receiver.getTask(created.taskId)?.terminalDelivery).toEqual(accepted);
	expect(receiverStore.outbox("accepted").filter((record) => record.envelope.kind === TaskEnvelopeKind.intent)).toHaveLength(1);
});

test("preserves blocked receiver terminal identity across same-lifetime core replacement", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-core-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const state = { blocked: false, retryable: false };
	const relay = expiringOriginRelay(state);
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: createTaskStore(), ids: sequence("origin") });
	const receiverStore = createTaskStore();
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "restart blocked terminal", timeoutMs: 1_000 });
	await receiver.receive();
	state.blocked = true;
	await expect(receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } })).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED" });
	const blocked = receiver.getTask(created.taskId)?.terminalDelivery;
	const quarantine = receiverStore.quarantinedOutbox();
	// Replace the core object, retaining this lifetime's RAM state.
	const restartedStore = receiverStore;
	const restarted = createTaskCore({ endpoint: RECEIVER, relay, store: restartedStore, ids: sequence("restart") });
	await expect(restarted.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } })).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED" });

	expect(restarted.getTask(created.taskId)?.terminalDelivery).toEqual(blocked);
	expect(restartedStore.quarantinedOutbox()).toEqual(quarantine);
	restartedStore.close();
});

test("reserves one origin cancellation across sequential, concurrent, and same-lifetime core replacement retries", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-core-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const state = { blockedTargetId: undefined as string | undefined, sent: [] as string[], receiveCalls: 0 };
	const relay = selectiveRelay(state);
	const store = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store, ids: sequence("origin") });
	await origin.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "cancel after receiver disappears", timeoutMs: 1_000 });
	state.blockedTargetId = RECEIVER.id;

	const blockedOutcome = {
		code: "TARGET_NOT_REGISTERED",
		retryable: false,
		details: {
			taskId: created.taskId,
			envelopeId: expect.any(String),
			target: RECEIVER,
			blockedAt: expect.any(Number),
			targetId: RECEIVER.id,
		},
	};
	await expect(origin.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} })).rejects.toMatchObject(blockedOutcome);
	const cancellationEventId = origin.getTask(created.taskId)?.events.find((event) => event.type === "task.cancelled")?.eventId;
	if (cancellationEventId === undefined) throw new Error("expected canonical cancellation before delivery failure");
	const durableEnvelopeIds = [...store.outbox("accepted"), ...store.quarantinedOutbox()].map((record) => record.envelope.envelopeId).sort();

	await expect(origin.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} })).rejects.toMatchObject(blockedOutcome);
	const concurrentRetries = await Promise.allSettled([
		origin.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} }),
		origin.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} }),
	]);
	for (const retry of concurrentRetries) {
		expect(retry.status).toBe("rejected");
		if (retry.status === "rejected") expect(retry.reason).toMatchObject(blockedOutcome);
	}
	// Replace the core object, retaining this lifetime's RAM state.
	const restartedStore = store;
	const restarted = createTaskCore({ endpoint: ORIGIN, relay, store: restartedStore, ids: sequence("restart") });
	await expect(restarted.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} })).rejects.toMatchObject(blockedOutcome);

	expect(restarted.getTask(created.taskId)?.events.map((event) => [event.type, event.eventId])).toEqual([
		["task.created", "origin-2"],
		["task.cancelled", cancellationEventId],
	]);
	expect([...restartedStore.outbox("accepted"), ...restartedStore.quarantinedOutbox()].map((record) => record.envelope.envelopeId).sort()).toEqual(durableEnvelopeIds);
	restartedStore.close();
});

test("evaluates every expired task when one timeout envelope is undeliverable", async () => {
	const state = { blockedTargetId: undefined as string | undefined, sent: [] as string[], receiveCalls: 0 };
	const relay = selectiveRelay(state);
	const origin = { relay: relay.id, id: "origin" };
	const now = { value: 1_000 };
	const core = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), clock: { now: (): number => now.value }, ids: sequence("timeout") });
	await core.connect();
	const stale = await core.createTask({ target: { relay: relay.id, id: "stale" }, task: "stale", timeoutMs: 500 });
	const live = await core.createTask({ target: { relay: relay.id, id: "live" }, task: "live", timeoutMs: 500 });
	state.blockedTargetId = "stale";
	now.value = 1_501;

	await expect(core.evaluateTimeouts()).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: false });

	expect(core.getTask(stale.taskId)?.status).toBe("timed_out");
	expect(core.getTask(live.taskId)?.status).toBe("timed_out");
});

test("reports timeout delivery only for envelopes created by that evaluation", async () => {
	const state = { blockedTargetId: "stale" as string | undefined, sent: [] as string[], receiveCalls: 0 };
	const relay = selectiveRelay(state);
	const origin = { relay: relay.id, id: "origin" };
	const now = { value: 1_000 };
	const store = createTaskStore();
	const core = createTaskCore({ endpoint: origin, relay, store, clock: { now: (): number => now.value }, ids: sequence("scoped-timeout") });
	await core.connect();
	await expect(core.createTask({ target: { relay: relay.id, id: "stale" }, task: "historical failure", timeoutMs: 10_000 })).rejects.toThrow("target is inactive");
	const expiring = await core.createTask({ target: { relay: relay.id, id: "live" }, task: "expire independently", timeoutMs: 500 });
	now.value = 1_501;

	await expect(core.evaluateTimeouts()).resolves.toBeUndefined();

	expect(core.getTask(expiring.taskId)?.status).toBe("timed_out");
	expect(store.outbox("pending")).toEqual([]);
	expect(store.quarantinedOutbox().map((record) => record.envelope.target.id)).toEqual(["stale"]);
});

test("receives inbox deliveries while an unrelated outbox envelope remains undeliverable", async () => {
	const state = { blockedTargetId: "stale" as string | undefined, sent: [] as string[], receiveCalls: 0 };
	const relay = selectiveRelay(state);
	const origin = { relay: relay.id, id: "origin" };
	const core = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), ids: sequence("receive") });
	await core.connect();
	await expect(core.createTask({ target: { relay: relay.id, id: "stale" }, task: "stale", timeoutMs: 1_000 })).rejects.toThrow("target is inactive");

	await expect(core.receive()).resolves.toEqual([]);
	expect(state.receiveCalls).toBe(1);
});

test("reuses a pending parent acknowledgment event and outbox identity after same-lifetime core replacement", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-core-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const relay = createInMemoryTaskRelay("memory");
	const originStore = createTaskStore();
	const receiverStore = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store: originStore, ids: sequence("origin") });
	const receiver = createTaskCore({ endpoint: RECEIVER, relay, store: receiverStore, ids: sequence("receiver") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: RECEIVER, task: "implement", timeoutMs: 1_000 });
	await receiver.receive();
	await receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
	await origin.receive();

	relay.failNextSend();
	await expect(origin.acknowledgeParent(created.taskId)).rejects.toThrow("in-memory relay send failed");
	const eventId = origin.getTask(created.taskId)?.events.find((event) => event.type === "task.parent_acknowledged")?.eventId;
	if (eventId === undefined) throw new Error("expected the parent acknowledgment event to persist before delivery");
	const outboxIds = [...originStore.outbox("pending"), ...originStore.outbox("accepted")].map((record) => record.envelope.envelopeId);
	// Replace the core object, retaining this lifetime's RAM state.
	const restartedStore = originStore;
	const restarted = createTaskCore({ endpoint: ORIGIN, relay, store: restartedStore, ids: sequence("restart") });
	await restarted.acknowledgeParent(created.taskId);

	expect(restarted.getTask(created.taskId)?.events.filter((event) => event.type === "task.parent_acknowledged").map((event) => event.eventId)).toEqual([eventId]);
	expect([...restartedStore.outbox("pending"), ...restartedStore.outbox("accepted")].map((record) => record.envelope.envelopeId).sort()).toEqual(outboxIds.sort());
	expect(restartedStore.outbox("pending")).toEqual([]);
	restartedStore.close();
});

test("scans every locally-owned origin task for timeout after relay acceptance fails and survives same-lifetime core replacement", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-core-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const relay = createInMemoryTaskRelay("memory");
	const now = { value: 1_000 };
	const clock = { now: (): number => now.value };
	const store = createTaskStore();
	const origin = createTaskCore({ endpoint: ORIGIN, relay, store, clock, ids: sequence("origin") });
	await origin.connect();
	await relay.connect({ endpoint: RECEIVER, protocolVersion: TASK_PROTOCOL_VERSION, receiveCursor: "0" });
	relay.failNextSend();
	await expect(origin.createTask({ target: RECEIVER, task: "implement", timeoutMs: 500 })).rejects.toThrow("in-memory relay send failed");
	now.value = 1_501;
	const restartedStore = store; // Same endpoint lifetime; no restart recovery.
	const restarted = createTaskCore({ endpoint: ORIGIN, relay, store: restartedStore, clock, ids: sequence("restart") });
	await restarted.evaluateTimeouts();

	expect(restarted.getTask("origin-1")?.status).toBe("timed_out");
	expect(restarted.getTask("origin-1")?.events.map((event) => event.type)).toEqual(["task.created", "task.timed_out"]);
	restartedStore.close();
});

function expiringOriginRelay(state: { blocked: boolean; retryable: boolean }): TaskRelay {
	const relay = createInMemoryTaskRelay("memory");
	return {
		id: relay.id,
		async connect(input) { return relay.connect(input); },
		async resolve(input) { return relay.resolve(input); },
		async send(input) {
			if (state.blocked && input.target.id === ORIGIN.id) throw new TaskProtocolError("TARGET_NOT_REGISTERED", "origin is inactive", { retryable: state.retryable, details: { targetId: input.target.id } });
			return relay.send(input);
		},
		async receive(input) { return relay.receive(input); },
		async acknowledgeDelivery(input) { await relay.acknowledgeDelivery(input); },
	};
}

function selectiveRelay(state: { blockedTargetId: string | undefined; sent: string[]; receiveCalls: number; retryable?: boolean }): TaskRelay {
	return {
		id: "selective",
		async connect(input) { return { endpoint: input.endpoint, receiveCursor: input.receiveCursor }; },
		async resolve(input) { return { relay: input.relay, id: input.reference }; },
		async send(input: RelayEnvelope) {
			state.sent.push(input.envelopeId);
			if (input.target.id === state.blockedTargetId) throw new TaskProtocolError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: state.retryable ?? false, details: { targetId: input.target.id } });
			return { envelopeId: input.envelopeId };
		},
		async receive(input) {
			state.receiveCalls += 1;
			return { deliveries: [], nextCursor: input.cursor, hasMore: false };
		},
		async acknowledgeDelivery() { undefined; },
	};
}

function sequence(prefix: string): () => string {
	let current = 0;
	return (): string => `${prefix}-${++current}`;
}
