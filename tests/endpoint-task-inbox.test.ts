import { describe, expect, spyOn, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";

import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { TASK_PROTOCOL_VERSION } from "../src/task-protocol";
import { deliverTaskInbox } from "../src/task-inbox";

const origin = { relay: "memory", id: "origin" };
const receiver = { relay: "memory", id: "receiver" };

test("archives non-waking facts before ACK, deduplicates an ACK retry and retains parent acknowledgment in session history", async () => {
	const relay = createInMemoryTaskRelay("memory"), parentStore = createTaskStore(), childStore = createTaskStore();
	const parent = createTaskCore({ endpoint: origin, relay, store: parentStore, ids: ids("archive-parent") });
	const child = createTaskCore({ endpoint: receiver, relay, store: childStore, ids: ids("archive-child") });
	await parent.connect(); await child.connect();
	const task = await parent.createTask({ target: receiver, task: "archive control facts", timeoutMs: 60_000 });
	await child.receive(); await child.submitIntent({ taskId: task.taskId, type: "task.completed", payload: { summary: "done" } });
	await parent.receive(); await parent.acknowledgeParent(task.taskId);
	const entries: any[] = []; let archived = false, failed = false;
	const pi = {
		sendMessage(message: any) { entries.push({ type: "custom_message", ...message }); },
		appendEntry(customType: string, data: unknown) { entries.push({ type: "custom", customType, data }); if (customType === "pi-tasks-event-record") archived = true; },
	};
	const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } };
	const acknowledge = child.acknowledgeRelayDelivery.bind(child);
	const ack = spyOn(child, "acknowledgeRelayDelivery").mockImplementation(async cursor => {
		if (archived && !failed) { failed = true; throw new Error("fixture ACK unavailable"); }
		return acknowledge(cursor);
	});
	try {
		await expect(deliverTaskInbox(pi, child, context)).rejects.toThrow("fixture ACK unavailable");
		await deliverTaskInbox(pi, child, context);
		await parent.receive(); await deliverTaskInbox(pi, child, context);
		const records = entries.filter(entry => entry.customType === "pi-tasks-event-record");
		expect(records.some(entry => entry.data.event.type === "task.parent_acknowledged")).toBe(true);
		expect(records.some(entry => entry.data.event.type === "task.delivery_receipt")).toBe(true);
		expect(new Set(records.map(entry => entry.data.eventId)).size).toBe(records.length);
		expect(records.every(entry => entry.data.event.taskId === task.taskId)).toBe(true);
		expect(entries.some(entry => entry.type === "custom_message" && entry.details?.event?.type === "task.parent_acknowledged")).toBe(false);
		const restarted = createTaskCore({ endpoint: receiver, relay, store: createTaskStore(), ids: ids("fresh") });
		expect(restarted.listTasks()).toEqual([]); // The archive does not recreate active state.
	} finally { ack.mockRestore(); parentStore.close(); childStore.close(); }
});

test("records inbound RAM state and structural session evidence before relay acknowledgement", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const parent = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), ids: ids("parent") });
	const child = createTaskCore({ endpoint: receiver, relay, store: createTaskStore(), ids: ids("child") });
	await parent.connect();
	await child.connect();
	const created = await parent.createTask({ target: receiver, task: "implement", timeoutMs: 1_000 });
	const entries: unknown[] = [];
	const pi = {
		sendMessage(message: { readonly customType: string; readonly details: unknown }) { entries.push({ type: "custom_message", customType: message.customType, details: message.details }); },
		appendEntry(customType: string, data: unknown) { entries.push({ type: "custom", customType, data }); },
	};
	const context = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => entries } };

	await deliverTaskInbox(pi, child, context);

	expect(entries).toContainEqual({ type: "custom_message", customType: "pi-tasks-event", details: expect.objectContaining({ taskId: created.taskId, eventId: "parent-2", event: expect.objectContaining({ type: "task.created", payload: expect.objectContaining({ task: "implement" }) }) }) });
	expect(relay.envelopesFor(receiver)).toHaveLength(1);
	await parent.receive();
	expect(parent.getTask(created.taskId)?.events.filter((event) => event.type === "task.delivery_receipt").map((event) => event.payload.stage)).toEqual([
		"receiver_recorded", "pi_inserted", "wake_requested", "wake_accepted",
	]);
});

test("reports receiver receipt (RAM) and blocked Pi insertion to the origin without advancing delivery", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const parent = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), ids: ids("parent") });
	const childStore = createTaskStore();
	const child = createTaskCore({ endpoint: receiver, relay, store: childStore, ids: ids("child") });
	await parent.connect();
	await child.connect();
	const created = await parent.createTask({ target: receiver, task: "implement", timeoutMs: 1_000 });
	const context = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => [] } };
	const pi = { sendMessage(): void { undefined; }, appendEntry(): void { undefined; } };

	await deliverTaskInbox(pi, child, context);
	await deliverTaskInbox(pi, child, context);
	await parent.receive();

	expect(childStore.getReceiveCursor()).toBe("0");
	expect(parent.getTask(created.taskId)?.events.filter((event) => event.type === "task.delivery_receipt").map((event) => event.payload)).toEqual([
		expect.objectContaining({ stage: "receiver_recorded", state: "confirmed" }),
		expect.objectContaining({ stage: "pi_insertion", state: "blocked", retryable: true }),
	]);
});

test("keeps the relay delivery retryable until a separate wake is durably accepted", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const parent = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), ids: ids("parent") });
	const childStore = createTaskStore();
	const child = createTaskCore({ endpoint: receiver, relay, store: childStore, ids: ids("child") });
	await parent.connect();
	await child.connect();
	const created = await parent.createTask({ target: receiver, task: "implement", timeoutMs: 1_000 });
	const entries: unknown[] = [];
	let insertionAttempts = 0;
	let wakeAttempts = 0;
	let rejectWake = true;
	const pi = {
		sendMessage(message: { readonly customType: string; readonly details: unknown }) {
			if (message.customType === "pi-tasks-event") insertionAttempts += 1;
			if (message.customType === "pi-tasks-wake") {
				wakeAttempts += 1;
				if (rejectWake) throw new Error("wake rejected");
			}
			entries.push({ type: "custom_message", customType: message.customType, details: message.details });
		},
		appendEntry(customType: string, data: unknown) { entries.push({ type: "custom", customType, data }); },
	};
	const context = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => entries } };

	await expect(deliverTaskInbox(pi, child, context)).rejects.toThrow("wake rejected");
	expect(childStore.getReceiveCursor()).toBe("0");
	expect(insertionAttempts).toBe(1);

	rejectWake = false;
	await deliverTaskInbox(pi, child, context);
	await deliverTaskInbox(pi, child, context);
	expect(childStore.getReceiveCursor()).toBe("1");
	expect(insertionAttempts).toBe(1);
	expect(wakeAttempts).toBe(2);
	expect(entries.filter((entry) => typeof entry === "object" && entry !== null && "customType" in entry && entry.customType === "pi-tasks-event" && "details" in entry && typeof entry.details === "object" && entry.details !== null && "taskId" in entry.details && entry.details.taskId === created.taskId)).toHaveLength(1);
	await parent.receive();
	expect(parent.getTask(created.taskId)?.events.filter((event) => event.type === "task.delivery_receipt").map((event) => event.payload.stage)).toEqual([
		"receiver_recorded", "pi_inserted", "wake_requested", "wake_accepted",
	]);
});

test("origin acknowledges raw receiver intents before rendering their canonical message and completion", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const parentStore = createTaskStore();
	const parent = createTaskCore({ endpoint: origin, relay, store: parentStore, ids: ids("parent") });
	const child = createTaskCore({ endpoint: receiver, relay, store: createTaskStore(), ids: ids("child") });
	await parent.connect();
	await child.connect();
	const created = await parent.createTask({ target: receiver, task: "implement", timeoutMs: 1_000 });
	const childEntries: unknown[] = [];
	const childPi = {
		sendMessage(message: { readonly customType: string; readonly details: unknown }) { childEntries.push({ type: "custom_message", customType: message.customType, details: message.details }); },
		appendEntry(customType: string, data: unknown) { childEntries.push({ type: "custom", customType, data }); },
	};
	const ready = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => childEntries } };
	await deliverTaskInbox(childPi, child, ready);
	await child.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "progress" } });
	await child.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });

	const parentEntries: unknown[] = [];
	const parentPi = {
		sendMessage(message: { readonly customType: string; readonly content: string; readonly details: unknown }) { parentEntries.push({ type: "custom_message", customType: message.customType, content: message.content, details: message.details }); },
		appendEntry(customType: string, data: unknown) { parentEntries.push({ type: "custom", customType, data }); },
	};
	const parentContext = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => parentEntries } };

	await deliverTaskInbox(parentPi, parent, parentContext);

	expect(parent.getTask(created.taskId)?.status).toBe("completed");
	expect(parent.getTask(created.taskId)?.events.filter((event) => event.type === "task.delivery_receipt").map((event) => event.payload.stage)).toEqual([
		"receiver_recorded", "pi_inserted", "wake_requested", "wake_accepted",
	]);
	expect(parent.getTask(created.taskId)?.events.slice(-2).map((event) => event.type)).toEqual(["task.information", "task.completed"]);
	expect(parentStore.getReceiveCursor()).toBe("7");
	expect(parentEntries).toEqual([]);

	await deliverTaskInbox(parentPi, parent, parentContext);

	expect(parentEntries).toEqual(expect.arrayContaining([
		expect.objectContaining({ type: "custom_message", content: expect.stringContaining("progress") }),
		expect.objectContaining({ type: "custom_message", content: expect.stringContaining("finished") }),
	]));
});

test("origin acknowledges raw receiver intents and renders their canonical message and completion once", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const parent = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), ids: ids("parent") });
	const child = createTaskCore({ endpoint: receiver, relay, store: createTaskStore(), ids: ids("child") });
	await parent.connect();
	await child.connect();
	const created = await parent.createTask({ target: receiver, task: "implement", timeoutMs: 1_000 });
	await child.receive();
	await child.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "progress" } });
	await child.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });

	expect(await parent.receive()).toEqual([]);
	const echoed = await relay.receive({ endpoint: origin, cursor: "0", limit: 100 });
	expect(echoed.deliveries).toHaveLength(2);
	expect(echoed.deliveries.every((delivery) => delivery.envelope.kind === "canonical_event")).toBe(true);

	const parentEntries: unknown[] = [];
	const parentPi = {
		sendMessage(message: { readonly customType: string; readonly content: string; readonly details: unknown }) { parentEntries.push({ type: "custom_message", customType: message.customType, content: message.content, details: message.details }); },
		appendEntry(customType: string, data: unknown) { parentEntries.push({ type: "custom", customType, data }); },
	};
	const parentContext = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => parentEntries } };

	await deliverTaskInbox(parentPi, parent, parentContext);

	expect(parentEntries).toEqual(expect.arrayContaining([
		expect.objectContaining({ type: "custom_message", content: expect.stringContaining("progress") }),
		expect.objectContaining({ type: "custom_message", content: expect.stringContaining("finished") }),
	]));
	expect(parentEntries.filter((entry) => typeof entry === "object" && entry !== null && "customType" in entry && entry.customType === "pi-tasks-event")).toHaveLength(2);
	expect(parentEntries.filter((entry) => typeof entry === "object" && entry !== null && "customType" in entry && entry.customType === "pi-tasks-wake")).toHaveLength(2);
});

test("recovers the next assignment after terminal completion, parent acknowledgement, and receiver core replacement", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-inbox-");
	const receiverPath = `${directory}/receiver.sqlite`;
	const relay = createInMemoryTaskRelay("memory");
	const parentStore = createTaskStore();
	let childStore = createTaskStore();
	const parent = createTaskCore({ endpoint: origin, relay, store: parentStore, ids: ids("parent") });
	let child = createTaskCore({ endpoint: receiver, relay, store: childStore, ids: ids("child") });
	const entries: unknown[] = [];
	const sent: Array<{ readonly customType: string; readonly details: unknown }> = [];
	const pi = {
		sendMessage(message: { readonly customType: string; readonly details: unknown }, _options: { readonly triggerTurn: boolean }) {
			sent.push({ customType: message.customType, details: message.details });
			entries.push({ type: "custom_message", customType: message.customType, details: message.details });
		},
		appendEntry(customType: string, data: unknown) { entries.push({ type: "custom", customType, data }); },
	};
	const context = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => entries } };

	try {
		await parent.connect();
		await child.connect();
		const first = await parent.createTask({ target: receiver, task: "first assignment", timeoutMs: 1_000 });
		await deliverTaskInbox(pi, child, context);
		await deliverTaskInbox(pi, parent, context);
		await deliverTaskInbox(pi, parent, context);

		await child.submitIntent({ taskId: first.taskId, type: "task.completed", payload: { summary: "first complete" } });
		await deliverTaskInbox(pi, parent, context);
		await deliverTaskInbox(pi, parent, context);
		expect(parent.getTask(first.taskId)?.status).toBe("completed");
		await parent.acknowledgeParent(first.taskId);
		await deliverTaskInbox(pi, child, context);
		expect(child.getTask(first.taskId)?.events.map((event) => event.type)).toContain("task.parent_acknowledged");

		const second = await parent.createTask({ target: receiver, task: "second assignment after same-lifetime core replacement", timeoutMs: 1_000 });
		await child.receive();
		expect(child.getTask(second.taskId)?.status).toBe("active");
		// Replace the core object, retaining this lifetime's RAM state.
		child = createTaskCore({ endpoint: receiver, relay, store: childStore, ids: ids("restarted-child") });
		await child.connect();
		await deliverTaskInbox(pi, child, context);

		expect(entries.filter((entry) => typeof entry === "object" && entry !== null && "customType" in entry && entry.customType === "pi-tasks-event" && "details" in entry && typeof entry.details === "object" && entry.details !== null && "taskId" in entry.details && entry.details.taskId === second.taskId)).toHaveLength(1);
		expect(sent.filter((message) => message.customType === "pi-tasks-wake" && typeof message.details === "object" && message.details !== null && "taskId" in message.details && message.details.taskId === second.taskId)).toHaveLength(1);
		await deliverTaskInbox(pi, child, context);
		expect(sent.filter((message) => message.customType === "pi-tasks-wake" && typeof message.details === "object" && message.details !== null && "taskId" in message.details && message.details.taskId === second.taskId)).toHaveLength(1);
	} finally {
		childStore.close();
		parentStore.close();
		rmSync(directory, { recursive: true, force: true });
	}
});

test("fails closed on an unknown canonical event without advancing the relay delivery cursor", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const parent = createTaskCore({ endpoint: origin, relay, store: createTaskStore(), ids: ids("parent") });
	const child = createTaskCore({ endpoint: receiver, relay, store: createTaskStore(), ids: ids("child") });
	await parent.connect();
	await child.connect();
	const created = await parent.createTask({ target: receiver, task: "implement", timeoutMs: 1_000 });
	await child.receive();
	await child.acknowledgeRelayDelivery("1");
	await relay.send({ envelopeId: "unknown-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source: origin, target: receiver, taskId: created.taskId, kind: "canonical_event", payload: JSON.stringify({ eventId: "unknown-event", taskId: created.taskId, type: "task.unrecognized", sequence: "2", source: origin, target: receiver, occurredAt: 1, payload: {} }) });
	const pi = { sendMessage() { throw new Error("must not insert an unknown event"); }, appendEntry() { throw new Error("must not advance cursor"); } };
	const context = { isIdle: (): boolean => true, hasPendingMessages: (): boolean => false, sessionManager: { getEntries: (): readonly unknown[] => [] } };

	await expect(deliverTaskInbox(pi, child, context)).rejects.toThrow("canonical event envelope headers or payload are invalid");
	await expect(deliverTaskInbox(pi, child, context)).rejects.toThrow("canonical event envelope headers or payload are invalid");
});

function ids(prefix: string): () => string {
	let current = 0;
	return (): string => `${prefix}-${++current}`;
}
