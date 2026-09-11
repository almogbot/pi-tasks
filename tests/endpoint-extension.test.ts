import { describe, expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";

import { createInMemoryTaskRelay } from "../src/in-memory-task-relay";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { registerAgentTaskTools } from "../src/extension";
import { TaskProtocolError } from "../src/task-protocol";
import type { TaskRelay } from "../src/task-protocol";

interface Tool {
	readonly name: string;
	readonly description: string;
	readonly parameters: unknown;
	execute(id: string, parameters: Record<string, unknown>, signal: AbortSignal, update: undefined, context: unknown): Promise<{ readonly content: readonly { readonly text: string }[]; readonly details: unknown; readonly terminate?: boolean }>;
}

test.each([
	["agent_task_status", {}],
	["agent_task_wait", { timeoutMs: 1_000 }],
	["agent_task_message", { type: "information", message: "historical" }],
	["agent_task_cancel", {}],
	["agent_task_ack", {}],
	["agent_task_done", { status: "completed", summary: "historical" }],
] as const)("unknown task tools return non-retryable UNKNOWN_TASK after RAM loss: %s", async (name, params) => {
	const relay = createInMemoryTaskRelay("memory");
	const store = createTaskStore();
	const core = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store, ids: sequence("parent") });
	await core.connect();
	await relay.connect({ endpoint: { relay: "memory", id: "child" }, protocolVersion: "pi-tasks/v2", receiveCursor: "0" });
	const { taskId } = await core.createTask({ target: { relay: "memory", id: "child" }, task: "historical assignment", timeoutMs: 1_000 });
	const history = [{ type: "custom_message", customType: "pi-tasks-event", details: { event: core.getTask(taskId)!.events[0] } }];
	store.clear();
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, core);

	const result = await tools[name]!.execute("call", { taskId, ...params }, new AbortController().signal, undefined, { sessionManager: { getEntries: () => history } });

	expect(result.details).toMatchObject({ error: { code: "UNKNOWN_TASK", retryable: false } });
	expect(core.getTask(taskId)).toBeUndefined();
	expect(core.listTasks()).toEqual([]);
	store.close();
});

test("registers endpoint-owned tools with only relay-qualified opaque targets", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const core = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	await core.connect();
	await relay.connect({ endpoint: { relay: "memory", id: "child" }, protocolVersion: "pi-tasks/v2", receiveCursor: "0" });
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, core);

	const result = await tools.agent_task_send!.execute("call", { to: { relay: "memory", id: "child" }, task: "implement narrowly", timeoutMs: 1_000 }, new AbortController().signal, undefined, {});

	expect(result.content[0]?.text).toContain("## task accepted");
	expect(result.content[0]?.text).toContain("memory/child");
	expect(JSON.stringify(tools.agent_task_send?.parameters)).not.toContain("machine");
	expect(JSON.stringify(tools.agent_task_send?.parameters)).not.toContain("tailnet");

	await tools.agent_task_message!.execute("call", { taskId: "parent-1", type: "information", message: "owner update" }, new AbortController().signal, undefined, {});
	expect(core.getTask("parent-1")?.events.at(-1)?.type).toBe("task.information");
});

test("status and inbox expose an active assignment only to its receiver", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const assignment = "inspect the receiver-only payload";
	const created = await origin.createTask({ target: receiver.endpoint, task: assignment, timeoutMs: 1_000 });
	await receiver.receive();
	const originTools: Record<string, Tool> = {};
	const receiverTools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; originTools[value.name] = value; } } as unknown as ExtensionAPI, origin);
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; receiverTools[value.name] = value; } } as unknown as ExtensionAPI, receiver);
	const signal = new AbortController().signal;
	const receiverEntries: unknown[] = [];
	const receiverContext = { sessionManager: { getEntries: (): readonly unknown[] => receiverEntries } };

	const preInsertionStatus = await receiverTools.agent_task_status!.execute("call", { taskId: created.taskId }, signal, undefined, receiverContext);
	const preInsertionInbox = await receiverTools.agent_task_inbox!.execute("call", {}, signal, undefined, receiverContext);
	const originStatus = await originTools.agent_task_status!.execute("call", { taskId: created.taskId }, signal, undefined, {});
	const originInbox = await originTools.agent_task_inbox!.execute("call", {}, signal, undefined, {});

	expect(preInsertionStatus.content[0]?.text).not.toContain(assignment);
	expect(preInsertionInbox.content[0]?.text).not.toContain(assignment);
	expect(originStatus.content[0]?.text).not.toContain(assignment);
	expect(originInbox.content[0]?.text).not.toContain(assignment);

	const createdEventId = receiver.getTask(created.taskId)?.events[0]?.eventId;
	receiverEntries.push({ type: "custom_message", customType: "pi-tasks-event", details: { taskId: created.taskId, eventId: createdEventId } });
	const receiverStatus = await receiverTools.agent_task_status!.execute("call", { taskId: created.taskId }, signal, undefined, receiverContext);
	const receiverInbox = await receiverTools.agent_task_inbox!.execute("call", {}, signal, undefined, receiverContext);
	expect(receiverStatus.content[0]?.text).toContain(assignment);
	expect(receiverInbox.content[0]?.text).toContain(assignment);
});

test("origin status reports structured receiver receipt (RAM) and blocked Pi insertion evidence", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "report blocked insertion", timeoutMs: 1_000 });
	await receiver.receive();
	const createdEventId = receiver.getTask(created.taskId)?.events[0]?.eventId ?? "";
	await receiver.submitIntent({ taskId: created.taskId, type: "task.delivery_receipt", payload: { eventId: createdEventId, stage: "receiver_recorded", state: "confirmed" } });
	await receiver.submitIntent({ taskId: created.taskId, type: "task.delivery_receipt", payload: { eventId: createdEventId, stage: "pi_insertion", state: "blocked", retryable: true } });
	await origin.receive();
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);

	const status = await tools.agent_task_status!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});

	expect(status.details).toMatchObject({
		deliveryEvidence: {
			receiverReceipt: "confirmed",
			piInsertion: "blocked",
			wakeAcceptance: "not_confirmed",
			modelExecution: "not_evidenced",
		},
	});
	expect(status.content[0]?.text).toContain("Pi insertion: blocked; retryable");
});

test("done tool reports successful origin-owned completion as canonical rather than an unsubmitted receiver intent", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "complete at origin", timeoutMs: 1_000 });
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);

	const result = await tools.agent_task_done!.execute("call", { taskId: created.taskId, status: "completed", summary: "finished" }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({
		taskId: created.taskId,
		requestedStatus: "completed",
		observedCanonicalStatus: "completed",
		canonicalCompletion: { state: "confirmed", status: "completed" },
	});
	expect(result.content[0]?.text).toContain("## task completed");
	expect(result.content[0]?.text).not.toContain("terminal intent");
	expect(result.terminate).toBe(true);
});

test("done tool keeps a reused canonical origin cancellation distinct from a late terminal", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "retry canonical cancellation", timeoutMs: 1_000 });
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);
	const signal = new AbortController().signal;
	await tools.agent_task_done!.execute("call-1", { taskId: created.taskId, status: "cancelled", summary: "cancel" }, signal, undefined, {});

	const retry = await tools.agent_task_done!.execute("call-2", { taskId: created.taskId, status: "cancelled", summary: "retry cancel" }, signal, undefined, {});

	expect(retry.details).toEqual({
		taskId: created.taskId,
		requestedStatus: "cancelled",
		observedCanonicalStatus: "cancelled",
		canonicalCompletion: { state: "confirmed", status: "cancelled" },
	});
	expect(retry.content[0]?.text).toContain("## task cancelled");
	expect(origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.cancelled"]);
});

test("done tool reports an origin-owned late terminal as a canonical event", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "record late terminal", timeoutMs: 1_000 });
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);
	const signal = new AbortController().signal;
	await tools.agent_task_done!.execute("call-1", { taskId: created.taskId, status: "completed", summary: "finished" }, signal, undefined, {});
	await tools.agent_task_done!.execute("call-2", { taskId: created.taskId, status: "cancelled", summary: "late cancellation" }, signal, undefined, {});
	await origin.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "later event" } });
	expect(origin.getTask(created.taskId)?.events.at(-1)?.type).toBe("task.information");

	const result = await tools.agent_task_done!.execute("call-3", { taskId: created.taskId, status: "cancelled", summary: "retry late cancellation" }, signal, undefined, {});

	expect(result.details).toEqual({
		taskId: created.taskId,
		requestedStatus: "cancelled",
		observedCanonicalStatus: "completed",
		canonicalEvent: { type: "task.late_terminal", reused: true },
	});
	expect(result.content[0]?.text).toContain("## canonical late terminal recorded");
	expect(result.content[0]?.text).not.toContain("terminal intent");
	expect(result.terminate).toBe(true);
});

test("done tool reports accepted terminal intent after observing canonical cancellation", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "cancel before completion", timeoutMs: 1_000 });
	await receiver.receive();
	await origin.submitIntent({ taskId: created.taskId, type: "task.cancelled", payload: {} });
	await receiver.receive();
	expect(receiver.getTask(created.taskId)?.status).toBe("cancelled");
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, receiver);

	const result = await tools.agent_task_done!.execute("call", { taskId: created.taskId, status: "completed", summary: "finished" }, new AbortController().signal, undefined, {});
	await origin.receive();

	expect(result.details).toMatchObject({
		taskId: created.taskId,
		requestedStatus: "completed",
		observedCanonicalStatus: "cancelled",
		terminalDelivery: { state: "accepted", intentType: "task.completed" },
	});
	expect(result.content[0]?.text).toContain("## terminal intent accepted");
	expect(result.content[0]?.text).toContain("observed canonical status: cancelled");
	expect(result.content[0]?.text).not.toContain("## task completed");
	expect(result.terminate).toBe(true);
	expect(origin.getTask(created.taskId)?.status).toBe("cancelled");
	expect(origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.cancelled", "task.late_terminal"]);
});

test("done tool preserves blocked receiver terminal evidence without claiming canonical completion", async () => {
	const state = { blocked: false };
	const relay = expiringTargetRelay(state, "parent");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "complete after origin disappears", timeoutMs: 1_000 });
	await receiver.receive();
	state.blocked = true;
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, receiver);

	const result = await tools.agent_task_done!.execute("call", { taskId: created.taskId, status: "completed", summary: "finished" }, new AbortController().signal, undefined, {});

	expect(result.details).toMatchObject({
		taskId: created.taskId,
		status: "active",
		terminalDelivery: {
			state: "delivery_blocked",
			intentType: "task.completed",
			origin: origin.endpoint,
			error: { code: "TARGET_NOT_REGISTERED", retryable: false, details: { targetId: origin.endpoint.id } },
		},
		error: { code: "TARGET_NOT_REGISTERED", message: "target is inactive", retryable: false },
	});
	expect(result.content[0]?.text).toContain("## terminal intent delivery blocked");
	expect(result.content[0]?.text).toContain("canonical completion not confirmed");
	expect(result.content[0]?.text).not.toContain("## task completed");
	expect(result.terminate).toBeUndefined();
	expect(receiver.getTask(created.taskId)?.status).toBe("active");
});

test("done tool reports canonical origin completion with a blocked target-delivery warning", async () => {
	const state = { blocked: false };
	const relay = expiringTargetRelay(state);
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "complete before target delivery", timeoutMs: 1_000 });
	await receiver.receive();
	state.blocked = true;
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);

	const result = await tools.agent_task_done!.execute("call", { taskId: created.taskId, status: "completed", summary: "finished" }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({
		taskId: created.taskId,
		status: "completed",
		warnings: [{
			code: "TARGET_NOT_REGISTERED",
			message: "target is inactive",
			retryable: false,
			delivery: "blocked",
			target: receiver.endpoint,
			details: { targetId: receiver.endpoint.id },
		}],
	});
	expect(result.content[0]?.text).toContain("## task completed");
	expect(result.content[0]?.text).toContain("canonical status: completed");
	expect(result.content[0]?.text).toContain("target delivery: blocked");
	expect(result.content[0]?.text).not.toContain("terminal intent");
	expect(result.content[0]?.text).not.toContain("## task error");
	expect(result.terminate).toBe(true);
	expect(origin.getTask(created.taskId)?.status).toBe("completed");
});

test("cancel tool preserves completed-task late-terminal behavior", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "complete before cancellation", timeoutMs: 1_000 });
	await receiver.receive();
	await receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
	await origin.receive();
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);

	const result = await tools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({ taskId: created.taskId });
	expect(result.content[0]?.text).toContain("task cancellation requested");
	expect(origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.completed", "task.late_terminal"]);
});

test("late-cancel retries keep structured delivery failure when the completed target disappears", async () => {
	const state = { blocked: false };
	const relay = expiringTargetRelay(state);
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore(), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore(), ids: sequence("child") });
	await origin.connect();
	await receiver.connect();
	const created = await origin.createTask({ target: receiver.endpoint, task: "complete before target disappears", timeoutMs: 1_000 });
	await receiver.receive();
	await receiver.submitIntent({ taskId: created.taskId, type: "task.completed", payload: { summary: "finished" } });
	await origin.receive();
	state.blocked = true;
	const tools: Record<string, Tool> = {};
	registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI, origin);

	const first = await tools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});
	const retry = await tools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});

	expect(first.details).toMatchObject({
		taskId: created.taskId,
		target: receiver.endpoint,
		targetId: receiver.endpoint.id,
		error: { code: "TARGET_NOT_REGISTERED", message: "target is inactive", retryable: false },
	});
	expect(first.details).not.toHaveProperty("status");
	expect(first.details).not.toHaveProperty("warnings");
	expect(retry.details).toEqual(first.details);
	expect(origin.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.completed", "task.late_terminal"]);
});

test("cancel retries preserve the durable blocked-delivery warning after same-lifetime core replacement", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-extension-");
	const path = join(directory, "tasks.sqlite");
	const state = { blocked: false };
	const relay = expiringTargetRelay(state);
	const target = { relay: "memory", id: "child" } as const;
	let store = createTaskStore();
	try {
		let core = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store, ids: sequence("parent") });
		await core.connect();
		await relay.connect({ endpoint: target, protocolVersion: "pi-tasks/v2", receiveCursor: "0" });
		const created = await core.createTask({ target, task: "cancel durably", timeoutMs: 1_000 });
		state.blocked = true;
		const firstTools: Record<string, Tool> = {};
		registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; firstTools[value.name] = value; } } as unknown as ExtensionAPI, core);

		const first = await firstTools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});
		expect(first.details).toMatchObject({ taskId: created.taskId, status: "cancelled", warnings: [{ delivery: "blocked" }] });
		expect(store.outbox("pending")).toEqual([]);
		const sequentialRetry = await firstTools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});
		const concurrentRetries = await Promise.all([
			firstTools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {}),
			firstTools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {}),
		]);
		for (const retry of [sequentialRetry, ...concurrentRetries]) expect(retry.details).toEqual(first.details);
		// Replace the core object, retaining this lifetime's RAM state.
		core = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store, ids: sequence("restart") });
		const retryTools: Record<string, Tool> = {};
		registerAgentTaskTools({ on: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; retryTools[value.name] = value; } } as unknown as ExtensionAPI, core);
		const retry = await retryTools.agent_task_cancel!.execute("call", { taskId: created.taskId }, new AbortController().signal, undefined, {});

		expect(retry.details).toEqual(first.details);
		expect(core.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.cancelled"]);
	} finally {
		store.close();
		rmSync(directory, { recursive: true, force: true });
	}
});

test("runs timeout evaluation and durable outbox retry in the default extension lifecycle", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const now = { value: 0 };
	const store = createTaskStore();
	const core = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store, clock: { now: (): number => now.value }, ids: sequence("parent") });
	await core.connect();
	await relay.connect({ endpoint: { relay: "memory", id: "child" }, protocolVersion: "pi-tasks/v2", receiveCursor: "0" });
	const created = await core.createTask({ target: { relay: "memory", id: "child" }, task: "expire", timeoutMs: 1_000 });
	relay.failNextSend();
	await expect(core.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "retry me" } })).rejects.toThrow("in-memory relay send failed");
	now.value = 1_000;

	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	registerAgentTaskTools({
		on(event: string, handler: unknown) { if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>; },
		registerTool() { undefined; },
	} as unknown as ExtensionAPI, core);
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	expect(sessionStart).toBeDefined();
	await sessionStart!({}, context);

	expect(core.getTask(created.taskId)?.status).toBe("timed_out");
	expect(store.outbox("pending")).toEqual([]);
});

test("publishes the supported core and relay contract independently from the Pi extension", async () => {
	const [source, packageJson] = await Promise.all([
		Bun.file(new URL("../src/extension.ts", import.meta.url)).text(),
		Bun.file(new URL("../package.json", import.meta.url)).json() as Promise<{ readonly module: string }>,
	]);
	expect(source).not.toContain("core: TaskCore = createInMemoryExtensionCore()");
	expect(packageJson.module).toBe("src/index.ts");
});

function expiringTargetRelay(state: { blocked: boolean }, blockedTargetId = "child"): TaskRelay {
	const relay = createInMemoryTaskRelay("memory");
	return {
		id: relay.id,
		async connect(input) { return relay.connect(input); },
		async resolve(input) { return relay.resolve(input); },
		async send(input) {
			if (state.blocked && input.target.id === blockedTargetId) throw new TaskProtocolError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false, details: { targetId: input.target.id } });
			return relay.send(input);
		},
		async receive(input) { return relay.receive(input); },
		async acknowledgeDelivery(input) { await relay.acknowledgeDelivery(input); },
	};
}

function sequence(prefix: string): () => string {
	let number = 0;
	return (): string => `${prefix}-${++number}`;
}
