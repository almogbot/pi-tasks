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
	execute(id: string, parameters: Record<string, unknown>, signal: AbortSignal, update: undefined, context: unknown): Promise<{ readonly content: readonly { readonly text: string }[]; readonly details: unknown }>;
}

test("registers endpoint-owned tools with only relay-qualified opaque targets", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const core = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore({ path: ":memory:" }), ids: sequence("parent") });
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

test("cancel tool preserves completed-task late-terminal behavior", async () => {
	const relay = createInMemoryTaskRelay("memory");
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore({ path: ":memory:" }), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore({ path: ":memory:" }), ids: sequence("child") });
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
	const origin = createTaskCore({ endpoint: { relay: "memory", id: "parent" }, relay, store: createTaskStore({ path: ":memory:" }), ids: sequence("parent") });
	const receiver = createTaskCore({ endpoint: { relay: "memory", id: "child" }, relay, store: createTaskStore({ path: ":memory:" }), ids: sequence("child") });
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

test("cancel retries preserve the durable blocked-delivery warning after restart", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-extension-");
	const path = join(directory, "tasks.sqlite");
	const state = { blocked: false };
	const relay = expiringTargetRelay(state);
	const target = { relay: "memory", id: "child" } as const;
	let store = createTaskStore({ path });
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
		store.close();

		store = createTaskStore({ path });
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
	const store = createTaskStore({ path: ":memory:" });
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
	expect(source).not.toContain("gateway-client");
	expect(source).not.toContain("core: TaskCore = createInMemoryExtensionCore()");
	expect(packageJson.module).toBe("src/index.ts");
});

function expiringTargetRelay(state: { blocked: boolean }): TaskRelay {
	const relay = createInMemoryTaskRelay("memory");
	return {
		id: relay.id,
		async connect(input) { return relay.connect(input); },
		async resolve(input) { return relay.resolve(input); },
		async send(input) {
			if (state.blocked && input.target.id === "child") throw new TaskProtocolError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false, details: { targetId: input.target.id } });
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
