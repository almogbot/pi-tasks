import { describe, expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import { registerAgentTaskTools as registerV2Tools } from "../src/extension";
import type { TaskCore } from "../src/task-core";
import type { TaskSnapshot } from "../src/task-protocol";
import { registerAgentTaskTools as registerV1Tools } from "../src/legacy-extension";
import type { TaskStatus, WolfpackGatewayClient } from "../src/gateway-client";

interface WaitTool {
	execute(
		id: string,
		parameters: Record<string, unknown>,
		signal: AbortSignal,
		update: ((result: unknown) => void) | undefined,
		context: unknown,
	): Promise<{ readonly details: unknown }>;
}

interface TrackedSignal {
	readonly controller: AbortController;
	readonly signal: AbortSignal;
	readonly additions: () => number;
	readonly removals: () => number;
	readonly listenerCount: () => number;
}

describe("task wait abort listeners", () => {
	test("v2 removes listeners after repeated polling and cancellation", async () => {
		let statusChecks = 0;
		const completedSignal = trackAbortListeners();
		const completedWait = v2WaitTool({
			getTask: () => v2Task(++statusChecks >= 3 ? "completed" : "active"),
		} as unknown as TaskCore);

		const completed = await completedWait.execute("call", { taskId: "task-1", timeoutMs: 5_000 }, completedSignal.signal, undefined, {});

		expect(completed.details).toMatchObject({ taskId: "task-1", status: "completed" });
		expect(completedSignal.additions()).toBe(2);
		expect(completedSignal.removals()).toBe(2);
		expect(completedSignal.listenerCount()).toBe(0);

		const cancelledSignal = trackAbortListeners();
		const cancelledWait = v2WaitTool({ getTask: () => v2Task("active") } as unknown as TaskCore);
		setTimeout(() => cancelledSignal.controller.abort(), 10);

		const cancelled = await cancelledWait.execute("call", { taskId: "task-1", timeoutMs: 5_000 }, cancelledSignal.signal, undefined, {});

		expect(cancelled.details).toEqual({ error: { code: "TASK_ERROR", message: "task wait was cancelled", retryable: true } });
		expect(cancelledSignal.additions()).toBe(1);
		expect(cancelledSignal.removals()).toBe(1);
		expect(cancelledSignal.listenerCount()).toBe(0);
	});

	test("v1 removes listeners after repeated polling and cancellation", async () => {
		let statusChecks = 0;
		const completedSignal = trackAbortListeners();
		const completedWait = v1WaitTool({
			status: async () => v1Task(++statusChecks >= 3 ? "completed" : "active"),
		} as unknown as WolfpackGatewayClient);

		const completed = await completedWait.execute("call", { taskId: "task-1", timeoutMs: 5_000 }, completedSignal.signal, undefined, {});

		expect(completed.details).toMatchObject({ task: { taskId: "task-1" }, status: "completed" });
		expect(completedSignal.additions()).toBe(2);
		expect(completedSignal.removals()).toBe(2);
		expect(completedSignal.listenerCount()).toBe(0);

		const cancelledSignal = trackAbortListeners();
		const cancelledWait = v1WaitTool({ status: async () => v1Task("active") } as unknown as WolfpackGatewayClient);
		setTimeout(() => cancelledSignal.controller.abort(), 10);

		const cancelled = await cancelledWait.execute("call", { taskId: "task-1", timeoutMs: 5_000 }, cancelledSignal.signal, undefined, {});

		expect(cancelled.details).toEqual({ error: { code: "ABORTED", message: "task wait was cancelled", retryable: true } });
		expect(cancelledSignal.additions()).toBe(1);
		expect(cancelledSignal.removals()).toBe(1);
		expect(cancelledSignal.listenerCount()).toBe(0);
	});
});

function v2WaitTool(core: TaskCore): WaitTool {
	const tools: Record<string, WaitTool> = {};
	registerV2Tools({
		on: () => undefined,
		registerTool(tool: unknown) { const registered = tool as WaitTool & { readonly name: string }; tools[registered.name] = registered; },
	} as unknown as ExtensionAPI, core);
	return tools.agent_task_wait!;
}

function v1WaitTool(client: WolfpackGatewayClient): WaitTool {
	const tools: Record<string, WaitTool> = {};
	registerV1Tools({
		on: () => undefined,
		registerTool(tool: unknown) { const registered = tool as WaitTool & { readonly name: string }; tools[registered.name] = registered; },
	} as unknown as ExtensionAPI, client);
	return tools.agent_task_wait!;
}

function trackAbortListeners(): TrackedSignal {
	const controller = new AbortController();
	const signal = controller.signal;
	const listeners = new Set<Parameters<AbortSignal["addEventListener"]>[1]>();
	let additions = 0;
	let removals = 0;
	const addEventListener = signal.addEventListener.bind(signal);
	const removeEventListener = signal.removeEventListener.bind(signal);
	Object.defineProperties(signal, {
		addEventListener: {
			value(...args: Parameters<AbortSignal["addEventListener"]>): void {
				const [type, listener] = args;
				if (type === "abort") { additions += 1; listeners.add(listener); }
				addEventListener(...args);
			},
		},
		removeEventListener: {
			value(...args: Parameters<AbortSignal["removeEventListener"]>): void {
				const [type, listener] = args;
				if (type === "abort") { removals += 1; listeners.delete(listener); }
				removeEventListener(...args);
			},
		},
	});
	return { controller, signal, additions: () => additions, removals: () => removals, listenerCount: () => listeners.size };
}

function v2Task(status: TaskSnapshot["status"]): TaskSnapshot {
	const endpoint = { relay: "memory", id: "parent" };
	return {
		taskId: "task-1",
		protocolVersion: "pi-tasks/v2",
		origin: endpoint,
		target: endpoint,
		task: "wait",
		createdAt: 0,
		expiresAt: 5_000,
		status,
		events: [],
		terminalDelivery: { state: "not_submitted" },
	};
}

function v1Task(status: string): TaskStatus {
	const address = { machine: "local", sessionId: "parent" };
	return {
		task: {
			taskId: "task-1",
			source: address,
			target: address,
			task: "wait",
			createdAt: "2026-08-03T00:00:00.000Z",
			expiresAt: "2026-08-03T01:00:00.000Z",
		},
		status,
		events: [],
		warnings: [],
	};
}
