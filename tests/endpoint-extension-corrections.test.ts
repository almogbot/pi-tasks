import { expect, test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

import piTasks, { createConfiguredCoreLoader, createSingleFlightInboxRefresh, registerAgentTaskTools } from "../src/extension";
import type { TaskCore } from "../src/task-core";
import { TASK_PROTOCOL_VERSION, TaskEnvelopeKind, TaskOutboxDeliveryError, TaskProtocolError } from "../src/task-protocol";

interface Tool {
	readonly name: string;
	execute(id: string, parameters: Record<string, unknown>, signal: AbortSignal, update: undefined, context: unknown): Promise<{ readonly content: readonly { readonly text: string }[]; readonly details: unknown }>;
}

test("default extension uses the configured Wolfpack relay adapter rather than an in-memory or missing relay", async () => {
	const previousSession = process.env.WOLFPACK_SESSION_NAME;
	delete process.env.WOLFPACK_SESSION_NAME;
	const tools: Record<string, Tool> = {};
	piTasks({ on: () => undefined, registerCommand: () => undefined, registerTool(tool: unknown) { const value = tool as Tool; tools[value.name] = value; } } as unknown as ExtensionAPI);

	const result = await tools.agent_task_send!.execute("call", { to: { relay: "wolfpack-pi-tasks-v2", id: "opaque" }, task: "implement" }, new AbortController().signal, undefined, {});

	if (previousSession === undefined) delete process.env.WOLFPACK_SESSION_NAME;
	else process.env.WOLFPACK_SESSION_NAME = previousSession;
	expect(result.content[0]?.text).toContain("WOLFPACK_SESSION_NAME is required");
});

test("returns structured relay metadata and the persisted task id after assignment delivery fails", async () => {
	const tools: Record<string, Tool> = {};
	const core = {
		async createTask(): Promise<never> {
			throw new TaskProtocolError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false, details: { taskId: "persisted-task" } });
		},
	} as unknown as TaskCore;
	registerAgentTaskTools({
		on(): void { undefined; },
		registerTool(tool: unknown): void { const value = tool as Tool; tools[value.name] = value; },
	} as unknown as ExtensionAPI, core);

	const result = await tools.agent_task_send!.execute("call", { to: { relay: "wolfpack-pi-tasks-v2", id: "inactive" }, task: "persist first" }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({
		taskId: "persisted-task",
		error: { code: "TARGET_NOT_REGISTERED", message: "target is inactive", retryable: false },
	});
});

test("reports canonical origin cancellation with a structured blocked-delivery warning", async () => {
	const tools: Record<string, Tool> = {};
	const origin = { relay: "memory", id: "origin" } as const;
	const target = { relay: "memory", id: "receiver" } as const;
	const core = {
		endpoint: origin,
		async submitIntent(): Promise<never> {
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "target is inactive", {
				retryable: false,
				details: { taskId: "task-1", envelopeId: "cancel-envelope", target, blockedAt: 1, targetId: target.id },
			});
		},
		getTask(taskId: string) {
			return { taskId, origin, target, status: "cancelled" };
		},
	} as unknown as TaskCore;
	registerAgentTaskTools({
		on(): void { undefined; },
		registerTool(tool: unknown): void { const value = tool as Tool; tools[value.name] = value; },
	} as unknown as ExtensionAPI, core);

	const result = await tools.agent_task_cancel!.execute("call", { taskId: "task-1" }, new AbortController().signal, undefined, {});

	expect(result.details).toEqual({
		taskId: "task-1",
		status: "cancelled",
		warnings: [{
			code: "TARGET_NOT_REGISTERED",
			message: "target is inactive",
			retryable: false,
			delivery: "blocked",
			target,
			details: { targetId: target.id },
		}],
	});
	expect(result.content[0]?.text).toContain("canonical status: cancelled");
	expect(result.content[0]?.text).toContain("receiver incorporation not confirmed");
});

test("continues timeout and inbox processing while reporting degraded outbox delivery", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const calls: string[] = [];
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { calls.push("connect"); },
		async flushOutbox(): Promise<void> {
			calls.push("flush");
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "target is inactive", { retryable: false });
		},
		async evaluateTimeouts(): Promise<void> { calls.push("timeout"); },
		async receive(): Promise<readonly []> { calls.push("receive"); return []; },
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(calls).toEqual(["connect", "flush", "timeout", "receive"]);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
	} finally {
		sessionShutdown?.();
	}
});

test("continues inbox receive and reports degradation when timeout delivery fails", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const calls: string[] = [];
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { calls.push("connect"); },
		async flushOutbox(): Promise<void> { calls.push("flush"); },
		async evaluateTimeouts(): Promise<void> {
			calls.push("timeout");
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "timed-out task target is inactive", { retryable: false });
		},
		async receive(): Promise<readonly []> { calls.push("receive"); return []; },
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(calls).toEqual(["connect", "flush", "timeout", "receive"]);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
	} finally {
		sessionShutdown?.();
	}
});

test("reports receive-created delivery failures as outbox degradation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive(): Promise<readonly []> {
			throw new TaskOutboxDeliveryError("TARGET_NOT_REGISTERED", "canonical event target is inactive", { retryable: false });
		},
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: outbox degraded");
	} finally {
		sessionShutdown?.();
	}
});

test("does not mislabel relay failures during outbox delivery as degradation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> {
			throw new TaskOutboxDeliveryError("RELAY_UNAVAILABLE", "relay is unavailable");
		},
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive(): Promise<readonly []> { return []; },
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => true,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");
	} finally {
		sessionShutdown?.();
	}
});

test("does not mislabel relay receive failures as outbox degradation", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive(): Promise<readonly []> {
			throw new TaskProtocolError("TARGET_NOT_REGISTERED", "relay receive registration is absent", { retryable: false });
		},
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");
	} finally {
		sessionShutdown?.();
	}
});

test("starts polling after rejected startup and clears the warning after autonomous recovery", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let poll: (() => void) | undefined;
	let attempts = 0;
	let recovered: (() => void) | undefined;
	const recovery = new Promise<void>((resolve) => { recovered = resolve; });
	const statuses: Array<string | undefined> = [];
	const originalSetInterval = globalThis.setInterval;
	globalThis.setInterval = ((handler: (...args: unknown[]) => void) => {
		poll = (): void => handler();
		return 1 as unknown as ReturnType<typeof setInterval>;
	}) as typeof setInterval;
	const healthyCore = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive(): Promise<readonly []> { return []; },
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => {
				statuses.push(value);
				if (value === undefined) recovered?.();
			},
			theme: { fg: (_color: string, text: string): string => text },
		},
	};

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
		} as unknown as ExtensionAPI, undefined, async () => {
			attempts += 1;
			if (attempts === 1) throw new Error("transient relay failure");
			return healthyCore;
		});

		await sessionStart!({}, context);
		expect(statuses).toEqual(["tasks: relay unavailable"]);
		expect(poll).toBeDefined();
		poll?.();
		await recovery;
		await Promise.resolve();
		expect(attempts).toBe(2);
		expect(statuses.at(-1)).toBeUndefined();
	} finally {
		sessionShutdown?.();
		globalThis.setInterval = originalSetInterval;
	}
});

test("renews registration while pending Pi messages prevent inbox receive", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let poll: (() => void) | undefined;
	let connects = 0;
	let refreshes = 0;
	let polled: (() => void) | undefined;
	const pollCompleted = new Promise<void>((resolve) => { polled = resolve; });
	const originalSetInterval = globalThis.setInterval;
	globalThis.setInterval = ((handler: (...args: unknown[]) => void) => {
		poll = (): void => handler();
		return 1 as unknown as ReturnType<typeof setInterval>;
	}) as typeof setInterval;
	const core = {
		async connect(): Promise<void> { connects += 1; },
		async flushOutbox(): Promise<void> {
			refreshes += 1;
			if (refreshes === 2) polled?.();
		},
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive(): Promise<readonly []> { throw new Error("receive must remain gated"); },
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => true,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
		} as unknown as ExtensionAPI, core);

		await sessionStart!({}, context);
		poll?.();
		await Promise.race([pollCompleted, Bun.sleep(100).then(() => { throw new Error("background poll did not refresh"); })]);
		expect(connects).toBe(2);
	} finally {
		sessionShutdown?.();
		globalThis.setInterval = originalSetInterval;
	}
});

test("continues autonomous polling after Pi replaces the extension context at agent_end", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let poll: (() => void) | undefined;
	let refreshes = 0;
	let polled: (() => void) | undefined;
	const pollCompleted = new Promise<void>((resolve) => { polled = resolve; });
	const originalSetInterval = globalThis.setInterval;
	globalThis.setInterval = ((handler: (...args: unknown[]) => void) => {
		poll = (): void => handler();
		return 1 as unknown as ReturnType<typeof setInterval>;
	}) as typeof setInterval;
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> {
			refreshes += 1;
			if (refreshes === 3) polled?.();
		},
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive(): Promise<readonly []> { return []; },
	} as unknown as TaskCore;
	const context = () => ({
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	});

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
		} as unknown as ExtensionAPI, core);

		await sessionStart!({}, context());
		await agentEnd!({}, context());
		poll?.();
		await Promise.race([pollCompleted, Bun.sleep(100).then(() => { throw new Error("background poll did not refresh"); })]);
		expect(refreshes).toBe(3);
	} finally {
		sessionShutdown?.();
		globalThis.setInterval = originalSetInterval;
	}
});

test("keeps missing idle insertion evidence unacknowledged and retryable after pending delivery clears", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentSettled: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let pending = true;
	let insertionAttempts = 0;
	let recordedInsertions = 0;
	let acknowledgements = 0;
	let wakes = 0;
	const deliveryEvidence = new Set<string>();
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const delivery = {
		cursor: "1",
		envelope: {
			envelopeId: "event-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.created", sequence: "1", source, target, occurredAt: 1, payload: { task: "pending persistence" } }),
		},
	} as const;
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive() { return [delivery]; },
		async submitIntent(input: { readonly payload: { readonly stage: string; readonly state: string } }): Promise<void> { deliveryEvidence.add(`${input.payload.stage}:${input.payload.state}`); },
		async recordInsertion(): Promise<void> { recordedInsertions += 1; },
		async acknowledgeRelayDelivery(): Promise<void> { acknowledgements += 1; },
	} as unknown as TaskCore;
	const context = {
		isIdle: (): boolean => true,
		hasPendingMessages: (): boolean => pending,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_settled") agentSettled = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string }): void {
			if (message.customType === "pi-tasks-event") insertionAttempts += 1;
			if (message.customType === "pi-tasks-wake") wakes += 1;
		},
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(insertionAttempts).toBe(0);

		pending = false;
		await agentSettled!({}, context);
		await agentSettled!({}, context);
		expect(insertionAttempts).toBe(2);
		expect(recordedInsertions).toBe(0);
		expect(acknowledgements).toBe(0);
		expect(wakes).toBe(0);
		expect([...deliveryEvidence]).toEqual(["receiver_recorded:confirmed", "pi_insertion:blocked"]);
	} finally {
		sessionShutdown?.();
	}
});

test("persists an idle task event before sending one separate wake", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentSettled: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let idle = false;
	let acknowledgements = 0;
	let recordedInsertions = 0;
	const deliveryEvidence = new Set<string>();
	const entries: unknown[] = [];
	const sent: Array<{ readonly customType: string; readonly triggerTurn: boolean | undefined }> = [];
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const delivery = {
		cursor: "1",
		envelope: {
			envelopeId: "assignment-envelope", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
			payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.created", sequence: "1", source, target, occurredAt: 1, payload: { task: "implement safely" } }),
		},
	} as const;
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive() { return acknowledgements === 0 ? [delivery] : []; },
		async submitIntent(input: { readonly payload: { readonly stage: string; readonly state: string } }): Promise<void> { deliveryEvidence.add(`${input.payload.stage}:${input.payload.state}`); },
		async recordInsertion(): Promise<void> { recordedInsertions = 1; },
		async acknowledgeRelayDelivery(): Promise<void> { acknowledgements += 1; },
	} as unknown as TaskCore;
	const context = {
		isIdle: (): boolean => idle,
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => entries },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_settled") agentSettled = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
		sendMessage(message: { readonly customType: string; readonly details?: unknown }, options?: { readonly triggerTurn?: boolean }): void {
			sent.push({ customType: message.customType, triggerTurn: options?.triggerTurn });
			if (message.customType === "pi-tasks-event") entries.push({ type: "custom_message", customType: message.customType, details: message.details });
		},
		appendEntry(customType: string, data: unknown): void { entries.push({ type: "custom", customType, data }); },
	} as unknown as ExtensionAPI, core);

	try {
		await sessionStart!({}, context);
		expect(sent).toEqual([]);
		expect(acknowledgements).toBe(0);

		idle = true;
		await agentSettled!({}, context);
		expect(sent).toEqual([
			{ customType: "pi-tasks-event", triggerTurn: false },
			{ customType: "pi-tasks-wake", triggerTurn: true },
		]);
		expect(entries.filter((entry) => typeof entry === "object" && entry !== null && "customType" in entry && entry.customType === "pi-tasks-event")).toHaveLength(1);
		expect(recordedInsertions).toBe(1);
		expect(acknowledgements).toBe(0);

		entries.push({ type: "custom_message", customType: "pi-tasks-wake", details: { taskId: "task-1", eventId: "event-1" } });
		await agentSettled!({}, context);
		expect(sent).toHaveLength(2);
		expect(recordedInsertions).toBe(1);
		expect(acknowledgements).toBe(1);
		expect([...deliveryEvidence]).toEqual(["receiver_recorded:confirmed", "wake_requested:confirmed", "wake_accepted:confirmed"]);
	} finally {
		sessionShutdown?.();
	}
});

test("invalidates a pending lifecycle poll when the session shuts down", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	let poll: (() => void) | undefined;
	let receiveCalls = 0;
	let releasePoll: (() => void) | undefined;
	let pollStarted: (() => void) | undefined;
	const started = new Promise<void>((resolve) => { pollStarted = resolve; });
	const pendingPoll = new Promise<void>((resolve) => { releasePoll = resolve; });
	const statuses: Array<string | undefined> = [];
	let deliveries = 0;
	const originalSetInterval = globalThis.setInterval;
	globalThis.setInterval = ((handler: (...args: unknown[]) => void) => {
		poll = (): void => handler();
		return 1 as unknown as ReturnType<typeof setInterval>;
	}) as typeof setInterval;
	const source = { relay: "memory", id: "parent" };
	const target = { relay: "memory", id: "receiver" };
	const core = {
		async connect(): Promise<void> { undefined; },
		async flushOutbox(): Promise<void> { undefined; },
		async evaluateTimeouts(): Promise<void> { undefined; },
		async receive() {
			receiveCalls += 1;
			if (receiveCalls === 1) return [];
			pollStarted?.();
			await pendingPoll;
			return [{
				cursor: "1",
				envelope: {
					envelopeId: "late-event", protocolVersion: TASK_PROTOCOL_VERSION, source, target, taskId: "task-1", kind: TaskEnvelopeKind.canonicalEvent,
					payload: JSON.stringify({ eventId: "event-1", taskId: "task-1", type: "task.information", sequence: "1", source, target, occurredAt: 1, payload: { message: "late" } }),
				},
			}] as const;
		},
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};

	try {
		registerAgentTaskTools({
			on(event: string, handler: unknown): void {
				if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
				if (event === "session_shutdown") sessionShutdown = handler as () => void;
			},
			registerTool(): void { undefined; },
			sendMessage(): void { deliveries += 1; },
		} as unknown as ExtensionAPI, core);

		await sessionStart!({}, context);
		const statusesBeforeShutdown = [...statuses];
		poll?.();
		await started;
		const joinedRefresh = agentEnd!({}, context);
		sessionShutdown?.();
		releasePoll?.();
		await expect(joinedRefresh).resolves.toBeUndefined();
		expect(deliveries).toBe(0);
		expect(statuses).toEqual(statusesBeforeShutdown);
	} finally {
		sessionShutdown?.();
		globalThis.setInterval = originalSetInterval;
	}
});

test("contains agent_end relay failures and leaves the unavailable warning", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let sessionShutdown: (() => void) | undefined;
	const statuses: Array<string | undefined> = [];
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: {
			setStatus: (_key: string, value: string | undefined): void => { statuses.push(value); },
			theme: { fg: (_color: string, text: string): string => text },
		},
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "session_shutdown") sessionShutdown = handler as () => void;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, undefined, async () => { throw new Error("relay unavailable"); });

	try {
		await sessionStart!({}, context);
		await expect(agentEnd!({}, context)).resolves.toBeUndefined();
		expect(statuses.at(-1)).toBe("tasks: relay unavailable");
	} finally {
		sessionShutdown?.();
	}
});

test("retries a rejected configured core during a later lifecycle inbox refresh", async () => {
	let sessionStart: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let agentEnd: ((event: unknown, context: unknown) => Promise<unknown>) | undefined;
	let attempts = 0;
	const calls: string[] = [];
	const healthyCore = {
		async connect(): Promise<void> { calls.push("connect"); },
		async flushOutbox(): Promise<void> { calls.push("flush"); },
		async evaluateTimeouts(): Promise<void> { calls.push("timeout"); },
		async receive(): Promise<readonly []> { calls.push("receive"); return []; },
	} as unknown as TaskCore;
	const context = {
		hasPendingMessages: (): boolean => false,
		sessionManager: { getEntries: (): readonly unknown[] => [] },
		ui: { setStatus: (): void => undefined, theme: { fg: (_color: string, text: string): string => text } },
	};
	registerAgentTaskTools({
		on(event: string, handler: unknown): void {
			if (event === "session_start") sessionStart = handler as (event: unknown, context: unknown) => Promise<unknown>;
			if (event === "agent_end") agentEnd = handler as (event: unknown, context: unknown) => Promise<unknown>;
		},
		registerTool(): void { undefined; },
	} as unknown as ExtensionAPI, undefined, async () => {
		attempts += 1;
		if (attempts === 1) throw new Error("transient relay failure");
		return healthyCore;
	});

	await sessionStart!({}, context);
	await agentEnd!({}, context);

	expect(attempts).toBe(2);
	expect(calls).toEqual(["connect", "flush", "timeout", "receive"]);
});

test("clears a rejected configured core before retrying it", async () => {
	let attempts = 0;
	const core = {} as TaskCore;
	const configuredCore = createConfiguredCoreLoader(async () => {
		attempts += 1;
		if (attempts === 1) throw new Error("transient relay failure");
		return core;
	});

	await expect(configuredCore()).rejects.toThrow("transient relay failure");
	await expect(configuredCore()).resolves.toBe(core);
	expect(attempts).toBe(2);
});

test("serializes concurrent inbox refreshes and accepts a new refresh only after the prior lifecycle completes", async () => {
	let calls = 0;
	let release: (() => void) | undefined;
	const refresh = createSingleFlightInboxRefresh(async () => {
		calls += 1;
		await new Promise<void>((resolve) => { release = resolve; });
	});

	const first = refresh();
	const second = refresh();
	expect(second).toBe(first);
	expect(calls).toBe(1);
	release?.();
	await first;

	const third = refresh();
	expect(third).not.toBe(first);
	expect(calls).toBe(2);
	release?.();
	await third;
});
