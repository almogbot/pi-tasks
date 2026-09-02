import { StringEnum } from "@earendil-works/pi-ai";
import type { AgentToolResult, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { Type } from "typebox";

import { abortableSleep } from "./abortable-sleep";
import { deliverTaskInbox, incorporatedTaskEvents } from "./task-inbox";
import { TaskDeliveryEvidenceState, TaskDeliveryStage, TaskOutboxDeliveryError, TaskProtocolError } from "./task-protocol";
import { createWolfpackTaskCore } from "./wolfpack-task-relay";
import type { SubmitIntentInput, SubmitIntentOutcome, TaskCore } from "./task-core";
import type { TaskEndpoint, TaskSnapshot } from "./task-protocol";

const DEFAULT_TASK_TIMEOUT_MS = 30 * 60 * 1_000;
const MIN_TASK_TIMEOUT_MS = 1_000;
const MAX_TASK_TIMEOUT_MS = 86_400_000;
const BACKGROUND_POLL_MS = 5_000;
const TARGET_NOT_REGISTERED_CODE = "TARGET_NOT_REGISTERED";
const WAIT_POLL_MS = 250;
const SUMMARY_MAX_CHARS = 1_200;
const PRE_ASSIGNMENT_TOOLS = new Set(["agent_task_inbox", "agent_task_status", "agent_task_wait"]);
const COORDINATOR_ONLY_TASK_TOOLS = new Set(["agent_task_send", "agent_task_cancel", "agent_task_ack"]);
export const WORKER_GATE_DENIAL_CODE = "PI_TASK_WORKER_ASSIGNMENT_REQUIRED";
export const WORKER_COORDINATION_FORBIDDEN_CODE = "PI_TASK_WORKER_COORDINATION_FORBIDDEN";

const EndpointParams = Type.Object({
	relay: Type.String({ minLength: 1, description: "relay identifier" }),
	id: Type.String({ minLength: 1, description: "opaque endpoint identifier" }),
});
const SendParams = Type.Object({
	to: EndpointParams,
	task: Type.String({ minLength: 1, maxLength: 16 * 1024 }),
	timeoutMs: Type.Optional(Type.Integer({ minimum: MIN_TASK_TIMEOUT_MS, maximum: MAX_TASK_TIMEOUT_MS })),
});
const TaskIdParams = Type.Object({ taskId: Type.String({ minLength: 1 }) });
const WaitParams = Type.Object({ taskId: Type.String({ minLength: 1 }), timeoutMs: Type.Optional(Type.Integer({ minimum: MIN_TASK_TIMEOUT_MS, maximum: MAX_TASK_TIMEOUT_MS })) });
const MessageParams = Type.Object({ taskId: Type.String({ minLength: 1 }), type: StringEnum(["question", "answer", "information"] as const), message: Type.String({ minLength: 1, maxLength: 16 * 1024 }) });
const DoneParams = Type.Object({
	taskId: Type.String({ minLength: 1 }), status: StringEnum(["completed", "failed", "cancelled"] as const), summary: Type.String({ minLength: 1, maxLength: SUMMARY_MAX_CHARS }),
	result: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
	error: Type.Optional(Type.Object({ code: Type.String(), message: Type.String(), retryable: Type.Boolean() })),
	artifacts: Type.Optional(Type.Array(Type.Object({ path: Type.String({ minLength: 1, description: "artifact paths are receiver-project-relative regular files" }), mimeType: Type.Optional(Type.String()), description: Type.Optional(Type.String()) }))),
});

export type ConfiguredCoreFactory = (signal?: AbortSignal) => Promise<TaskCore>;

/** Reuses a successful configured core while allowing the next lifecycle operation to retry a failed setup. */
export function createConfiguredCoreLoader(factory: ConfiguredCoreFactory): ConfiguredCoreFactory {
	let corePromise: Promise<TaskCore> | undefined;
	return async (signal?: AbortSignal): Promise<TaskCore> => {
		if (corePromise) return corePromise;
		const candidate = Promise.resolve().then(() => factory(signal));
		corePromise = candidate;
		try {
			return await candidate;
		} catch (error) {
			if (corePromise === candidate) corePromise = undefined;
			throw error;
		}
	};
}

/** Registers v2 endpoint-owned tools using a configured durable Wolfpack relay by default. */
export function registerAgentTaskTools(pi: ExtensionAPI, core: TaskCore | undefined = undefined, createCore: ConfiguredCoreFactory = (signal) => createWolfpackTaskCore({}, signal)): void {
	let inboxContext: ExtensionContext | undefined;
	let backgroundTimer: ReturnType<typeof setInterval> | undefined;
	let lifecycleEpoch = 0;
	const closingTaskIds = new Set<string>();
	const workerGateEnabled = process.env.PI_TASK_WORKER === "1";
	const configuredCore: ConfiguredCoreFactory = core === undefined ? createConfiguredCoreLoader(createCore) : async (): Promise<TaskCore> => core;
	const refreshInbox = createSingleFlightInboxRefresh(async (signal) => {
		const context = inboxContext;
		if (!context) return undefined;
		const activeCore = await configuredCore(signal);
		if (inboxContext !== context) return undefined;
		let outboxError: unknown;
		try {
			await activeCore.flushOutbox(signal);
		} catch (error) {
			outboxError = error;
		}
		if (inboxContext !== context) return undefined;
		try {
			await activeCore.evaluateTimeouts(signal);
		} catch (error) {
			outboxError ??= error;
		}
		if (inboxContext !== context) return undefined;
		const isCurrent = (): boolean => inboxContext === context;
		const guardedCore: TaskCore = {
			...activeCore,
			async receive(receiveSignal) {
				const deliveries = await activeCore.receive(receiveSignal);
				return isCurrent() ? deliveries : [];
			},
			async acknowledgeRelayDelivery(cursor, acknowledgementSignal) {
				if (isCurrent()) await activeCore.acknowledgeRelayDelivery(cursor, acknowledgementSignal);
			},
			async recordInsertion(input, insertionSignal) {
				if (isCurrent()) await activeCore.recordInsertion(input, insertionSignal);
			},
		};
		await deliverTaskInbox({
			sendMessage(message, options) { if (isCurrent()) pi.sendMessage(message, options); },
			appendEntry(customType, data) { if (isCurrent()) pi.appendEntry(customType, data); },
		}, guardedCore, {
			isIdle: (): boolean => isCurrent() && context.isIdle(),
			hasPendingMessages: (): boolean => !isCurrent() || context.hasPendingMessages(),
			sessionManager: context.sessionManager,
		}, signal);
		return outboxError;
	});
	const refreshLifecycle = async (context: ExtensionContext, epoch: number): Promise<void> => {
		const isCurrent = (): boolean => lifecycleEpoch === epoch && inboxContext === context;
		try {
			const activeCore = await configuredCore();
			let connectionError: unknown;
			try {
				await activeCore.connect();
			} catch (error) {
				connectionError = error;
			}
			if (!isCurrent()) return;
			const outboxError = await refreshInbox();
			if (!isCurrent()) return;
			const lifecycleError = outboxError ?? connectionError;
			const status = lifecycleError === undefined ? undefined : outboxFailureStatus(lifecycleError);
			context.ui.setStatus("pi-tasks", status === undefined ? undefined : context.ui.theme.fg("warning", status));
		} catch (error) {
			if (isCurrent()) context.ui.setStatus("pi-tasks", context.ui.theme.fg("warning", outboxFailureStatus(error)));
		}
	};

	pi.on("tool_call", async (event, context) => {
		if (!workerGateEnabled || PRE_ASSIGNMENT_TOOLS.has(event.toolName)) return undefined;
		if (COORDINATOR_ONLY_TASK_TOOLS.has(event.toolName)) return { block: true, reason: WORKER_COORDINATION_FORBIDDEN_CODE };
		try {
			const activeCore = await configuredCore(context.signal);
			const assignments = assignedWorkerTasks(activeCore, context.sessionManager.getEntries());
			const taskId = inputTaskId(event.input);
			if (event.toolName === "agent_task_done") {
				const assigned = taskId === undefined ? undefined : assignments.find((task) => task.taskId === taskId);
				if (assigned === undefined || (!eligibleWorkerTask(assigned, closingTaskIds) && assigned.terminalDelivery.state === "not_submitted" && !closingTaskIds.has(assigned.taskId))) {
					return { block: true, reason: WORKER_GATE_DENIAL_CODE };
				}
				closingTaskIds.add(assigned.taskId);
				return undefined;
			}
			if (event.toolName === "agent_task_message") {
				const assigned = taskId === undefined ? undefined : assignments.find((task) => task.taskId === taskId);
				return assigned !== undefined && eligibleWorkerTask(assigned, closingTaskIds) ? undefined : { block: true, reason: WORKER_GATE_DENIAL_CODE };
			}
			if (assignments.some((task) => eligibleWorkerTask(task, closingTaskIds))) return undefined;
		} catch {
			// Authorization evidence unavailable or malformed: fail closed with the stable public reason.
		}
		return { block: true, reason: WORKER_GATE_DENIAL_CODE };
	});

	pi.on("session_start", async (_event, context) => {
		closingTaskIds.clear();
		const epoch = ++lifecycleEpoch;
		inboxContext = context;
		if (backgroundTimer) clearInterval(backgroundTimer);
		backgroundTimer = setInterval(() => {
			const currentContext = inboxContext;
			if (currentContext) void refreshLifecycle(currentContext, epoch);
		}, BACKGROUND_POLL_MS);
		await refreshLifecycle(context, epoch);
	});
	pi.on("agent_end", async (_event, context) => {
		const epoch = lifecycleEpoch;
		inboxContext = context;
		await refreshLifecycle(context, epoch);
	});
	pi.on("agent_settled", async (_event, context) => {
		const epoch = lifecycleEpoch;
		inboxContext = context;
		await refreshLifecycle(context, epoch);
	});
	pi.on("session_shutdown", () => {
		closingTaskIds.clear();
		lifecycleEpoch += 1;
		if (backgroundTimer) clearInterval(backgroundTimer);
		backgroundTimer = undefined;
		inboxContext = undefined;
	});

	pi.registerTool({
		name: "agent_task_send", label: "Send Agent Task", description: "Persist an endpoint-owned task and submit its opaque assignment envelope to a relay.", parameters: SendParams,
		async execute(_id, params, signal) {
			try {
				const sent = await (await configuredCore(signal)).createTask({ target: params.to, task: params.task, timeoutMs: params.timeoutMs ?? DEFAULT_TASK_TIMEOUT_MS }, signal);
				return toolResult(sent, `## task accepted\n- task: \`${sent.taskId}\`\n- target: \`${params.to.relay}/${params.to.id}\`\n- delivery: relay acceptance only`);
			} catch (error) { return taskError(error); }
		},
		renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_status", label: "Task Status", description: "Read local endpoint-owned task state; status is unavailable while its origin is offline.", parameters: TaskIdParams,
		async execute(_id, params, signal, _onUpdate, context) {
			try {
				const activeCore = await configuredCore(signal);
				const task = activeCore.getTask(params.taskId);
				if (!task) return taskError(new Error("unknown local task"));
				const deliveryEvidence = taskDeliveryEvidence(task);
				return toolResult({ ...task, deliveryEvidence }, `## task status\n- task: \`${task.taskId}\`\n- status: ${task.status}${receiverAssignment(activeCore, task, context)}${deliveryEvidenceText(deliveryEvidence)}`);
			} catch (error) { return taskError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_wait", label: "Wait Agent Task", description: "Wait for a locally-known endpoint-owned task to reach a terminal state.", parameters: WaitParams,
		async execute(_id, params, signal, onUpdate) {
			const deadline = Date.now() + (params.timeoutMs ?? DEFAULT_TASK_TIMEOUT_MS);
			try {
				for (;;) {
					await refreshInbox(signal);
					const task = (await configuredCore(signal)).getTask(params.taskId);
					if (!task) return taskError(new Error("unknown local task"));
					if (terminal(task.status)) return toolResult(task, `## task status\n- task: \`${task.taskId}\`\n- status: ${task.status}`);
					if (Date.now() >= deadline) return toolResult({ taskId: params.taskId, status: task.status }, `## task wait\n- task: \`${params.taskId}\`\n- status: ${task.status}\n- wait timed out`);
					onUpdate?.({ content: [{ type: "text", text: `waiting for ${params.taskId}...` }], details: {} });
					await abortableSleep(Math.max(1, Math.min(WAIT_POLL_MS, deadline - Date.now())), signal, () => new Error("task wait was cancelled"));
				}
			} catch (error) { return taskError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_inbox", label: "Task Inbox", description: "Read local task records after processing relay deliveries without acknowledging task lifecycle.", parameters: Type.Object({}),
		async execute(_id, _params, signal, _onUpdate, context) {
			try {
				await refreshInbox(signal);
				const activeCore = await configuredCore(signal);
				const tasks = activeCore.listTasks();
				return toolResult({ tasks }, `## task inbox\n${tasks.map((task) => `- \`${task.taskId}\`: ${task.status}${receiverAssignment(activeCore, task, context)}`).join("\n") || "- empty"}`);
			} catch (error) { return taskError(error); }
		},
		renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_message", label: "Message Agent Task", description: "Persist a message intent before relay submission.", parameters: MessageParams,
		async execute(_id, params, signal) { try { await (await configuredCore(signal)).submitIntent({ taskId: params.taskId, type: `task.${params.type}`, payload: { message: params.message } }, signal); return toolResult({ taskId: params.taskId }, `## task ${params.type}\n- task: \`${params.taskId}\``); } catch (error) { return taskError(error); } },
		renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_cancel", label: "Cancel Agent Task", description: "Persist cancellation before relay submission.", parameters: TaskIdParams,
		async execute(_id, params, signal) {
			try {
				const activeCore = await configuredCore(signal);
				try {
					await activeCore.submitIntent({ taskId: params.taskId, type: "task.cancelled", payload: {} }, signal);
					return toolResult({ taskId: params.taskId }, `## task cancellation requested\n- task: \`${params.taskId}\``);
				} catch (error) {
					return blockedCancellationResult(activeCore, params.taskId, error) ?? taskError(error);
				}
			} catch (error) { return taskError(error); }
		},
		renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_ack", label: "Acknowledge Agent Task", description: "Record parent acknowledgement as an origin-owned logical event.", parameters: TaskIdParams,
		async execute(_id, params, signal) { try { await (await configuredCore(signal)).acknowledgeParent(params.taskId, signal); return toolResult({ taskId: params.taskId }, `## task acknowledged\n- task: \`${params.taskId}\``); } catch (error) { return taskError(error); } },
		renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
	pi.registerTool({
		name: "agent_task_done", label: "Complete Agent Task", description: "Persist a terminal task intent before relay submission.", parameters: DoneParams,
		async execute(_id, params, signal) {
			try {
				const activeCore = await configuredCore(signal);
				let submission: SubmitIntentOutcome | undefined;
				try {
					const input: SubmitIntentInput = { taskId: params.taskId, type: `task.${params.status}`, payload: { summary: params.summary, ...(params.result === undefined ? {} : { result: params.result }), ...(params.error === undefined ? {} : { error: params.error }), ...(params.artifacts === undefined ? {} : { artifacts: params.artifacts }) } };
					if (activeCore.submitIntentWithOutcome) submission = await activeCore.submitIntentWithOutcome(input, signal);
					else await activeCore.submitIntent(input, signal);
				} catch (error) {
					return blockedDoneResult(activeCore, params.taskId, params.status, error) ?? taskError(error);
				}
				const task = activeCore.getTask(params.taskId);
				if (!task) return taskError(new Error("unknown local task"));
				if (submission?.authority === "origin" && submission.canonicalEvent.type === "task.late_terminal") {
					return {
						...toolResult({ taskId: params.taskId, requestedStatus: params.status, observedCanonicalStatus: task.status, canonicalEvent: submission.canonicalEvent }, `## canonical late terminal recorded\n- task: \`${params.taskId}\`\n- requested status: ${params.status}\n- canonical status remains: ${task.status}\n- ${params.summary}`),
						terminate: true,
					};
				}
				if (submission?.authority === "origin" && task.status === params.status) {
					return {
						...toolResult({ taskId: params.taskId, requestedStatus: params.status, observedCanonicalStatus: task.status, canonicalCompletion: { state: "confirmed", status: task.status } }, `## task ${task.status}\n- task: \`${params.taskId}\`\n- canonical status: ${task.status}\n- ${params.summary}`),
						terminate: true,
					};
				}
				const deliveryOutcome = task.terminalDelivery.state === "accepted" ? "accepted" : "recorded";
				const result = toolResult({ taskId: params.taskId, requestedStatus: params.status, observedCanonicalStatus: task.status, terminalDelivery: task.terminalDelivery }, `## terminal intent ${deliveryOutcome}\n- task: \`${params.taskId}\`\n- requested status: ${params.status}\n- observed canonical status: ${task.status}\n- ${params.summary}`);
				return task.terminalDelivery.state === "accepted" || terminal(task.status) ? { ...result, terminate: true } : result;
			} catch (error) { return taskError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
}

export function createSingleFlightInboxRefresh<TValue>(refresh: (signal?: AbortSignal) => Promise<TValue>): (signal?: AbortSignal) => Promise<TValue> {
	let inFlight: Promise<TValue> | undefined;
	return (signal): Promise<TValue> => {
		if (inFlight) return inFlight;
		const current = refresh(signal).finally(() => {
			if (inFlight === current) inFlight = undefined;
		});
		inFlight = current;
		return current;
	};
}

export default function piTasks(pi: ExtensionAPI): void {
	registerAgentTaskTools(pi);
}

interface TaskDeliveryEvidence {
	readonly receiverPersistence: "not_confirmed" | "confirmed";
	readonly piInsertion: "not_confirmed" | "blocked" | "confirmed";
	readonly wakeAcceptance: "not_confirmed" | "pending" | "confirmed";
	readonly modelExecution: "not_evidenced";
}

function taskDeliveryEvidence(task: TaskSnapshot): TaskDeliveryEvidence {
	let receiverPersistence: TaskDeliveryEvidence["receiverPersistence"] = "not_confirmed";
	let piInsertion: TaskDeliveryEvidence["piInsertion"] = "not_confirmed";
	let wakeAcceptance: TaskDeliveryEvidence["wakeAcceptance"] = "not_confirmed";
	const assignmentEventId = task.events.find((event) => event.type === "task.created")?.eventId;
	for (const event of task.events) {
		if (event.type !== "task.delivery_receipt" || event.payload.eventId !== assignmentEventId) continue;
		if (event.payload.stage === TaskDeliveryStage.receiverPersisted) receiverPersistence = "confirmed";
		if (event.payload.stage === TaskDeliveryStage.piInsertion && event.payload.state === TaskDeliveryEvidenceState.blocked) piInsertion = "blocked";
		if (event.payload.stage === TaskDeliveryStage.piInserted || event.payload.stage === undefined) {
			receiverPersistence = "confirmed";
			piInsertion = "confirmed";
		}
		if (event.payload.stage === TaskDeliveryStage.wakeRequested) wakeAcceptance = "pending";
		if (event.payload.stage === TaskDeliveryStage.wakeAccepted) wakeAcceptance = "confirmed";
	}
	return { receiverPersistence, piInsertion, wakeAcceptance, modelExecution: "not_evidenced" };
}

function deliveryEvidenceText(evidence: TaskDeliveryEvidence): string {
	const insertion = evidence.piInsertion === "blocked" ? "blocked; retryable" : evidence.piInsertion;
	return `\n- receiver persistence: ${evidence.receiverPersistence}\n- Pi insertion: ${insertion}\n- wake acceptance: ${evidence.wakeAcceptance}\n- model execution: ${evidence.modelExecution}`;
}

function toolResult(details: unknown, markdown: string): AgentToolResult<unknown> {
	return { content: [{ type: "text", text: markdown }], details };
}

function taskError(error: unknown): AgentToolResult<unknown> {
	const message = error instanceof Error ? error.message : "task operation failed";
	if (error instanceof TaskProtocolError) {
		return toolResult({ ...error.details, error: { code: error.code, message, retryable: error.retryable } }, `## task error\n- ${message}`);
	}
	return toolResult({ error: { code: "TASK_ERROR", message, retryable: true } }, `## task error\n- ${message}`);
}

function blockedCancellationResult(core: TaskCore, taskId: string, error: unknown): AgentToolResult<unknown> | undefined {
	if (!(error instanceof TaskOutboxDeliveryError) || error.details?.taskId !== taskId) return undefined;
	const task = core.getTask(taskId);
	const warning = task === undefined ? undefined : blockedDeliveryWarning(error, task.target);
	if (task?.status !== "cancelled" || !sameEndpoint(task.origin, core.endpoint) || warning === undefined) return undefined;
	return toolResult({ taskId, status: "cancelled", warnings: [warning] }, `## task cancelled\n- task: \`${taskId}\`\n- canonical status: cancelled\n- target delivery: blocked; receiver incorporation not confirmed`);
}

function blockedDoneResult(core: TaskCore, taskId: string, requestedStatus: string, error: unknown): AgentToolResult<unknown> | undefined {
	if (!(error instanceof TaskOutboxDeliveryError)) return undefined;
	const task = core.getTask(taskId);
	if (task === undefined) return undefined;
	if (!sameEndpoint(task.origin, core.endpoint) && task.terminalDelivery.state === "delivery_blocked" && task.terminalDelivery.intentType === `task.${requestedStatus}`) {
		return toolResult({
			taskId,
			status: task.status,
			terminalDelivery: task.terminalDelivery,
			error: { code: error.code, message: error.message, retryable: error.retryable },
		}, `## terminal intent delivery blocked\n- task: \`${taskId}\`\n- canonical status: ${task.status}\n- canonical completion not confirmed; retry remains allowed`);
	}
	const warning = blockedDeliveryWarning(error, task.target);
	if (!sameEndpoint(task.origin, core.endpoint) || task.status !== requestedStatus || warning === undefined) return undefined;
	return {
		...toolResult({ taskId, status: task.status, warnings: [warning] }, `## task ${task.status}\n- task: \`${taskId}\`\n- canonical status: ${task.status}\n- target delivery: blocked; receiver incorporation not confirmed`),
		terminate: true,
	};
}

function blockedDeliveryWarning(error: TaskOutboxDeliveryError, target: TaskEndpoint): Readonly<Record<string, unknown>> | undefined {
	if (error.code !== TARGET_NOT_REGISTERED_CODE || error.retryable) return undefined;
	const details = { ...error.details };
	delete details.taskId;
	delete details.envelopeId;
	delete details.target;
	delete details.blockedAt;
	return { code: error.code, message: error.message, retryable: false, delivery: "blocked", target, details };
}

function outboxFailureStatus(error: unknown): "tasks: relay unavailable" | "tasks: outbox degraded" {
	return error instanceof TaskOutboxDeliveryError && error.code === TARGET_NOT_REGISTERED_CODE ? "tasks: outbox degraded" : "tasks: relay unavailable";
}

function terminal(status: string): boolean {
	return ["completed", "failed", "cancelled", "timed_out"].includes(status);
}

function receiverAssignment(core: TaskCore, task: TaskSnapshot, context: Pick<ExtensionContext, "sessionManager">): string {
	if (task.status !== "active" || sameEndpoint(task.origin, core.endpoint) || !sameEndpoint(task.target, core.endpoint)) return "";
	const created = task.events.find((event) => event.type === "task.created");
	if (!created || !incorporatedTaskEvents(context.sessionManager.getEntries()).some((evidence) => evidence.taskId === task.taskId && evidence.eventId === created.eventId)) return "";
	return `\n\n## task assignment\n${task.task}`;
}

function assignedWorkerTasks(core: TaskCore, entries: readonly unknown[]): readonly TaskSnapshot[] {
	const assignments = new Map<string, TaskSnapshot>();
	for (const evidence of incorporatedTaskEvents(entries)) {
		const task = core.getTask(evidence.taskId);
		if (!task || !sameEndpoint(task.target, core.endpoint)) continue;
		const created = task.events.find((event) => event.eventId === evidence.eventId);
		if (!created || created.type !== "task.created" || !sameEndpoint(created.target, core.endpoint) || !sameEndpoint(created.source, task.origin)) continue;
		assignments.set(task.taskId, task);
	}
	return [...assignments.values()];
}

function eligibleWorkerTask(task: TaskSnapshot, closingTaskIds: ReadonlySet<string>): boolean {
	return task.status === "active" && task.terminalDelivery.state === "not_submitted" && !closingTaskIds.has(task.taskId);
}

function sameEndpoint(left: TaskEndpoint, right: TaskEndpoint): boolean {
	return left.relay === right.relay && left.id === right.id;
}

function inputTaskId(input: unknown): string | undefined {
	return isRecord(input) && typeof input.taskId === "string" ? input.taskId : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function text(result: { readonly content?: readonly unknown[] }): string {
	const first = result.content?.[0];
	return first && typeof first === "object" && "type" in first && first.type === "text" && "text" in first && typeof first.text === "string" ? first.text : "";
}
