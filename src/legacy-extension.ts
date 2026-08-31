import { StringEnum } from "@earendil-works/pi-ai";
import type { AgentToolResult, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { Type } from "typebox";

import { abortableSleep } from "./abortable-sleep";
import {
	DEFAULT_TASK_TIMEOUT_MS,
	GatewayClientError,
	MAX_TASK_TIMEOUT_MS,
	MIN_TASK_TIMEOUT_MS,
	createWolfpackGatewayClient,
	type TaskCompletionInput,
	type TaskStatus,
	type WolfpackGatewayClient,
} from "./gateway-client";
import { deliverTaskInbox, restoredInboxCursor } from "./legacy-task-inbox";

const BACKGROUND_POLL_MS = 5_000;
const WAIT_POLL_MS = 250;
const SUMMARY_MAX_CHARS = 1_200;

const AddressParams = Type.Object({
	machine: Type.String({ minLength: 1, description: "local or a canonical Wolfpack machine identity" }),
	sessionId: Type.String({ minLength: 1, description: "Stable opaque Wolfpack broker session id" }),
});

const ContextRefParams = Type.Object({
	path: Type.String({ minLength: 1 }),
	selector: Type.Optional(Type.String()),
	purpose: Type.Optional(Type.String()),
});

const SendParams = Type.Object({
	to: AddressParams,
	task: Type.String({ minLength: 1, maxLength: 16 * 1024 }),
	context: Type.Optional(Type.Object({
		summary: Type.Optional(Type.String({ minLength: 1, maxLength: 16 * 1024 })),
		refs: Type.Optional(Type.Array(ContextRefParams)),
	})),
	role: Type.Optional(Type.String()),
	preflight: Type.Optional(Type.Object({ requiredProject: Type.Optional(Type.String()) })),
	metadata: Type.Optional(Type.Object({
		phaseId: Type.Optional(Type.String()),
		issueId: Type.Optional(Type.String()),
		verificationTier: Type.Optional(Type.String()),
		rootCause: Type.Optional(Type.String()),
	})),
	onCompletePrompt: Type.Optional(Type.String({ maxLength: 4_000 })),
	timeoutMs: Type.Optional(Type.Integer({ minimum: MIN_TASK_TIMEOUT_MS, maximum: MAX_TASK_TIMEOUT_MS })),
	idempotencyKey: Type.Optional(Type.String({ minLength: 1 })),
});

const TaskIdParams = Type.Object({ taskId: Type.String({ minLength: 1 }) });
const WaitParams = Type.Object({ taskId: Type.String({ minLength: 1 }), timeoutMs: Type.Optional(Type.Integer({ minimum: MIN_TASK_TIMEOUT_MS, maximum: MAX_TASK_TIMEOUT_MS })) });
const InboxParams = Type.Object({ includeAcknowledged: Type.Optional(Type.Boolean({ default: false })) });
const MessageParams = Type.Object({
	taskId: Type.String({ minLength: 1 }),
	type: StringEnum(["question", "answer", "information"] as const),
	message: Type.String({ minLength: 1, maxLength: 16 * 1024 }),
	replyToMessageId: Type.Optional(Type.String({ minLength: 1 })),
});
const DoneParams = Type.Object({
	taskId: Type.String({ minLength: 1 }),
	status: StringEnum(["completed", "failed", "cancelled"] as const),
	summary: Type.String({ minLength: 1, maxLength: SUMMARY_MAX_CHARS }),
	result: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
	error: Type.Optional(Type.Object({ code: Type.String(), message: Type.String(), retryable: Type.Boolean() })),
	artifacts: Type.Optional(Type.Array(Type.Object({ path: Type.String({ minLength: 1, description: "artifact paths must name receiver-project-relative regular files to inspect; artifacts are not a changed-file list." }), mimeType: Type.Optional(Type.String()), description: Type.Optional(Type.String()) }))),
});

export function registerAgentTaskTools(pi: ExtensionAPI, client: WolfpackGatewayClient = createWolfpackGatewayClient()): void {
	let backgroundTimer: ReturnType<typeof setInterval> | undefined;
	let inboxContext: ExtensionContext | undefined;
	const refreshInbox = createSingleFlightInboxRefresh(async () => {
		if (!inboxContext) return;
		const cursor = restoredInboxCursor(inboxContext.sessionManager.getEntries());
		await deliverTaskInbox(pi, client, inboxContext, cursor);
	});

	pi.on("session_start", async (_event, ctx) => {
		inboxContext = ctx;
		ctx.ui.setStatus("wolfpack-tasks", ctx.ui.theme.fg("dim", `tasks: ${client.callerSession}`));
		backgroundTimer = setInterval(() => {
			void refreshInbox().catch(() => ctx.ui.setStatus("wolfpack-tasks", ctx.ui.theme.fg("warning", "tasks: unavailable")));
		}, BACKGROUND_POLL_MS);
		await refreshInbox().catch(() => undefined);
	});
	pi.on("agent_end", async (_event, ctx) => {
		inboxContext = ctx;
		setTimeout(() => { void refreshInbox().catch(() => undefined); }, 0);
	});
	pi.on("session_shutdown", () => {
		if (backgroundTimer) clearInterval(backgroundTimer);
		backgroundTimer = undefined;
		inboxContext = undefined;
	});

	pi.registerTool({
		name: "agent_task_send", label: "Send Agent Task", description: "Durably send a bounded task to an existing Wolfpack Pi session.",
		promptSnippet: "Send nonblocking structured tasks through the local Wolfpack gateway",
		promptGuidelines: ["Use agent_task_send for normal nonblocking delegation; do not wait unless the user explicitly asks."],
		parameters: SendParams,
		async execute(_toolCallId, params, signal) {
			try {
				const receipt = await client.send(params, signal);
				return toolResult(receipt, `## task accepted\n- task: \`${receipt.taskId}\`\n- target: \`${params.to.machine}/${params.to.sessionId}\`\n- delivery: pending adapter insertion (confirmed by \`task.delivered\`)\n${warnings(receipt.warnings)}`);
			} catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_status", label: "Task Status", description: "Read compact structured task status from Wolfpack.", parameters: TaskIdParams,
		async execute(_toolCallId, params, signal) {
			try { const status = await client.status(params.taskId, signal); return statusResult(status); } catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_wait", label: "Wait Agent Task", description: "Block only when explicitly invoked until a task reaches a terminal state or this wait times out.", parameters: WaitParams,
		async execute(_toolCallId, params, signal, onUpdate) {
			const deadline = Date.now() + (params.timeoutMs ?? DEFAULT_TASK_TIMEOUT_MS);
			try {
				for (;;) {
					throwIfAborted(signal);
					const status = await client.status(params.taskId, signal);
					if (terminal(status.status)) return statusResult(status);
					if (Date.now() >= deadline) return toolResult({ taskId: params.taskId, status: status.status, error: { code: "WAIT_TIMEOUT", retryable: true } }, `## task wait\n- task: \`${params.taskId}\`\n- status: ${status.status}\n- wait timed out`);
					onUpdate?.({ content: [{ type: "text", text: `waiting for ${params.taskId}...` }], details: {} });
					await abortableSleep(Math.min(WAIT_POLL_MS, Math.max(1, deadline - Date.now())), signal, () => new GatewayClientError("ABORTED", "task wait was cancelled", true));
				}
			} catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_inbox", label: "Task Inbox", description: "Read inbox task events without acknowledging them.", parameters: InboxParams,
		async execute(_toolCallId, params, signal) {
			try {
				let cursor = "0";
				const taskIds = new Set<string>();
				for (;;) {
					const page = await client.inbox(cursor, params.includeAcknowledged ?? false, signal);
					for (const event of page.events) taskIds.add(event.taskId);
					if (!page.hasMore) break;
					cursor = page.nextCursor;
				}
				const tasks = await Promise.all([...taskIds].map((taskId) => client.status(taskId, signal)));
				return toolResult({ tasks }, `## task inbox\n${tasks.length === 0 ? "- empty" : tasks.map((task) => `- \`${task.task.taskId}\`: ${task.status}${task.completion ? ` — ${task.completion.summary}` : ""}`).join("\n")}`);
			} catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_ack", label: "Acknowledge Agent Task", description: "Acknowledge one independently verified terminal task.", parameters: TaskIdParams,
		async execute(_toolCallId, params, signal) {
			try { return toolResult(await client.ack(params.taskId, signal), `## task acknowledged\n- task: \`${params.taskId}\``); } catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_message", label: "Message Agent Task", description: "Durably send a question, answer, or information event for a task.", parameters: MessageParams,
		async execute(_toolCallId, params, signal) {
			try {
				const [identity, status] = await Promise.all([client.sessionStatus(signal), client.status(params.taskId, signal)]);
				const receipt = await client.message(params, signal);
				const receiverQuestion = params.type === "question" && identity.sessionId === status.task.target.sessionId;
				return { ...toolResult(receipt, `## task ${params.type}\n- task: \`${params.taskId}\`\n${params.message}`), ...(receiverQuestion ? { terminate: true } : {}) };
			} catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_cancel", label: "Cancel Agent Task", description: "Request cancellation for a non-terminal task through Wolfpack.", parameters: TaskIdParams,
		async execute(_toolCallId, params, signal) {
			try { const receipt = await client.cancel(params.taskId, signal); return toolResult(receipt, `## task cancellation requested\n- task: \`${params.taskId}\`${warnings(receipt.warnings)}`); } catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});

	pi.registerTool({
		name: "agent_task_done", label: "Complete Agent Task", description: "Finish an assigned task with a structured terminal result. Report source modifications in result.changedFiles; artifacts are receiver-project-relative regular files to inspect, not a changed-file list. Use exactly once as the final action.",
		promptSnippet: "Complete an assigned task: report changed files in result.changedFiles and artifacts separately", promptGuidelines: ["Report source modifications under result.changedFiles. Reserve artifacts for receiver-project-relative regular files a parent should inspect, not changed-file lists.", "Use agent_task_done exactly once as the final action for an assigned task."], parameters: DoneParams,
		async execute(_toolCallId, params, signal) {
			try {
				const result: TaskCompletionInput = { summary: params.summary, ...(params.result && { result: params.result }), ...(params.error && { error: params.error }), ...(params.artifacts && { artifacts: params.artifacts }) };
				const receipt = await client.complete({ taskId: params.taskId, status: params.status, result }, signal);
				return { ...toolResult(receipt, `## task ${params.status}\n- task: \`${params.taskId}\`\n- ${params.summary}${warnings(receipt.warnings)}`), terminate: true };
			} catch (error) { return clientError(error); }
		}, renderResult(result, _options, theme) { return new Text(theme.fg("accent", text(result))); },
	});
}

export function createSingleFlightInboxRefresh(refresh: () => Promise<void>): () => Promise<void> {
	let inFlight: Promise<void> | undefined;
	return (): Promise<void> => {
		if (inFlight) return inFlight;
		const current = refresh().finally(() => {
			if (inFlight === current) inFlight = undefined;
		});
		inFlight = current;
		return current;
	};
}

export default function piTasks(pi: ExtensionAPI): void {
	registerAgentTaskTools(pi);
}

function statusResult(status: TaskStatus): AgentToolResult<unknown> {
	return toolResult(status, `## task status\n- task: \`${status.task.taskId}\`\n- status: ${status.status}${status.completion ? `\n- summary: ${status.completion.summary}` : ""}${warnings(status.warnings)}`);
}

function toolResult(details: unknown, markdown: string): AgentToolResult<unknown> {
	return { content: [{ type: "text", text: markdown }], details };
}

function clientError(error: unknown): AgentToolResult<unknown> {
	const gateway = error instanceof GatewayClientError ? error : new GatewayClientError("CLIENT_ERROR", error instanceof Error ? error.message : "task gateway request failed", true);
	const path = gateway.path === undefined ? {} : { path: gateway.path };
	return toolResult(
		{ error: { code: gateway.code, message: gateway.message, retryable: gateway.retryable, ...path } },
		`## task gateway error\n- ${gateway.code}: ${gateway.message}${gateway.path === undefined ? "" : `\n- path: \`${gateway.path}\``}`,
	);
}

function warnings(values: readonly { readonly code: string; readonly message: string }[]): string {
	return values.length === 0 ? "" : `\n## gateway warnings\n${values.map((value) => `- ${value.code}: ${value.message}`).join("\n")}`;
}

function terminal(status: string): boolean {
	return status === "completed" || status === "failed" || status === "cancelled" || status === "timed_out";
}

function text(result: { readonly content?: readonly unknown[] }): string {
	const first = result.content?.[0];
	return first && typeof first === "object" && "type" in first && first.type === "text" && "text" in first && typeof first.text === "string" ? first.text : "";
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (signal?.aborted) throw new GatewayClientError("ABORTED", "task wait was cancelled", true);
}
