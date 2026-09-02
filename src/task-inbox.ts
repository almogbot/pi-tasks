import type { TaskCore } from "./task-core";
import { TaskEnvelopeKind, TaskProtocolError } from "./task-protocol";
import type { RelayDelivery, TaskEvent } from "./task-protocol";

const TASK_EVENT_CUSTOM_TYPE = "pi-tasks-event";
const TASK_WAKE_CUSTOM_TYPE = "pi-tasks-wake";
const TASK_CURSOR_CUSTOM_TYPE = "pi-tasks-relay-cursor";

interface InboxContext {
	readonly isIdle: () => boolean;
	readonly hasPendingMessages: () => boolean;
	readonly sessionManager: { readonly getEntries: () => readonly unknown[] };
}

interface InboxPi {
	sendMessage(
		message: { readonly customType: string; readonly content: string; readonly display: boolean; readonly details: TaskEventDetails },
		options: { readonly triggerTurn: false } | { readonly triggerTurn: true; readonly deliverAs: "followUp" },
	): void;
	appendEntry(customType: string, data: { readonly cursor: string }): void;
}

export interface TaskEventDetails {
	readonly taskId: string;
	readonly eventId: string;
}

/** Persists model-visible Pi evidence before advancing the relay cursor, then starts one separate turn. */
export async function deliverTaskInbox(pi: InboxPi, core: TaskCore, context: InboxContext, signal?: AbortSignal): Promise<void> {
	if (context.hasPendingMessages()) return;
	const deliveries = await core.receive(signal);
	for (const delivery of deliveries) {
		if (delivery.envelope.kind === TaskEnvelopeKind.intent) {
			await core.acknowledgeRelayDelivery(delivery.cursor, signal);
			pi.appendEntry(TASK_CURSOR_CUSTOM_TYPE, { cursor: delivery.cursor });
			continue;
		}
		const event = inboxEvent(delivery);
		if (!isKnownEvent(event.type)) throw new TaskProtocolError("UNKNOWN_EVENT", `unknown task inbox event type: ${event.type}`);
		if (context.hasPendingMessages()) return;
		const eventDetails = { taskId: event.taskId, eventId: event.eventId };
		const eventKey = key(event.taskId, event.eventId);
		let incorporated = incorporatedEvents(context.sessionManager.getEntries()).has(eventKey);
		if (!incorporated && isModelVisible(event.type)) {
			if (!context.isIdle()) return;
			pi.sendMessage({
				customType: TASK_EVENT_CUSTOM_TYPE,
				content: renderTaskEvent(event),
				display: true,
				details: eventDetails,
			}, { triggerTurn: false });
			incorporated = incorporatedEvents(context.sessionManager.getEntries()).has(eventKey);
			if (!incorporated) return;
		}
		if (isModelVisible(event.type)) {
			await core.recordInsertion(eventDetails, signal);
			let wakeAccepted = taskMessageKeys(context.sessionManager.getEntries(), TASK_WAKE_CUSTOM_TYPE).has(eventKey);
			if (!wakeAccepted) {
				if (!context.isIdle()) return;
				pi.sendMessage({
					customType: TASK_WAKE_CUSTOM_TYPE,
					content: "Process the pending Pi task event.",
					display: false,
					details: eventDetails,
				}, { triggerTurn: true, deliverAs: "followUp" });
				wakeAccepted = taskMessageKeys(context.sessionManager.getEntries(), TASK_WAKE_CUSTOM_TYPE).has(eventKey);
				if (!wakeAccepted) return;
			}
		}
		await core.acknowledgeRelayDelivery(delivery.cursor, signal);
		pi.appendEntry(TASK_CURSOR_CUSTOM_TYPE, { cursor: delivery.cursor });
	}
}

function inboxEvent(delivery: RelayDelivery): TaskEvent {
	try {
		const payload = JSON.parse(delivery.envelope.payload) as unknown;
		if (!isRecord(payload)) throw new Error("not an object");
		if (delivery.envelope.kind === TaskEnvelopeKind.assignment && isTaskEvent(payload.event)) return payload.event;
		if (delivery.envelope.kind === TaskEnvelopeKind.canonicalEvent && isTaskEvent(payload)) return payload;
	} catch { /* normalize malformed opaque payloads to a protocol error */ }
	throw new TaskProtocolError("INVALID_INBOX_EVENT", "relay envelope does not contain a valid model event");
}

function isKnownEvent(type: string): boolean {
	return ["task.created", "task.completed", "task.failed", "task.cancelled", "task.timed_out", "task.information", "task.question", "task.answer", "task.delivery_receipt", "task.parent_acknowledged", "task.late_terminal"].includes(type);
}

function isModelVisible(type: string): boolean {
	return !["task.delivery_receipt", "task.parent_acknowledged", "task.late_terminal"].includes(type);
}

function renderTaskEvent(event: TaskEvent): string {
	if (event.type === "task.created") return `## task assignment\ntask: \`${event.taskId}\` · event: \`${event.eventId}\`\n\n${String(event.payload.task ?? "")}`;
	const body = typeof event.payload.message === "string" ? `\n\n${event.payload.message}` : "";
	const summary = typeof event.payload.summary === "string" ? `\n\n**summary:** ${event.payload.summary}` : "";
	return `## task ${event.type.replace(/^task\./, "").replaceAll("_", " ")}\ntask: \`${event.taskId}\` · event: \`${event.eventId}\`${body}${summary}`;
}

export function incorporatedTaskEvents(entries: readonly unknown[]): readonly TaskEventDetails[] {
	return taskMessageDetails(entries, TASK_EVENT_CUSTOM_TYPE);
}

function taskMessageDetails(entries: readonly unknown[], customType: string): readonly TaskEventDetails[] {
	const events: TaskEventDetails[] = [];
	for (const entry of entries) {
		if (!isRecord(entry) || entry.type !== "custom_message" || entry.customType !== customType || !isRecord(entry.details) || typeof entry.details.taskId !== "string" || typeof entry.details.eventId !== "string") continue;
		events.push({ taskId: entry.details.taskId, eventId: entry.details.eventId });
	}
	return events;
}

function taskMessageKeys(entries: readonly unknown[], customType: string): Set<string> {
	return new Set(taskMessageDetails(entries, customType).map((event) => key(event.taskId, event.eventId)));
}

function incorporatedEvents(entries: readonly unknown[]): Set<string> {
	return taskMessageKeys(entries, TASK_EVENT_CUSTOM_TYPE);
}

function isTaskEvent(value: unknown): value is TaskEvent {
	return isRecord(value) && typeof value.eventId === "string" && typeof value.taskId === "string" && typeof value.type === "string" && typeof value.sequence === "string" && isRecord(value.source) && isRecord(value.target) && typeof value.occurredAt === "number" && isRecord(value.payload);
}

function key(taskId: string, eventId: string): string {
	return `${taskId}\u0000${eventId}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
