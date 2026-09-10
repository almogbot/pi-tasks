export const TASK_PROTOCOL_VERSION = "pi-tasks/v2" as const;
export const MAX_RELAY_PAYLOAD_BYTES = 48 * 1024;
export const INVALID_RELAY_METADATA = "INVALID_RELAY_METADATA";
export const ORIGIN_CANCELLATION_OPERATION = "origin_cancellation";
export const PARENT_ACKNOWLEDGMENT_OPERATION = "parent_acknowledgment";
export const TERMINAL_INTENT_OPERATION = "terminal_intent";

export const TaskDeliveryStage = {
	receiverRecorded: "receiver_recorded",
	piInsertion: "pi_insertion",
	piInserted: "pi_inserted",
	wakeRequested: "wake_requested",
	wakeAccepted: "wake_accepted",
} as const;

export const TaskDeliveryEvidenceState = {
	confirmed: "confirmed",
	blocked: "blocked",
} as const;

export interface TaskEndpoint {
	readonly relay: string;
	readonly id: string;
}

/** Endpoint-owned transport binding; not task lifecycle or relay replay state. */
export interface RelayTransportBinding {
	readonly profile: "volatile-v1";
	readonly epoch: string;
	readonly endpoint: TaskEndpoint;
	readonly generation: string;
	readonly callerSession: string;
	readonly url: string;
	readonly reset?: true;
}

/** A provider-defined reference which core resolves without interpreting. */
export interface RelayTargetReference {
	readonly relay: string;
	readonly reference: string;
}

export interface RelayConnectInput {
	readonly endpoint: TaskEndpoint;
	readonly protocolVersion: string;
	readonly receiveCursor: string;
}

export interface RelayConnection {
	readonly endpoint: TaskEndpoint;
	readonly receiveCursor: string;
}

export const TaskEnvelopeKind = {
	assignment: "assignment",
	intent: "intent",
	canonicalEvent: "canonical_event",
} as const;

export type TaskEnvelopeKind = (typeof TaskEnvelopeKind)[keyof typeof TaskEnvelopeKind];

export interface RelayEnvelope {
	readonly envelopeId: string;
	readonly protocolVersion: string;
	readonly source: TaskEndpoint;
	readonly target: TaskEndpoint;
	readonly taskId: string;
	readonly kind: TaskEnvelopeKind;
	/** Immutable transport creation time, persisted with new outbox envelopes.
	 * Received/legacy records may omit it; Wolfpack sends require it. */
	readonly createdAt?: string;
	/** Opaque to relays. The endpoint protocol defines its JSON representation. */
	readonly payload: string;
}

export interface RelayAcceptance {
	readonly envelopeId: string;
}

export interface RelayReceiveRequest {
	readonly endpoint: TaskEndpoint;
	readonly cursor: string;
	readonly limit: number;
}

export interface RelayDelivery {
	readonly cursor: string;
	readonly envelope: RelayEnvelope;
}

export interface RelayInboxPage {
	readonly deliveries: readonly RelayDelivery[];
	readonly nextCursor: string;
	readonly hasMore: boolean;
}

export interface RelayDeliveryAck {
	readonly endpoint: TaskEndpoint;
	readonly cursor: string;
	/** Durable cursor-to-envelope binding, for identical ACK retry after reopen. */
	readonly envelopeId?: string;
}

export interface TaskRelay {
	readonly id: string;
	connect(input: RelayConnectInput, signal?: AbortSignal): Promise<RelayConnection>;
	resolve(input: RelayTargetReference, signal?: AbortSignal): Promise<TaskEndpoint>;
	send(input: RelayEnvelope, signal?: AbortSignal): Promise<RelayAcceptance>;
	receive(input: RelayReceiveRequest, signal?: AbortSignal): Promise<RelayInboxPage>;
	/** Acknowledge this delivery only, never an implicit cursor prefix. */
	acknowledgeDelivery(input: RelayDeliveryAck, signal?: AbortSignal): Promise<void>;
}

export interface TaskEvent {
	readonly eventId: string;
	readonly taskId: string;
	readonly type: string;
	readonly sequence: string;
	readonly source: TaskEndpoint;
	readonly target: TaskEndpoint;
	readonly occurredAt: number;
	readonly payload: Record<string, unknown>;
}

export interface TaskRecord {
	readonly taskId: string;
	readonly protocolVersion: string;
	readonly origin: TaskEndpoint;
	readonly target: TaskEndpoint;
	readonly task: string;
	readonly createdAt: number;
	readonly expiresAt: number;
	readonly status: "active" | "completed" | "failed" | "cancelled" | "timed_out";
	readonly events: readonly TaskEvent[];
}

export type TerminalTaskIntentType = "task.completed" | "task.failed" | "task.cancelled";

export type TaskDeliveryStage = (typeof TaskDeliveryStage)[keyof typeof TaskDeliveryStage];
export type TaskDeliveryEvidenceState = (typeof TaskDeliveryEvidenceState)[keyof typeof TaskDeliveryEvidenceState];

export interface TaskIntent {
	readonly intentId: string;
	readonly taskId: string;
	readonly type: TerminalTaskIntentType | "task.information" | "task.question" | "task.answer" | "task.delivery_receipt";
	readonly payload: Record<string, unknown>;
}

interface TerminalDeliveryIdentity {
	readonly intentId: string;
	readonly intentType: TerminalTaskIntentType;
	readonly envelopeId: string;
	readonly origin: TaskEndpoint;
}

interface TerminalDeliveryPending extends TerminalDeliveryIdentity {
	readonly state: "pending";
}

interface TerminalDeliveryAccepted extends TerminalDeliveryIdentity {
	readonly state: "accepted";
}

interface TerminalDeliveryBlocked extends TerminalDeliveryIdentity {
	readonly state: "delivery_blocked";
	readonly blockedAt: number;
	readonly error: { readonly code: string; readonly retryable: false; readonly details: Readonly<Record<string, unknown>> };
}

export type TerminalDeliveryState =
	| { readonly state: "not_submitted" }
	| TerminalDeliveryPending
	| TerminalDeliveryAccepted
	| TerminalDeliveryBlocked;

export interface TaskSnapshot extends TaskRecord {
	readonly terminalDelivery: TerminalDeliveryState;
}

export class TaskProtocolError extends Error {
	readonly code: string;
	readonly retryable: boolean;
	readonly details: Readonly<Record<string, unknown>> | undefined;

	constructor(code: string, message: string, options: { readonly retryable?: boolean; readonly details?: Readonly<Record<string, unknown>> } = {}) {
		super(message);
		this.name = "TaskProtocolError";
		this.code = code;
		this.retryable = options.retryable ?? true;
		this.details = options.details;
	}
}

export class TaskOutboxDeliveryError extends TaskProtocolError {
	constructor(code: string, message: string, options: { readonly retryable?: boolean; readonly details?: Readonly<Record<string, unknown>> } = {}) {
		super(code, message, options);
		this.name = "TaskOutboxDeliveryError";
	}
}
