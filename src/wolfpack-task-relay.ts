import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { join } from "node:path";

import { createTaskCore } from "./task-core";
import { wolfpackEndpointView } from "./wolfpack-endpoint-view";
import { createTaskStore } from "./task-store";
import { INVALID_RELAY_METADATA, TASK_PROTOCOL_VERSION, TaskProtocolError } from "./task-protocol";
import type {
	RelayAcceptance,
	RelayConnectInput,
	RelayConnection,
	RelayDeliveryAck,
	RelayEnvelope,
	RelayInboxPage,
	RelayReceiveRequest,
	RelayTargetReference,
	TaskEndpoint,
	TaskRelay,
} from "./task-protocol";
import type { TaskCoreOptions } from "./task-core";
import type { TaskStore, TaskStoreOptions } from "./task-store";

export const WOLFPACK_TASK_RELAY_ID = "wolfpack-pi-tasks-v2";
export const WOLFPACK_TASK_RELAY_PROTOCOL_VERSION = 2;
export const WOLFPACK_TASK_RELAY_LEASE_MS = 60_000;
const DEFAULT_WOLFPACK_PORT = 18_790;
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const MAX_REQUEST_TIMEOUT_MS = 60_000;
const REGISTRATION_POLL_MARGIN_MS = 10_000;
const SESSION_STORE_DIRECTORY = "sessions";
const ENDPOINT_ROTATED_CODE = "ENDPOINT_ROTATED";

export interface WolfpackTaskRelayOptions {
	readonly baseUrl?: string;
	readonly sessionName?: string;
	readonly generation?: string;
	readonly leaseMs?: number;
	readonly requestTimeoutMs?: number;
	readonly fetch?: typeof fetch;
}

export interface WolfpackTaskCoreOptions extends WolfpackTaskRelayOptions, TaskStoreOptions {
	readonly clock?: TaskCoreOptions["clock"];
	readonly ids?: TaskCoreOptions["ids"];
}

export interface WolfpackTaskRelay extends TaskRelay {
	endpoint(signal?: AbortSignal): Promise<TaskEndpoint>;
}

/** Returns the isolated durable store path for one Wolfpack session. */
export function wolfpackTaskStorePath(sessionName: string): string {
	if (!nonEmpty(sessionName)) throw new TaskProtocolError("RELAY_UNAVAILABLE", "WOLFPACK_SESSION_NAME is required for the Wolfpack relay v2 adapter");
	const sessionKey = createHash("sha256").update(sessionName).digest("hex");
	return join(homedir(), ".pi", "tasks", "v2", SESSION_STORE_DIRECTORY, sessionKey, "tasks.sqlite");
}

interface WolfpackRelayEnvelope {
	readonly envelopeId: string;
	readonly protocolVersion: number;
	readonly source: TaskEndpoint;
	readonly target: TaskEndpoint;
	readonly payload: unknown;
	readonly createdAt: string;
}

interface WolfpackRelayResponse {
	readonly ok: true;
}

interface WolfpackConnectResponse extends WolfpackRelayResponse {
	readonly endpoint: TaskEndpoint;
	readonly leaseExpiresAt: string;
}

interface WolfpackResolveResponse extends WolfpackRelayResponse {
	readonly endpoint: TaskEndpoint;
}

interface WolfpackSendResponse extends WolfpackRelayResponse {
	readonly acceptanceId: string;
}

interface WolfpackReceiveResponse extends WolfpackRelayResponse {
	readonly envelopes: readonly WolfpackRelayEnvelope[];
	readonly nextCursor: string;
	readonly hasMore: boolean;
}

interface Registration {
	readonly endpoint: TaskEndpoint;
	readonly expiresAt: number;
}

/** Creates a production relay backed by the local Wolfpack relay v2 HTTP contract. */
export function createWolfpackTaskRelay(options: WolfpackTaskRelayOptions = {}): WolfpackTaskRelay {
	const callerSession = options.sessionName ?? process.env.WOLFPACK_SESSION_NAME;
	const baseUrl = options.baseUrl ?? `http://127.0.0.1:${portFrom(process.env.WOLFPACK_PORT)}`;
	const generation = options.generation ?? crypto.randomUUID();
	const requestTimeoutMs = boundedRequestTimeout(options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS);
	const registrationRenewalWindowMs = requestTimeoutMs + REGISTRATION_POLL_MARGIN_MS;
	const minimumLeaseMs = requestTimeoutMs + registrationRenewalWindowMs;
	const leaseMs = Math.max(options.leaseMs ?? WOLFPACK_TASK_RELAY_LEASE_MS, minimumLeaseMs);
	const requestFetch = options.fetch ?? fetch;
	const envelopeIds = new Map<string, string>();
	let registration: Registration | undefined;

	const register = async (signal?: AbortSignal): Promise<TaskEndpoint> => {
		if (registration && registration.expiresAt - Date.now() > registrationRenewalWindowMs) return registration.endpoint;
		if (!nonEmpty(callerSession)) throw new TaskProtocolError("RELAY_UNAVAILABLE", "WOLFPACK_SESSION_NAME is required for the Wolfpack relay v2 adapter");
		const response = await request<WolfpackConnectResponse>(requestFetch, baseUrl, requestTimeoutMs, "POST", "/api/task-relay/v2/connect", {
			callerSession,
			generation,
			protocolVersions: [WOLFPACK_TASK_RELAY_PROTOCOL_VERSION],
			leaseMs,
		}, signal);
		if (!isEndpoint(response.endpoint) || !futureTimestamp(response.leaseExpiresAt)) throw new TaskProtocolError("INVALID_CONNECTION", "Wolfpack relay returned an invalid registration lease");
		registration = { endpoint: response.endpoint, expiresAt: Date.parse(response.leaseExpiresAt) };
		return registration.endpoint;
	};

	return {
		id: WOLFPACK_TASK_RELAY_ID,
		endpoint: register,
		async connect(input: RelayConnectInput, signal?: AbortSignal): Promise<RelayConnection> {
			if (input.protocolVersion !== TASK_PROTOCOL_VERSION) throw new TaskProtocolError("INCOMPATIBLE_PROTOCOL", "task endpoint protocol is incompatible");
			const registered = await register(signal);
			if (!sameEndpoint(input.endpoint, registered)) throw new TaskProtocolError("INVALID_CONNECTION", "task core endpoint does not match the Wolfpack relay registration");
			return { endpoint: registered, receiveCursor: input.receiveCursor };
		},
		async resolve(input: RelayTargetReference, signal?: AbortSignal): Promise<TaskEndpoint> {
			await register(signal);
			const response = await request<WolfpackResolveResponse>(requestFetch, baseUrl, requestTimeoutMs, "POST", "/api/task-relay/v2/resolve", {
				callerSession: requiredSession(callerSession),
				target: { relay: input.relay, id: input.reference },
				protocolVersion: WOLFPACK_TASK_RELAY_PROTOCOL_VERSION,
			}, signal);
			if (!isEndpoint(response.endpoint)) throw new TaskProtocolError("INVALID_TARGET", "Wolfpack relay returned an invalid target endpoint");
			return response.endpoint;
		},
		async send(input: RelayEnvelope, signal?: AbortSignal): Promise<RelayAcceptance> {
			// Validate/capture before registration or any send. A fresh timestamp or
			// process-local cache cannot safely retry a previously accepted envelope.
			const envelope = toWolfpackEnvelope(input);
			await register(signal);
			const response = await request<WolfpackSendResponse>(requestFetch, baseUrl, requestTimeoutMs, "POST", "/api/task-relay/v2/send", {
				callerSession: requiredSession(callerSession),
				envelope,
			}, signal);
			if (!nonEmpty(response.acceptanceId)) throw new TaskProtocolError("INVALID_ACCEPTANCE", "Wolfpack relay returned an invalid acceptance");
			return { envelopeId: input.envelopeId };
		},
		async receive(input: RelayReceiveRequest, signal?: AbortSignal): Promise<RelayInboxPage> {
			await register(signal);
			const response = await request<WolfpackReceiveResponse>(requestFetch, baseUrl, requestTimeoutMs, "GET", "/api/task-relay/v2/receive", undefined, signal, {
				callerSession: requiredSession(callerSession),
				cursor: input.cursor,
			});
			if (!Array.isArray(response.envelopes) || !decimal(response.nextCursor) || typeof response.hasMore !== "boolean") throw new TaskProtocolError("INVALID_INBOX", "Wolfpack relay returned an invalid inbox page");
			const start = Number(input.cursor);
			if (!Number.isSafeInteger(start) || start < 0) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor must be a non-negative integer");
			const envelopes = response.envelopes.slice(0, input.limit).map(fromWolfpackEnvelope);
			const deliveries = envelopes.map((envelope, index) => {
				const cursor = String(start + index + 1);
				envelopeIds.set(cursor, envelope.envelopeId);
				return { cursor, envelope };
			});
			return { deliveries, nextCursor: response.nextCursor, hasMore: response.hasMore || response.envelopes.length > envelopes.length };
		},
		async acknowledgeDelivery(input: RelayDeliveryAck, signal?: AbortSignal): Promise<void> {
			const registered = await register(signal);
			if (!sameEndpoint(input.endpoint, registered)) throw new TaskProtocolError("INVALID_CONNECTION", "delivery acknowledgement belongs to a different endpoint");
			const cachedId = envelopeIds.get(input.cursor);
			if (cachedId !== undefined && input.envelopeId !== undefined && cachedId !== input.envelopeId) throw new TaskProtocolError("INVALID_CURSOR", "delivery acknowledgement conflicts with its cursor binding");
			const envelopeId = input.envelopeId ?? cachedId;
			if (!nonEmpty(envelopeId)) throw new TaskProtocolError("INVALID_CURSOR", "relay delivery cursor is not available for acknowledgement");
			await request<WolfpackRelayResponse>(requestFetch, baseUrl, requestTimeoutMs, "POST", "/api/task-relay/v2/delivery-ack", { callerSession: requiredSession(callerSession), envelopeId }, signal);
			envelopeIds.delete(input.cursor);
		},
	};
}

/** Registers this process, then returns a core whose endpoint is the registered opaque Wolfpack endpoint. */
export async function createWolfpackTaskCore(options: WolfpackTaskCoreOptions = {}, signal?: AbortSignal): Promise<ReturnType<typeof createTaskCore>> {
	const sessionName = options.sessionName ?? process.env.WOLFPACK_SESSION_NAME;
	const store = createTaskStore({ path: options.path ?? wolfpackTaskStorePath(requiredSession(sessionName)) });
	const generation = options.generation ?? store.getEndpointGeneration() ?? crypto.randomUUID();
	store.transaction(() => { store.setEndpointGeneration(generation); });
	const relay = createWolfpackTaskRelay({ ...options, sessionName, generation });
	try {
		const endpoint = await relay.endpoint(signal);
		bindRegisteredEndpoint(store, endpoint, options.clock?.now() ?? Date.now());
		return createTaskCore({ endpoint, relay, store, ...(options.clock === undefined ? {} : { clock: options.clock }), ...(options.ids === undefined ? {} : { ids: options.ids }) });
	} catch (error) {
		store.close();
		throw error;
	}
}

function bindRegisteredEndpoint(store: TaskStore, endpoint: TaskEndpoint, now: number): void {
	const priorEndpoint = store.getEndpointBinding();
	if (!priorEndpoint) {
		store.transaction(() => { store.setEndpointBinding(endpoint); });
		return;
	}
	if (sameEndpoint(priorEndpoint, endpoint)) return;
	store.transaction(() => {
		store.setEndpointBinding(endpoint);
		store.setReceiveCursor("0");
		for (const record of store.outbox("pending")) {
			if (!sameEndpoint(record.envelope.source, priorEndpoint)) continue;
			store.quarantineOutbox(record.envelope.envelopeId, {
				errorCode: ENDPOINT_ROTATED_CODE,
				reason: "source endpoint rotated before delivery",
				details: { priorEndpoint, endpoint },
				quarantinedAt: now,
			});
		}
	});
}

function toWolfpackEnvelope(envelope: RelayEnvelope): WolfpackRelayEnvelope {
	const createdAt = envelope.createdAt;
	if (!transportTimestamp(createdAt)) {
		throw new TaskProtocolError(INVALID_RELAY_METADATA, "relay envelope requires an immutable persisted creation timestamp; legacy wire metadata cannot be reconstructed safely", {
			retryable: false, details: { envelopeId: envelope.envelopeId, reason: createdAt === undefined ? "missing" : "invalid" },
		});
	}
	let payload: unknown;
	try {
		payload = JSON.parse(envelope.payload) as unknown;
	} catch {
		throw new TaskProtocolError("INVALID_PAYLOAD", "task relay envelope payload is not valid JSON");
	}
	return {
		envelopeId: envelope.envelopeId,
		protocolVersion: WOLFPACK_TASK_RELAY_PROTOCOL_VERSION,
		source: envelope.source,
		target: envelope.target,
		payload: { taskId: envelope.taskId, kind: envelope.kind, payload },
		createdAt,
	};
}

function fromWolfpackEnvelope(envelope: WolfpackRelayEnvelope): RelayEnvelope {
	if (!nonEmpty(envelope.envelopeId) || envelope.protocolVersion !== WOLFPACK_TASK_RELAY_PROTOCOL_VERSION || !isEndpoint(envelope.source) || !isEndpoint(envelope.target)) {
		throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay returned an invalid envelope");
	}
	const taskId = taskIdFromPayload(envelope.payload);
	const kind = envelopeKindFromPayload(envelope.payload);
	const payload = JSON.stringify(wolfpackEndpointView(envelope.source, envelope.target, kind, relayPayload(envelope.payload)));
	if (payload === undefined) throw new TaskProtocolError("INVALID_PAYLOAD", "Wolfpack relay envelope payload is not JSON-serializable");
	return { envelopeId: envelope.envelopeId, protocolVersion: TASK_PROTOCOL_VERSION, source: envelope.source, target: envelope.target, taskId, kind, payload };
}

function taskIdFromPayload(payload: unknown): string {
	if (!isRecord(payload) || !nonEmpty(payload.taskId)) throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay payload does not identify a task");
	return payload.taskId;
}

function envelopeKindFromPayload(payload: unknown): RelayEnvelope["kind"] {
	if (!isRecord(payload) || typeof payload.kind !== "string") throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay payload does not identify an envelope kind");
	if (payload.kind === "assignment" || payload.kind === "intent" || payload.kind === "canonical_event") return payload.kind;
	throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay payload has an invalid envelope kind");
}

function relayPayload(payload: unknown): unknown {
	if (!isRecord(payload) || !("payload" in payload)) throw new TaskProtocolError("INVALID_ENVELOPE", "Wolfpack relay payload is missing its opaque body");
	return payload.payload;
}

async function request<TResponse extends WolfpackRelayResponse>(requestFetch: typeof fetch, baseUrl: string, requestTimeoutMs: number, method: "GET" | "POST", path: string, body?: unknown, signal?: AbortSignal, query?: Record<string, string>): Promise<TResponse> {
	const timeout = new AbortController();
	const initialAbortError = requestAbortError(signal, timeout.signal, requestTimeoutMs);
	if (initialAbortError) throw initialAbortError;
	const controller = new AbortController();
	const abort = (): void => controller.abort();
	const timeoutId = setTimeout(() => timeout.abort(), requestTimeoutMs);
	signal?.addEventListener("abort", abort, { once: true });
	timeout.signal.addEventListener("abort", abort, { once: true });
	try {
		let response: Response;
		try {
			const url = new URL(path, baseUrl);
			for (const [name, value] of Object.entries(query ?? {})) url.searchParams.set(name, value);
			response = await requestFetch(url, body === undefined ? { method, signal: controller.signal } : { method, headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: controller.signal });
		} catch (error) {
			const abortError = requestAbortError(signal, timeout.signal, requestTimeoutMs);
			if (abortError) throw abortError;
			throw new TaskProtocolError("RELAY_UNAVAILABLE", error instanceof Error ? error.message : "Wolfpack relay request failed");
		}
		let payload: unknown;
		try {
			payload = await response.json();
		} catch {
			const abortError = requestAbortError(signal, timeout.signal, requestTimeoutMs);
			if (abortError) throw abortError;
			throw new TaskProtocolError("INVALID_RESPONSE", "Wolfpack relay returned invalid JSON");
		}
		if (!isRecord(payload)) throw new TaskProtocolError("INVALID_RESPONSE", "Wolfpack relay returned an invalid response");
		if (payload.ok === false && isRecord(payload.error) && nonEmpty(payload.error.code) && typeof payload.error.message === "string" && typeof payload.error.retryable === "boolean") {
			throw new TaskProtocolError(payload.error.code, payload.error.message, { retryable: payload.error.retryable });
		}
		if (!response.ok || payload.ok !== true) throw new TaskProtocolError("INVALID_RESPONSE", "Wolfpack relay returned an invalid response");
		return payload as TResponse;
	} finally {
		clearTimeout(timeoutId);
		signal?.removeEventListener("abort", abort);
		timeout.signal.removeEventListener("abort", abort);
	}
}

function requestAbortError(signal: AbortSignal | undefined, timeoutSignal: AbortSignal, requestTimeoutMs: number): TaskProtocolError | undefined {
	if (signal?.aborted) return new TaskProtocolError("ABORTED", "task relay request was cancelled");
	if (timeoutSignal.aborted) return new TaskProtocolError("RELAY_TIMEOUT", `task relay request exceeded ${requestTimeoutMs}ms`);
	return undefined;
}

function requiredSession(sessionName: string | undefined): string {
	if (!nonEmpty(sessionName)) throw new TaskProtocolError("RELAY_UNAVAILABLE", "WOLFPACK_SESSION_NAME is required for the Wolfpack relay v2 adapter");
	return sessionName;
}

function boundedRequestTimeout(value: number): number {
	return Number.isInteger(value) && value >= 1 && value <= MAX_REQUEST_TIMEOUT_MS ? value : DEFAULT_REQUEST_TIMEOUT_MS;
}

function transportTimestamp(value: unknown): value is string {
	if (typeof value !== "string") return false;
	const parsed = Date.parse(value);
	return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function futureTimestamp(value: unknown): value is string {
	return typeof value === "string" && Number.isFinite(Date.parse(value)) && Date.parse(value) > Date.now();
}

function portFrom(value: string | undefined): number {
	const port = value === undefined ? DEFAULT_WOLFPACK_PORT : Number(value);
	return Number.isInteger(port) && port > 0 && port < 65_536 ? port : DEFAULT_WOLFPACK_PORT;
}

function sameEndpoint(left: TaskEndpoint, right: TaskEndpoint): boolean {
	return left.relay === right.relay && left.id === right.id;
}

function isEndpoint(value: unknown): value is TaskEndpoint {
	return isRecord(value) && nonEmpty(value.relay) && nonEmpty(value.id);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function nonEmpty(value: unknown): value is string {
	return typeof value === "string" && value.length > 0;
}

function decimal(value: unknown): value is string {
	return typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value);
}
