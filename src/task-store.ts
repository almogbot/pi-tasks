import { chmodSync, existsSync, mkdirSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

import { openSqliteDatabase } from "./sqlite-database";
import type { SqliteDatabase } from "./sqlite-database";
import { TERMINAL_INTENT_OPERATION, TaskProtocolError } from "./task-protocol";
import type { RelayTransportBinding, RelayEnvelope, TaskEndpoint, TaskEvent, TaskRecord, TaskSnapshot, TerminalDeliveryState, TerminalTaskIntentType } from "./task-protocol";

const SCHEMA_VERSION = 5;
const OWNER_ONLY_MODE = 0o700;
const OWNER_ONLY_FILE_MODE = 0o600;
const ENDPOINT_GENERATION_STATE_KEY = "endpoint_generation";
const ENDPOINT_BINDING_STATE_KEY = "endpoint_binding";

export interface TaskStoreOptions {
	readonly path?: string;
}

export interface OutboxRecord {
	readonly envelope: RelayEnvelope;
	readonly state: "pending" | "accepted";
}

export interface OutboxQuarantineInput {
	readonly errorCode: string;
	readonly reason: string;
	readonly details: Readonly<Record<string, unknown>>;
	readonly quarantinedAt: number;
}

export interface OutboxQuarantineRecord extends OutboxQuarantineInput {
	readonly envelope: RelayEnvelope;
	readonly priorState: OutboxRecord["state"];
}

export interface TaskOperationRecord {
	readonly taskId: string;
	readonly operation: string;
	readonly logicalId: string;
	readonly logicalType: string;
	readonly envelopeIds: readonly string[];
}

export interface TaskStore {
	transaction<T>(operation: () => T): T;
	putTask(task: Omit<TaskRecord, "events">): void;
	getTask(taskId: string): TaskSnapshot | undefined;
	listTasks(): readonly TaskSnapshot[];
	setStatus(taskId: string, status: TaskRecord["status"]): void;
	appendEvent(event: TaskEvent): boolean;
	putOutbox(envelope: RelayEnvelope): void;
	outbox(state: OutboxRecord["state"]): readonly OutboxRecord[];
	markOutboxAccepted(envelopeId: string): void;
	quarantineOutbox(envelopeId: string, input: OutboxQuarantineInput): void;
	quarantinedOutbox(): readonly OutboxQuarantineRecord[];
	persistInbox(envelope: RelayEnvelope, cursor: string): boolean;
	getReceiveCursor(): string;
	setReceiveCursor(cursor: string): void;
	trackRelayDeliveries(endpoint: TaskEndpoint, deliveries: readonly { readonly cursor: string; readonly envelopeId: string }[]): void;
	pendingRelayEnvelopeId(endpoint: TaskEndpoint, cursor: string): string | undefined;
	requestRelayAcknowledgement(endpoint: TaskEndpoint, cursor: string): void;
	requestedRelayAcknowledgements(endpoint: TaskEndpoint): readonly string[];
	acknowledgeRelayCursor(endpoint: TaskEndpoint, cursor: string): void;
	putIntent(intentId: string, taskId: string, envelopeId: string): void;
	putInsertionReceipt(taskId: string, eventId: string): boolean;
	reserveTaskOperation(input: TaskOperationRecord): { readonly created: boolean; readonly record: TaskOperationRecord };
	getEndpointGeneration(): string | undefined;
	setEndpointGeneration(generation: string): void;
	getEndpointBinding(): TaskEndpoint | undefined;
	setEndpointBinding(endpoint: TaskEndpoint): void;
	getRelayTransportBinding(): RelayTransportBinding | undefined;
	setRelayTransportBinding(binding: RelayTransportBinding): void;
	close(): void;
}

export function createTaskStore(options: TaskStoreOptions = {}): TaskStore {
	const path = options.path ?? join(homedir(), ".pi", "tasks", "v2", "tasks.sqlite");
	preparePath(path);
	const database = openSqliteDatabase(path);
	try {
		initializeSchema(database);
		database.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
	} catch (error) {
		database.close();
		throw error;
	}

	const store: TaskStore = {
		transaction(operation) {
			return database.transaction(operation)();
		},
		putTask(task) {
			database.query(`INSERT INTO tasks (task_id, protocol_version, origin_relay, origin_id, target_relay, target_id, task, created_at, expires_at, status)
				VALUES ($taskId, $protocolVersion, $originRelay, $originId, $targetRelay, $targetId, $task, $createdAt, $expiresAt, $status)
				ON CONFLICT(task_id) DO NOTHING`).run(taskRow(task));
		},
		getTask(taskId) {
			const row = database.query("SELECT * FROM tasks WHERE task_id = ?").get(taskId) as TaskRow | null;
			if (!row) return undefined;
			const eventRows = database.query("SELECT * FROM events WHERE task_id = ? ORDER BY CAST(sequence AS INTEGER), event_id").all(taskId) as EventRow[];
			return taskFromRow(database, row, eventRows);
		},
		listTasks() {
			const rows = database.query("SELECT * FROM tasks ORDER BY created_at, task_id").all() as TaskRow[];
			const events = database.query("SELECT * FROM events ORDER BY CAST(sequence AS INTEGER), event_id").all() as EventRow[];
			const eventsByTask = new Map<string, EventRow[]>();
			for (const event of events) {
				const current = eventsByTask.get(event.task_id) ?? [];
				current.push(event);
				eventsByTask.set(event.task_id, current);
			}
			return rows.map((row) => taskFromRow(database, row, eventsByTask.get(row.task_id) ?? []));
		},
		setStatus(taskId, status) {
			database.query("UPDATE tasks SET status = ? WHERE task_id = ?").run(status, taskId);
		},
		appendEvent(event) {
			const result = database.query(`INSERT INTO events (event_id, task_id, type, sequence, source_relay, source_id, target_relay, target_id, occurred_at, payload)
				VALUES ($eventId, $taskId, $type, $sequence, $sourceRelay, $sourceId, $targetRelay, $targetId, $occurredAt, $payload)
				ON CONFLICT(event_id) DO NOTHING`).run(eventRow(event));
			return result.changes === 1;
		},
		putOutbox(envelope) {
			database.query("INSERT INTO outbox (envelope_id, envelope, state) VALUES (?, ?, 'pending') ON CONFLICT(envelope_id) DO NOTHING").run(envelope.envelopeId, JSON.stringify(envelope));
		},
		outbox(state) {
			const rows = database.query("SELECT envelope FROM outbox WHERE state = ? ORDER BY rowid").all(state) as Array<{ readonly envelope: string }>;
			return rows.map((row) => ({ envelope: parseEnvelope(row.envelope), state }));
		},
		markOutboxAccepted(envelopeId) {
			database.query("UPDATE outbox SET state = 'accepted' WHERE envelope_id = ?").run(envelopeId);
		},
		quarantineOutbox(envelopeId, input) {
			const row = database.query("SELECT envelope, state FROM outbox WHERE envelope_id = ?").get(envelopeId) as { readonly envelope: string; readonly state: OutboxRecord["state"] } | null;
			if (!row) return;
			database.query(`INSERT INTO outbox_quarantine (envelope, error_code, reason, details, quarantined_at, prior_state, envelope_id)
				VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(envelope_id) DO NOTHING`).run(
				row.envelope, input.errorCode, input.reason, JSON.stringify(input.details), input.quarantinedAt, row.state, envelopeId,
			);
			database.query("DELETE FROM outbox WHERE envelope_id = ?").run(envelopeId);
		},
		quarantinedOutbox() {
			const rows = database.query("SELECT envelope, error_code, reason, details, quarantined_at, prior_state FROM outbox_quarantine ORDER BY rowid").all() as OutboxQuarantineRow[];
			return rows.map((row) => ({
				envelope: parseEnvelope(row.envelope),
				errorCode: row.error_code,
				reason: row.reason,
				details: parseRecord(row.details),
				quarantinedAt: row.quarantined_at,
				priorState: row.prior_state,
			}));
		},
		persistInbox(envelope, cursor) {
			const result = database.query("INSERT INTO inbox (envelope_id, cursor, envelope) VALUES (?, ?, ?) ON CONFLICT(envelope_id) DO NOTHING").run(envelope.envelopeId, cursor, JSON.stringify(envelope));
			return result.changes === 1;
		},
		getReceiveCursor() {
			const row = database.query("SELECT value FROM relay_state WHERE name = 'receive_cursor'").get() as { readonly value: string } | null;
			return row?.value ?? "0";
		},
		setReceiveCursor(cursor) {
			database.query("INSERT INTO relay_state (name, value) VALUES ('receive_cursor', ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value").run(cursor);
		},
		trackRelayDeliveries(endpoint, deliveries) {
			const state = deliveryCheckpoint(endpoint);
			const priorHighWater = BigInt(state.highWater);
			for (const delivery of deliveries) {
				const prior = state.pending[delivery.cursor];
				if (prior !== undefined && prior !== delivery.envelopeId) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor changed envelope identity");
				// A previously observed but no longer pending cursor was ACKed. In
				// particular, legacy relays may still return ACKed rows on reread.
				if (BigInt(delivery.cursor) > priorHighWater) state.pending[delivery.cursor] = delivery.envelopeId;
				if (BigInt(delivery.cursor) > BigInt(state.highWater)) state.highWater = delivery.cursor;
			}
			writeDeliveryCheckpoint(endpoint, state);
		},
		pendingRelayEnvelopeId(endpoint, cursor) { return deliveryCheckpoint(endpoint).pending[cursor]; },
		requestRelayAcknowledgement(endpoint, cursor) {
			const state = deliveryCheckpoint(endpoint);
			if (state.pending[cursor] === undefined) throw new TaskProtocolError("INVALID_CURSOR", "relay delivery is not pending in this endpoint scope");
			(state.requested ??= {})[cursor] = true;
			writeDeliveryCheckpoint(endpoint, state);
		},
		requestedRelayAcknowledgements(endpoint) { return Object.keys(deliveryCheckpoint(endpoint).requested ?? {}); },
		acknowledgeRelayCursor(endpoint, cursor) {
			const state = deliveryCheckpoint(endpoint);
			// Concurrent confirmations of the same requested ACK are harmless.
			if (state.pending[cursor] === undefined) return;
			delete state.pending[cursor];
			if (state.requested) delete state.requested[cursor];
			const pending = Object.keys(state.pending);
			if (pending.length === 0) state.frontier = state.highWater;
			else if (BigInt(cursor) > BigInt(state.frontier) && pending.every(value => BigInt(value) > BigInt(cursor))) state.frontier = cursor;
			writeDeliveryCheckpoint(endpoint, state);
			store.setReceiveCursor(state.frontier);
		},
		putIntent(intentId, taskId, envelopeId) {
			database.query("INSERT INTO intents (intent_id, task_id, envelope_id) VALUES (?, ?, ?) ON CONFLICT(intent_id) DO NOTHING").run(intentId, taskId, envelopeId);
		},
		putInsertionReceipt(taskId, eventId) {
			const result = database.query("INSERT INTO insertion_receipts (task_id, event_id) VALUES (?, ?) ON CONFLICT(task_id, event_id) DO NOTHING").run(taskId, eventId);
			return result.changes === 1;
		},
		reserveTaskOperation(input) {
			const result = database.query(`INSERT INTO task_operations (task_id, operation, logical_id, logical_type, envelope_ids)
				VALUES (?, ?, ?, ?, ?) ON CONFLICT(task_id, operation) DO NOTHING`).run(input.taskId, input.operation, input.logicalId, input.logicalType, JSON.stringify(input.envelopeIds));
			const row = database.query("SELECT task_id, operation, logical_id, logical_type, envelope_ids FROM task_operations WHERE task_id = ? AND operation = ?").get(input.taskId, input.operation) as TaskOperationRow | null;
			if (!row) throw new Error("task operation reservation was not persisted");
			return { created: result.changes === 1, record: taskOperationFromRow(row) };
		},
		getEndpointGeneration() {
			const row = database.query("SELECT value FROM relay_state WHERE name = ?").get(ENDPOINT_GENERATION_STATE_KEY) as { readonly value: string } | null;
			return row?.value;
		},
		setEndpointGeneration(generation) {
			database.query("INSERT INTO relay_state (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value").run(ENDPOINT_GENERATION_STATE_KEY, generation);
		},
		getEndpointBinding() {
			const row = database.query("SELECT value FROM relay_state WHERE name = ?").get(ENDPOINT_BINDING_STATE_KEY) as { readonly value: string } | null;
			return row ? parseEndpoint(row.value) : undefined;
		},
		setEndpointBinding(endpoint) {
			database.query("INSERT INTO relay_state (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value").run(ENDPOINT_BINDING_STATE_KEY, JSON.stringify(endpoint));
		},
		getRelayTransportBinding() {
			const row = database.query("SELECT value FROM relay_state WHERE name = 'transport_binding'").get() as { readonly value: string } | null;
			if (!row) return undefined;
			let value: unknown;
			try { value = JSON.parse(row.value) as unknown; } catch { throw new TaskProtocolError("INVALID_RELAY_METADATA", "invalid persisted transport binding", { retryable: false }); }
			if (!isRecord(value) || value.profile !== "volatile-v1" || typeof value.epoch !== "string" || !isRecord(value.endpoint)
				|| typeof value.endpoint.relay !== "string" || typeof value.endpoint.id !== "string" || typeof value.generation !== "string"
				|| typeof value.callerSession !== "string" || typeof value.url !== "string" || (value.reset !== undefined && value.reset !== true)) {
				throw new TaskProtocolError("INVALID_RELAY_METADATA", "invalid persisted transport binding", { retryable: false });
			}
			return value as unknown as RelayTransportBinding;
		},
		setRelayTransportBinding(binding) {
			database.query("INSERT INTO relay_state (name, value) VALUES ('transport_binding', ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value").run(JSON.stringify(binding));
		},
		close() { database.close(); },
	};
	function checkpointKey(endpoint: TaskEndpoint): string {
		const bound = store.getEndpointBinding();
		if (bound && (bound.relay !== endpoint.relay || bound.id !== endpoint.id)) throw new TaskProtocolError("RELAY_RESET", "delivery checkpoint belongs to a retired endpoint", { retryable: false });
		const transport = store.getRelayTransportBinding();
		return `delivery_checkpoint:${JSON.stringify(transport ? [endpoint.relay, endpoint.id, transport.profile, transport.epoch] : [endpoint.relay, endpoint.id])}`;
	}
	function deliveryCheckpoint(endpoint: TaskEndpoint): DeliveryCheckpoint {
		const row = database.query("SELECT value FROM relay_state WHERE name = ?").get(checkpointKey(endpoint)) as { readonly value: string } | null;
		if (row) return JSON.parse(row.value) as DeliveryCheckpoint;
		const frontier = store.getReceiveCursor();
		return { frontier, highWater: frontier, pending: {} };
	}
	function writeDeliveryCheckpoint(endpoint: TaskEndpoint, state: DeliveryCheckpoint): void {
		database.query("INSERT INTO relay_state (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value").run(checkpointKey(endpoint), JSON.stringify(state));
	}
	return store;
}

interface DeliveryCheckpoint {
	frontier: string;
	highWater: string;
	pending: Record<string, string>;
	requested?: Record<string, true>;
}

interface TaskRow {
	readonly task_id: string;
	readonly protocol_version: string;
	readonly origin_relay: string;
	readonly origin_id: string;
	readonly target_relay: string;
	readonly target_id: string;
	readonly task: string;
	readonly created_at: number;
	readonly expires_at: number;
	readonly status: string;
}

interface OutboxQuarantineRow {
	readonly envelope: string;
	readonly error_code: string;
	readonly reason: string;
	readonly details: string;
	readonly quarantined_at: number;
	readonly prior_state: OutboxRecord["state"];
}

interface TaskOperationRow {
	readonly task_id: string;
	readonly operation: string;
	readonly logical_id: string;
	readonly logical_type: string;
	readonly envelope_ids: string;
}

interface EventRow {
	readonly event_id: string;
	readonly task_id: string;
	readonly type: string;
	readonly sequence: string;
	readonly source_relay: string;
	readonly source_id: string;
	readonly target_relay: string;
	readonly target_id: string;
	readonly occurred_at: number;
	readonly payload: string;
}

function initializeSchema(database: SqliteDatabase): void {
	const version = database.query("PRAGMA user_version").get() as { readonly user_version: number };
	if (version.user_version === SCHEMA_VERSION) return;
	if (version.user_version !== 0) throw new Error("task store schema version is unsupported");
	database.transaction(() => {
		database.exec(`
			CREATE TABLE tasks (task_id TEXT PRIMARY KEY, protocol_version TEXT NOT NULL, origin_relay TEXT NOT NULL, origin_id TEXT NOT NULL, target_relay TEXT NOT NULL, target_id TEXT NOT NULL, task TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, status TEXT NOT NULL);
			CREATE TABLE events (event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(task_id), type TEXT NOT NULL, sequence TEXT NOT NULL, source_relay TEXT NOT NULL, source_id TEXT NOT NULL, target_relay TEXT NOT NULL, target_id TEXT NOT NULL, occurred_at INTEGER NOT NULL, payload TEXT NOT NULL);
			CREATE TABLE outbox (envelope_id TEXT PRIMARY KEY, envelope TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending', 'accepted')));
			CREATE TABLE inbox (envelope_id TEXT PRIMARY KEY, cursor TEXT NOT NULL, envelope TEXT NOT NULL);
			CREATE TABLE intents (intent_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, envelope_id TEXT NOT NULL);
			CREATE TABLE relay_state (name TEXT PRIMARY KEY, value TEXT NOT NULL);
			CREATE TABLE insertion_receipts (task_id TEXT NOT NULL REFERENCES tasks(task_id), event_id TEXT NOT NULL, PRIMARY KEY (task_id, event_id));
			CREATE TABLE outbox_quarantine (
				envelope_id TEXT PRIMARY KEY,
				envelope TEXT NOT NULL,
				reason TEXT NOT NULL,
				quarantined_at INTEGER NOT NULL,
				error_code TEXT NOT NULL,
				details TEXT NOT NULL,
				prior_state TEXT NOT NULL
			);
			CREATE TABLE task_operations (
				task_id TEXT NOT NULL REFERENCES tasks(task_id),
				operation TEXT NOT NULL,
				logical_id TEXT NOT NULL,
				logical_type TEXT NOT NULL,
				envelope_ids TEXT NOT NULL,
				PRIMARY KEY (task_id, operation)
			);
			CREATE INDEX tasks_origin_expiry ON tasks (origin_relay, origin_id, status, expires_at);
			CREATE INDEX inbox_cursor ON inbox (cursor);
			PRAGMA user_version = 5;
		`);
	})();
}

function preparePath(path: string): void {
	if (path === ":memory:") return;
	const directory = dirname(path);
	mkdirSync(directory, { recursive: true, mode: OWNER_ONLY_MODE });
	const directoryMode = statSync(directory).mode & 0o777;
	if ((directoryMode & 0o077) !== 0) throw new Error(`task store directory is not owner-only: ${directory}`);
	if (existsSync(path)) {
		const fileMode = statSync(path).mode & 0o777;
		if ((fileMode & 0o077) !== 0) throw new Error(`task store file is not owner-only: ${path}`);
	} else {
		writeFileSync(path, "", { mode: OWNER_ONLY_FILE_MODE });
		chmodSync(path, OWNER_ONLY_FILE_MODE);
	}
}

function taskRow(task: Omit<TaskRecord, "events">): Record<string, string | number> {
	return {
		taskId: task.taskId, protocolVersion: task.protocolVersion, originRelay: task.origin.relay, originId: task.origin.id,
		targetRelay: task.target.relay, targetId: task.target.id, task: task.task, createdAt: task.createdAt, expiresAt: task.expiresAt, status: task.status,
	};
}

function taskFromRow(database: SqliteDatabase, row: TaskRow, events: readonly EventRow[]): TaskSnapshot {
	return {
		taskId: row.task_id,
		protocolVersion: row.protocol_version,
		origin: { relay: row.origin_relay, id: row.origin_id },
		target: { relay: row.target_relay, id: row.target_id },
		task: row.task,
		createdAt: row.created_at,
		expiresAt: row.expires_at,
		status: row.status as TaskRecord["status"],
		events: events.map(eventFromRow),
		terminalDelivery: terminalDeliveryFromDatabase(database, row),
	};
}

function eventRow(event: TaskEvent): Record<string, string | number> {
	return {
		eventId: event.eventId, taskId: event.taskId, type: event.type, sequence: event.sequence,
		sourceRelay: event.source.relay, sourceId: event.source.id, targetRelay: event.target.relay, targetId: event.target.id,
		occurredAt: event.occurredAt, payload: JSON.stringify(event.payload),
	};
}

function eventFromRow(row: EventRow): TaskEvent {
	return {
		eventId: row.event_id, taskId: row.task_id, type: row.type, sequence: row.sequence,
		source: { relay: row.source_relay, id: row.source_id }, target: { relay: row.target_relay, id: row.target_id },
		occurredAt: row.occurred_at, payload: parseRecord(row.payload),
	};
}

function parseEnvelope(value: string): RelayEnvelope {
	return JSON.parse(value) as RelayEnvelope;
}

function taskOperationFromRow(row: TaskOperationRow): TaskOperationRecord {
	const envelopeIds = JSON.parse(row.envelope_ids) as unknown;
	if (!Array.isArray(envelopeIds) || !envelopeIds.every((value) => typeof value === "string" && value.length > 0)) throw new Error("task operation envelope identities are malformed");
	return { taskId: row.task_id, operation: row.operation, logicalId: row.logical_id, logicalType: row.logical_type, envelopeIds };
}

function terminalDeliveryFromDatabase(database: SqliteDatabase, task: TaskRow): TerminalDeliveryState {
	const operation = database.query("SELECT task_id, operation, logical_id, logical_type, envelope_ids FROM task_operations WHERE task_id = ? AND operation = ?").get(task.task_id, TERMINAL_INTENT_OPERATION) as TaskOperationRow | null;
	if (!operation) return { state: "not_submitted" };
	const record = taskOperationFromRow(operation);
	if (!isTerminalIntentType(record.logicalType) || record.envelopeIds.length !== 1) throw new Error("terminal task operation is malformed");
	const envelopeId = record.envelopeIds[0];
	if (envelopeId === undefined) throw new Error("terminal task operation has no envelope identity");
	const identity = {
		intentId: record.logicalId,
		intentType: record.logicalType,
		envelopeId,
		origin: { relay: task.origin_relay, id: task.origin_id },
	};
	const outbox = database.query("SELECT state FROM outbox WHERE envelope_id = ?").get(identity.envelopeId) as { readonly state: OutboxRecord["state"] } | null;
	if (outbox?.state === "pending") return { state: "pending", ...identity };
	if (outbox?.state === "accepted") return { state: "accepted", ...identity };
	const blocked = database.query("SELECT error_code, details, quarantined_at FROM outbox_quarantine WHERE envelope_id = ?").get(identity.envelopeId) as Pick<OutboxQuarantineRow, "error_code" | "details" | "quarantined_at"> | null;
	if (!blocked) throw new Error("terminal task operation has no durable delivery record");
	return {
		state: "delivery_blocked",
		...identity,
		blockedAt: blocked.quarantined_at,
		error: { code: blocked.error_code, retryable: false, details: parseRecord(blocked.details) },
	};
}

function isTerminalIntentType(value: string): value is TerminalTaskIntentType {
	return value === "task.completed" || value === "task.failed" || value === "task.cancelled";
}

function parseEndpoint(value: string): TaskEndpoint {
	const endpoint = JSON.parse(value) as unknown;
	if (!isRecord(endpoint) || typeof endpoint.relay !== "string" || endpoint.relay.length === 0 || typeof endpoint.id !== "string" || endpoint.id.length === 0) {
		throw new Error("task store endpoint binding is malformed");
	}
	return { relay: endpoint.relay, id: endpoint.id };
}

function parseRecord(value: string): Record<string, unknown> {
	return JSON.parse(value) as Record<string, unknown>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
