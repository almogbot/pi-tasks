import { TERMINAL_INTENT_OPERATION, TaskProtocolError } from "./task-protocol";
import type { RelayTransportBinding, RelayEnvelope, TaskEndpoint, TaskEvent, TaskRecord, TaskSnapshot, TerminalDeliveryState, TerminalTaskIntentType } from "./task-protocol";

export interface TaskStoreOptions {
  /** Lower-only per-endpoint limits; session history, not this store, is the archive. */
  readonly maxEntries?: number;
  readonly maxBytes?: number;
}
export interface OutboxRecord { readonly envelope: RelayEnvelope; readonly state: "pending" | "accepted"; }
export interface OutboxQuarantineInput {
  readonly errorCode: string; readonly reason: string;
  readonly details: Readonly<Record<string, unknown>>; readonly quarantinedAt: number;
}
export interface OutboxQuarantineRecord extends OutboxQuarantineInput { readonly envelope: RelayEnvelope; readonly priorState: OutboxRecord["state"]; }
export interface TaskOperationRecord {
  readonly taskId: string; readonly operation: string; readonly logicalId: string;
  readonly logicalType: string; readonly envelopeIds: readonly string[];
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
  clear(): void;
  close(): void;
}

const LIMITS = { entries: 16_384, bytes: 32 * 1024 * 1024 };
type Cell = { readonly text: string; readonly bytes: number; readonly order: number };
type Undo = { readonly table: Map<string, Cell>; readonly key: string; readonly prior: Cell | undefined };
interface Checkpoint { frontier: string; highWater: string; pending: Record<string, string>; requested: Record<string, true>; }

/** One endpoint lifetime. No SQLite, files, migrations, startup reads or replay. */
export function createTaskStore(options: TaskStoreOptions = {}): TaskStore {
  if (Object.keys(options).some(key => key !== "maxEntries" && key !== "maxBytes")) throw new TypeError("task state is memory-only; persistence options are unsupported");
  const maxEntries = options.maxEntries ?? LIMITS.entries, maxBytes = options.maxBytes ?? LIMITS.bytes;
  if (!Number.isInteger(maxEntries) || maxEntries < 1 || maxEntries > LIMITS.entries || !Number.isInteger(maxBytes) || maxBytes < 1 || maxBytes > LIMITS.bytes) throw new TypeError("invalid task memory limit");
  const tables = new Map<string, Map<string, Cell>>();
  const frames: Map<string, Undo>[] = [];
  let bytes = 0, entries = 0, sequence = 0, closed = false;
  const remember = (name: string, key: string, target: Map<string, Cell>, prior: Cell | undefined) => {
    const frame = frames.at(-1), identity = JSON.stringify([name, key]);
    if (frame && !frame.has(identity)) frame.set(identity, { table: target, key, prior });
  };
  const check = () => { if (closed) throw new TaskProtocolError("RELAY_CLOSED", "task state is closed", { retryable: false }); };
  const table = (name: string) => { check(); let value = tables.get(name); if (!value) { value = new Map(); tables.set(name, value); } return value; };
  const get = <T>(name: string, key: string): T | undefined => { const row = table(name).get(key); return row ? JSON.parse(row.text) as T : undefined; };
  const rows = <T>(name: string): T[] => [...table(name).values()].sort((a, b) => a.order - b.order).map(row => JSON.parse(row.text) as T);
  const put = (name: string, key: string, value: unknown, onlyNew = false): boolean => {
    const target = table(name), prior = target.get(key);
    if (onlyNew && prior) return false;
    const text = JSON.stringify(value), size = Buffer.byteLength(text) + Buffer.byteLength(key) + 64;
    if (bytes - (prior?.bytes ?? 0) + size > maxBytes || (!prior && entries >= maxEntries)) throw new TaskProtocolError("TASK_CAPACITY", "active task state is full", { retryable: false });
    remember(name, key, target, prior);
    target.set(key, { text, bytes: size, order: prior?.order ?? sequence++ });
    bytes += size - (prior?.bytes ?? 0); if (!prior) entries++;
    return true;
  };
  const remove = (name: string, key: string) => {
    const target = table(name), prior = target.get(key); if (!prior) return;
    remember(name, key, target, prior);
    target.delete(key); bytes -= prior.bytes; entries--;
  };
  const pair = (a: string, b: string) => JSON.stringify([a, b]);
  const requireTask = (id: string) => { if (!get("tasks", id)) throw new TaskProtocolError("TASK_NOT_FOUND", "task is not active in this endpoint lifetime"); };
  const sortEvents = (a: TaskEvent, b: TaskEvent) => BigInt(a.sequence) < BigInt(b.sequence) ? -1 : BigInt(a.sequence) > BigInt(b.sequence) ? 1 : a.eventId.localeCompare(b.eventId);
  const terminal = (task: Omit<TaskRecord, "events">): TerminalDeliveryState => {
    const record = get<TaskOperationRecord>("operations", pair(task.taskId, TERMINAL_INTENT_OPERATION));
    if (!record) return { state: "not_submitted" };
    if (!["task.completed", "task.failed", "task.cancelled"].includes(record.logicalType) || record.envelopeIds.length !== 1) throw new Error("terminal operation is malformed");
    const identity = { intentId: record.logicalId, intentType: record.logicalType as TerminalTaskIntentType, envelopeId: record.envelopeIds[0]!, origin: task.origin };
    const outbox = get<OutboxRecord>("outbox", identity.envelopeId);
    if (outbox) return { state: outbox.state, ...identity };
    const blocked = get<OutboxQuarantineRecord>("quarantine", identity.envelopeId);
    if (!blocked) throw new Error("terminal operation has no delivery record");
    return { state: "delivery_blocked", ...identity, blockedAt: blocked.quarantinedAt, error: { code: blocked.errorCode, retryable: false, details: blocked.details } };
  };
  const snapshot = (task: Omit<TaskRecord, "events">, events: TaskEvent[]): TaskSnapshot => ({ ...task, events: events.sort(sortEvents), terminalDelivery: terminal(task) });
  const state = <T>(key: string) => get<T>("state", key);
  const checkpointKey = (endpoint: TaskEndpoint) => {
    const bound = store.getEndpointBinding();
    if (bound && (bound.relay !== endpoint.relay || bound.id !== endpoint.id)) throw new TaskProtocolError("RELAY_RESET", "delivery checkpoint belongs to a retired endpoint", { retryable: false });
    return JSON.stringify([endpoint.relay, endpoint.id, store.getRelayTransportBinding()?.epoch]);
  };
  const checkpoint = (endpoint: TaskEndpoint): Checkpoint => get<Checkpoint>("checkpoints", checkpointKey(endpoint)) ?? { frontier: store.getReceiveCursor(), highWater: store.getReceiveCursor(), pending: {}, requested: {} };
  const writeCheckpoint = (endpoint: TaskEndpoint, value: Checkpoint) => { put("checkpoints", checkpointKey(endpoint), value); };
  const store: TaskStore = {
    transaction<T>(operation: () => T): T {
      check(); const priorBytes = bytes, priorEntries = entries, priorSequence = sequence;
      const frame = new Map<string, Undo>(); frames.push(frame);
      try {
        const result = operation();
        if (result instanceof Promise) throw new TypeError("task transactions must be synchronous");
        const parent = frames.at(-2);
        if (parent) for (const [key, item] of frame) if (!parent.has(key)) parent.set(key, item);
        return result;
      } catch (error) {
        for (const item of [...frame.values()].reverse()) { if (item.prior) item.table.set(item.key, item.prior); else item.table.delete(item.key); }
        bytes = priorBytes; entries = priorEntries; sequence = priorSequence;
        throw error;
      } finally { frames.pop(); }
    },
    putTask(task) { put("tasks", task.taskId, task, true); },
    getTask(id) { const task = get<Omit<TaskRecord, "events">>("tasks", id); return task ? snapshot(task, rows<TaskEvent>("events").filter(event => event.taskId === id)) : undefined; },
    listTasks() {
      const events = new Map<string, TaskEvent[]>();
      for (const event of rows<TaskEvent>("events")) { const list = events.get(event.taskId) ?? []; list.push(event); events.set(event.taskId, list); }
      return rows<Omit<TaskRecord, "events">>("tasks").sort((a, b) => a.createdAt - b.createdAt || a.taskId.localeCompare(b.taskId)).map(task => snapshot(task, events.get(task.taskId) ?? []));
    },
    setStatus(id, status) { const task = get<Omit<TaskRecord, "events">>("tasks", id); if (task) put("tasks", id, { ...task, status }); },
    appendEvent(event) { requireTask(event.taskId); return put("events", event.eventId, event, true); },
    putOutbox(envelope) { put("outbox", envelope.envelopeId, { envelope, state: "pending" }, true); },
    outbox(status) { return rows<OutboxRecord>("outbox").filter(item => item.state === status); },
    markOutboxAccepted(id) { const prior = get<OutboxRecord>("outbox", id); if (prior) put("outbox", id, { ...prior, state: "accepted" }); },
    quarantineOutbox(id, input) {
      store.transaction(() => { const prior = get<OutboxRecord>("outbox", id); if (!prior) return;
        put("quarantine", id, { ...input, envelope: prior.envelope, priorState: prior.state }, true); remove("outbox", id);
      });
    },
    quarantinedOutbox() { return rows<OutboxQuarantineRecord>("quarantine"); },
    persistInbox(envelope, cursor) { return put("inbox", envelope.envelopeId, { envelope, cursor }, true); },
    getReceiveCursor() { return state<string>("cursor") ?? "0"; },
    setReceiveCursor(cursor) { put("state", "cursor", cursor); },
    trackRelayDeliveries(endpoint, deliveries) {
      const value = checkpoint(endpoint), highWater = BigInt(value.highWater);
      for (const delivery of deliveries) {
        const prior = value.pending[delivery.cursor];
        if (prior !== undefined && prior !== delivery.envelopeId) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor changed envelope identity");
        if (BigInt(delivery.cursor) > highWater) value.pending[delivery.cursor] = delivery.envelopeId;
        if (BigInt(delivery.cursor) > BigInt(value.highWater)) value.highWater = delivery.cursor;
      }
      writeCheckpoint(endpoint, value);
    },
    pendingRelayEnvelopeId(endpoint, cursor) { return checkpoint(endpoint).pending[cursor]; },
    requestRelayAcknowledgement(endpoint, cursor) {
      const value = checkpoint(endpoint);
      if (value.pending[cursor] === undefined) throw new TaskProtocolError("INVALID_CURSOR", "relay delivery is not pending in this endpoint scope");
      value.requested[cursor] = true; writeCheckpoint(endpoint, value);
    },
    requestedRelayAcknowledgements(endpoint) { return Object.keys(checkpoint(endpoint).requested); },
    acknowledgeRelayCursor(endpoint, cursor) {
      store.transaction(() => {
        const value = checkpoint(endpoint); if (value.pending[cursor] === undefined) return;
        delete value.pending[cursor]; delete value.requested[cursor];
        const pending = Object.keys(value.pending);
        if (!pending.length) value.frontier = value.highWater;
        else if (BigInt(cursor) > BigInt(value.frontier) && pending.every(other => BigInt(other) > BigInt(cursor))) value.frontier = cursor;
        writeCheckpoint(endpoint, value); store.setReceiveCursor(value.frontier);
      });
    },
    putIntent(intentId, taskId, envelopeId) { put("intents", intentId, { taskId, envelopeId }, true); },
    putInsertionReceipt(taskId, eventId) { requireTask(taskId); return put("insertions", pair(taskId, eventId), true, true); },
    reserveTaskOperation(input) { requireTask(input.taskId); const key = pair(input.taskId, input.operation); const created = put("operations", key, input, true); return { created, record: get<TaskOperationRecord>("operations", key)! }; },
    getEndpointGeneration() { return state<string>("generation"); },
    setEndpointGeneration(value) { put("state", "generation", value); },
    getEndpointBinding() { return state<TaskEndpoint>("endpoint"); },
    setEndpointBinding(value) { put("state", "endpoint", value); },
    getRelayTransportBinding() { return state<RelayTransportBinding>("transport"); },
    setRelayTransportBinding(value) { put("state", "transport", value); },
    clear() { check(); if (frames.length) throw new Error("cannot clear task state during a transaction"); tables.clear(); bytes = entries = sequence = 0; },
    close() { if (closed) return; store.clear(); closed = true; },
  };
  return store;
}
