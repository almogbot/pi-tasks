import { TaskProtocolError } from "./task-protocol";
import type { RelayEnvelope, TaskEndpoint } from "./task-protocol";

export const WOLFPACK_TASK_RELAY_ID = "wolfpack-pi-tasks-v2";
const PEER = new RegExp(`^${WOLFPACK_TASK_RELAY_ID}:peer:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, "i");

/**
 * Project only protocol-defined references from the sender's local namespace
 * into the recipient's namespace. The authenticated transport header supplies
 * the source peer route and destination identity; payload labels cannot do so.
 * Outgoing wire bytes, opaque application payloads and historical rows stay put.
 */
export function wolfpackEndpointView(source: TaskEndpoint, target: TaskEndpoint, kind: RelayEnvelope["kind"], payload: unknown): unknown {
  if (source.relay === WOLFPACK_TASK_RELAY_ID && target.relay === WOLFPACK_TASK_RELAY_ID) return payload; // Includes origin/self canonical fanout.
  if (!PEER.test(source.relay) || target.relay !== WOLFPACK_TASK_RELAY_ID) invalid();
  if (kind === "intent") return payload; // Intents carry no protocol endpoint references.
  if (!record(payload)) invalid();
  const originalSource = { relay: WOLFPACK_TASK_RELAY_ID, id: source.id };
  const validateSource = (value: unknown): void => {
    if (!endpoint(value) || !same(value, originalSource)) invalid();
  };
  const validateTarget = (value: unknown): TaskEndpoint => {
    if (!endpoint(value) || !PEER.test(value.relay) || value.id !== target.id) invalid();
    return value;
  };
  if (kind === "canonical_event") {
    validateSource(payload.source); validateTarget(payload.target);
    return { ...payload, source, target };
  }
  if (!record(payload.task) || !record(payload.event)) invalid();
  validateSource(payload.task.origin); validateSource(payload.event.source);
  const originalTarget = validateTarget(payload.task.target);
  if (!endpoint(payload.event.target) || !same(payload.event.target, originalTarget)) invalid();
  return {
    ...payload,
    task: { ...payload.task, origin: source, target },
    event: { ...payload.event, source, target },
  };
}

function invalid(): never {
  throw new TaskProtocolError("INVALID_ENDPOINT_REFERENCE", "peer task references do not match the transport's endpoint roles", { retryable: false });
}
function record(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function endpoint(value: unknown): value is TaskEndpoint { return record(value) && typeof value.relay === "string" && typeof value.id === "string"; }
function same(a: TaskEndpoint, b: TaskEndpoint): boolean { return a.relay === b.relay && a.id === b.id; }
