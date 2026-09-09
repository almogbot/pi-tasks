import {
	MAX_RELAY_PAYLOAD_BYTES,
	TASK_PROTOCOL_VERSION,
	TaskProtocolError,
} from "./task-protocol";
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

interface MailboxItem {
	readonly cursor: string;
	readonly envelope: RelayEnvelope;
}

/** Deterministic conformance fixture; production relays must provide durable storage. */
export function createInMemoryTaskRelay(id: string): InMemoryTaskRelay {
	return new InMemoryTaskRelay(id);
}

export class InMemoryTaskRelay implements TaskRelay {
	readonly id: string;
	private readonly endpoints = new Map<string, TaskEndpoint>();
	private readonly mailboxes = new Map<string, MailboxItem[]>();
	private readonly accepted = new Set<string>();
	private readonly acknowledged = new Set<string>();
	private nextDelivery = 0;
	private sendFailure = false;

	constructor(id: string) {
		this.id = id;
	}

	async connect(input: RelayConnectInput): Promise<RelayConnection> {
		if (input.protocolVersion !== TASK_PROTOCOL_VERSION) throw new TaskProtocolError("INCOMPATIBLE_PROTOCOL", "task endpoint protocol is incompatible");
		this.assertEndpoint(input.endpoint);
		this.endpoints.set(input.endpoint.id, input.endpoint);
		if (!this.mailboxes.has(input.endpoint.id)) this.mailboxes.set(input.endpoint.id, []);
		return { endpoint: input.endpoint, receiveCursor: input.receiveCursor };
	}

	async resolve(input: RelayTargetReference): Promise<TaskEndpoint> {
		if (input.relay !== this.id) throw new TaskProtocolError("UNKNOWN_RELAY", "target belongs to another relay");
		const endpoint = this.endpoints.get(input.reference);
		if (!endpoint) throw new TaskProtocolError("UNREGISTERED_TARGET", "target is not registered for this protocol");
		return endpoint;
	}

	async send(input: RelayEnvelope): Promise<RelayAcceptance> {
		if (this.sendFailure) {
			this.sendFailure = false;
			throw new Error("in-memory relay send failed");
		}
		this.assertEnvelope(input);
		if (this.accepted.has(input.envelopeId)) return { envelopeId: input.envelopeId };
		const mailbox = this.mailboxes.get(input.target.id);
		if (!mailbox) throw new TaskProtocolError("UNREGISTERED_TARGET", "target is not registered for this protocol");
		this.accepted.add(input.envelopeId);
		mailbox.push({ cursor: String(++this.nextDelivery), envelope: input });
		return { envelopeId: input.envelopeId };
	}

	async receive(input: RelayReceiveRequest): Promise<RelayInboxPage> {
		this.assertEndpoint(input.endpoint);
		const mailbox = this.mailboxes.get(input.endpoint.id);
		if (!mailbox) throw new TaskProtocolError("UNREGISTERED_TARGET", "endpoint is not registered for this protocol");
		const cursor = Number(input.cursor);
		if (!Number.isSafeInteger(cursor) || cursor < 0) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor must be a non-negative integer");
		const start = cursor;
		const pending = mailbox.filter((item) => Number(item.cursor) > start && !this.acknowledged.has(item.envelope.envelopeId));
		const deliveries = pending.slice(0, input.limit);
		return {
			deliveries,
			nextCursor: deliveries.at(-1)?.cursor ?? String(start),
			hasMore: pending.length > deliveries.length,
		};
	}

	async acknowledgeDelivery(input: RelayDeliveryAck): Promise<void> {
		this.assertEndpoint(input.endpoint);
		const cursor = Number(input.cursor);
		if (!Number.isSafeInteger(cursor) || cursor < 0) throw new TaskProtocolError("INVALID_CURSOR", "relay cursor must be a non-negative integer");
		const item = this.mailboxes.get(input.endpoint.id)?.find(item => item.cursor === input.cursor);
		if (!item) throw new TaskProtocolError("INVALID_CURSOR", "relay delivery is not in this endpoint mailbox");
		this.acknowledged.add(item.envelope.envelopeId);
	}

	failNextSend(): void {
		this.sendFailure = true;
	}

	envelopesFor(endpoint: TaskEndpoint): readonly RelayEnvelope[] {
		return (this.mailboxes.get(endpoint.id) ?? []).map((item) => item.envelope);
	}

	private assertEndpoint(endpoint: TaskEndpoint): void {
		if (endpoint.relay !== this.id || endpoint.id.length === 0) throw new TaskProtocolError("INVALID_ENDPOINT", "endpoint must belong to this relay and have an opaque id");
	}

	private assertEnvelope(envelope: RelayEnvelope): void {
		if (envelope.protocolVersion !== TASK_PROTOCOL_VERSION) throw new TaskProtocolError("INCOMPATIBLE_PROTOCOL", "envelope protocol is incompatible");
		this.assertEndpoint(envelope.source);
		this.assertEndpoint(envelope.target);
		if (!this.endpoints.has(envelope.source.id)) throw new TaskProtocolError("UNREGISTERED_SOURCE", "source is not registered for this protocol");
		if (new TextEncoder().encode(envelope.payload).byteLength > MAX_RELAY_PAYLOAD_BYTES) throw new TaskProtocolError("PAYLOAD_TOO_LARGE", "relay payload exceeds the protocol limit");
	}
}
