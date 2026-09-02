import { afterAll, beforeAll, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";

import { createTaskStore } from "../src/task-store";
import { createWolfpackTaskCore, createWolfpackTaskRelay, wolfpackTaskStorePath } from "../src/wolfpack-task-relay";
import { TASK_PROTOCOL_VERSION } from "../src/task-protocol";
import type { RelayEnvelope, TaskEndpoint } from "../src/task-protocol";

const temporaryDirectories: string[] = [];

let server: ReturnType<typeof Bun.serve>;
let baseUrl = "";
const requests: Array<{ readonly path: string; readonly body: unknown }> = [];
const endpointsBySession = new Map<string, { readonly relay: string; readonly id: string }>();

beforeAll(() => {
	server = Bun.serve({
		port: 0,
		async fetch(request) {
			const url = new URL(request.url);
			const body = request.method === "POST" ? await request.json() : undefined;
			requests.push({ path: url.pathname, body });
			if (url.pathname === "/api/task-relay/v2/connect") {
				const callerSession = typeof body === "object" && body !== null && "callerSession" in body ? body.callerSession : undefined;
				if (callerSession === "renewing-sender") return Response.json({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "renewing-sender" }, leaseExpiresAt: new Date(Date.now() + 20).toISOString() });
				if (callerSession === "proactive-sender") return Response.json({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "proactive-sender" }, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() });
				if (callerSession === "maximum-timeout-sender") {
					const leaseMs = typeof body === "object" && body !== null && "leaseMs" in body && typeof body.leaseMs === "number" ? body.leaseMs : 0;
					return Response.json({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "maximum-timeout-sender" }, leaseExpiresAt: new Date(Date.now() + leaseMs).toISOString() });
				}
				const generation = typeof body === "object" && body !== null && "generation" in body && typeof body.generation === "string" ? body.generation : "";
				const endpoint = { relay: "wolfpack-pi-tasks-v2", id: callerSession === "restart-sender" ? `restart-${generation}` : "sender" };
				if (typeof callerSession === "string") endpointsBySession.set(callerSession, endpoint);
				return Response.json({ ok: true, endpoint, leaseExpiresAt: "2099-01-01T00:00:00.000Z" });
			}
			if (url.pathname === "/api/task-relay/v2/resolve") return Response.json({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "receiver" } });
			if (url.pathname === "/api/task-relay/v2/send") {
				const callerSession = typeof body === "object" && body !== null && "callerSession" in body ? body.callerSession : undefined;
				if (callerSession === "structured-error-sender") return Response.json({ ok: false, error: { code: "TARGET_NOT_REGISTERED", message: "target is inactive", retryable: false } }, { status: 409 });
				return Response.json({ ok: true, kind: "accepted", acceptanceId: "accepted-envelope", forwarding: "local" });
			}
			if (url.pathname === "/api/task-relay/v2/receive") {
				const endpoint = endpointsBySession.get(url.searchParams.get("callerSession") ?? "") ?? { relay: "wolfpack-pi-tasks-v2", id: "sender" };
				return Response.json({
				ok: true,
				envelopes: [{
					envelopeId: "canonical-event", protocolVersion: 2,
					source: endpoint,
					target: endpoint,
					payload: { taskId: "task-1", kind: "canonical_event", payload: { eventId: "event-2", taskId: "task-1", type: "task.information", sequence: "2", source: endpoint, target: { relay: "wolfpack-pi-tasks-v2", id: "receiver" }, occurredAt: 1, payload: { message: "progress" } } },
					createdAt: "2026-08-09T00:00:00.000Z",
				}], nextCursor: "1", hasMore: false,
				});
			}
			if (url.pathname === "/api/task-relay/v2/delivery-ack") return Response.json({ ok: true, kind: "acknowledged" });
			return Response.json({ ok: false, error: { code: "INVALID_REQUEST", message: "unexpected route", retryable: false } }, { status: 400 });
		},
	});
	baseUrl = `http://127.0.0.1:${server.port}`;
});

afterAll(() => {
	server.stop(true);
	for (const directory of temporaryDirectories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

test("uses the configured Wolfpack relay v2 HTTP contract instead of the in-memory relay", async () => {
	const core = await createWolfpackTaskCore({ baseUrl, sessionName: "sender", generation: "test-process", path: ":memory:", ids: sequence() });
	await core.connect();
	const created = await core.createTask({ target: { relay: "wolfpack-pi-tasks-v2", id: "receiver" }, task: "implement narrowly", timeoutMs: 1_000 });
	const deliveries = await core.receive();
	await core.acknowledgeRelayDelivery(deliveries[0]?.cursor ?? "0");

	expect(created.taskId).toBe("task-1");
	expect(core.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.information"]);
	expect(requests).toEqual(expect.arrayContaining([
		{ path: "/api/task-relay/v2/connect", body: { callerSession: "sender", generation: "test-process", protocolVersions: [2], leaseMs: 60_000 } },
		{ path: "/api/task-relay/v2/resolve", body: { callerSession: "sender", target: { relay: "wolfpack-pi-tasks-v2", id: "receiver" }, protocolVersion: 2 } },
		expect.objectContaining({ path: "/api/task-relay/v2/send", body: expect.objectContaining({ callerSession: "sender", envelope: expect.objectContaining({ envelopeId: "task-3", protocolVersion: 2, source: { relay: "wolfpack-pi-tasks-v2", id: "sender" }, target: { relay: "wolfpack-pi-tasks-v2", id: "receiver" } }) }) }),
		{ path: "/api/task-relay/v2/delivery-ack", body: { callerSession: "sender", envelopeId: "canonical-event" } },
	]));
	const send = requests.find((request) => request.path === "/api/task-relay/v2/send");
	expect(send?.body).toMatchObject({ envelope: { payload: expect.objectContaining({ taskId: "task-1", kind: "assignment" }) } });
});

test("persists the Wolfpack generation and durable receive, intent, and timeout state across restart", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-wolfpack-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const now = { value: 1_000 };
	const options = { baseUrl, sessionName: "restart-sender", path, clock: { now: (): number => now.value } };
	const initial = await createWolfpackTaskCore({ ...options, ids: sequence() });
	await initial.connect();
	const created = await initial.createTask({ target: { relay: "wolfpack-pi-tasks-v2", id: "receiver" }, task: "survive restart", timeoutMs: 500 });
	await initial.receive();
	expect(initial.getTask(created.taskId)?.expiresAt).toBe(1_500);
	await initial.submitIntent({ taskId: created.taskId, type: "task.information", payload: { message: "persisted intent" } });

	now.value = 1_501;
	const restarted = await createWolfpackTaskCore({ ...options, ids: sequence("restart") });
	await restarted.connect();
	expect(restarted.getTask(created.taskId)?.expiresAt).toBeLessThan(now.value);
	expect(restarted.getTask(created.taskId)?.origin).toEqual(endpointsBySession.get("restart-sender"));
	expect(restarted.getTask(created.taskId)?.status).toBe("active");
	await restarted.evaluateTimeouts();

	expect(restarted.getTask(created.taskId)?.events.map((event) => event.type)).toEqual(["task.created", "task.information", "task.information", "task.timed_out"]);
	expect(restarted.getTask(created.taskId)?.status).toBe("timed_out");
	const generations = requests
		.filter((request) => request.path === "/api/task-relay/v2/connect" && (request.body as { readonly callerSession?: string }).callerSession === "restart-sender")
		.map((request) => (request.body as { readonly generation: string }).generation);
	expect(generations).toHaveLength(2);
	expect(generations[0]).toBe(generations[1]);
});

test("first endpoint binding preserves all pending work and the cursor", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-wolfpack-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "first-endpoint" };
	const unrelatedEndpoint = { relay: "wolfpack-pi-tasks-v2", id: "unrelated-endpoint" };
	const store = createTaskStore({ path });
	store.setReceiveCursor("7");
	store.putOutbox(assignment("pending-first-binding", endpoint, "receiver"));
	store.putOutbox(assignment("pending-unrelated-source", unrelatedEndpoint, "receiver"));

	await createWolfpackTaskCore({ sessionName: "first-binding", generation: "first-generation", path, fetch: registrationFetch(endpoint) });

	expect(store.getEndpointBinding()).toEqual(endpoint);
	expect(store.getReceiveCursor()).toBe("7");
	expect(store.outbox("pending").map((record) => record.envelope.envelopeId)).toEqual(["pending-first-binding", "pending-unrelated-source"]);
	expect(store.quarantinedOutbox()).toEqual([]);
	store.close();
});

test("atomically resets rotated endpoint state and quarantines only prior-source pending work", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-wolfpack-");
	temporaryDirectories.push(directory);
	const path = join(directory, "tasks.sqlite");
	const prior = { relay: "wolfpack-pi-tasks-v2", id: "prior-endpoint" };
	const rotated = { relay: "wolfpack-pi-tasks-v2", id: "rotated-endpoint" };
	const unrelated = { relay: "wolfpack-pi-tasks-v2", id: "unrelated-source" };
	const store = createTaskStore({ path });
	store.setEndpointBinding(prior);
	store.setReceiveCursor("9");
	store.putOutbox(assignment("prior-pending", prior, "receiver"));
	store.putOutbox(assignment("unrelated-pending", unrelated, "receiver"));
	store.putOutbox(assignment("prior-accepted", prior, "receiver"));
	store.markOutboxAccepted("prior-accepted");

	await createWolfpackTaskCore({ sessionName: "rotated-binding", generation: "rotated-generation", path, fetch: registrationFetch(rotated) });

	expect(store.getEndpointBinding()).toEqual(rotated);
	expect(store.getReceiveCursor()).toBe("0");
	expect(store.outbox("pending").map((record) => record.envelope.envelopeId)).toEqual(["unrelated-pending"]);
	expect(store.outbox("accepted").map((record) => record.envelope.envelopeId)).toEqual(["prior-accepted"]);
	expect(store.quarantinedOutbox()).toEqual([expect.objectContaining({
		errorCode: "ENDPOINT_ROTATED",
		priorState: "pending",
		details: { priorEndpoint: prior, endpoint: rotated },
		envelope: expect.objectContaining({ envelopeId: "prior-pending" }),
	})]);
	store.close();
});

test("isolates default durable state by Wolfpack session and reuses one session generation", async () => {
	const firstSession = `store-a-${randomUUID()}`;
	const secondSession = `store-b-${randomUUID()}`;
	const firstPath = wolfpackTaskStorePath(firstSession);
	const secondPath = wolfpackTaskStorePath(secondSession);

	try {
		await createWolfpackTaskCore({ baseUrl, sessionName: firstSession });
		await createWolfpackTaskCore({ baseUrl, sessionName: firstSession });
		await createWolfpackTaskCore({ baseUrl, sessionName: secondSession });

		expect(firstPath).not.toBe(secondPath);
		expect(existsSync(firstPath)).toBe(true);
		expect(existsSync(secondPath)).toBe(true);
		const firstGenerations = requests
			.filter((request) => request.path === "/api/task-relay/v2/connect" && (request.body as { readonly callerSession?: string }).callerSession === firstSession)
			.map((request) => (request.body as { readonly generation: string }).generation);
		const secondGenerations = requests
			.filter((request) => request.path === "/api/task-relay/v2/connect" && (request.body as { readonly callerSession?: string }).callerSession === secondSession)
			.map((request) => (request.body as { readonly generation: string }).generation);
		expect(firstGenerations).toHaveLength(2);
		expect(new Set(firstGenerations).size).toBe(1);
		expect(secondGenerations).toHaveLength(1);
		expect(secondGenerations[0]).not.toBe(firstGenerations[0]);
	} finally {
		rmSync(dirname(firstPath), { recursive: true, force: true });
		rmSync(dirname(secondPath), { recursive: true, force: true });
	}
});

test("isolates per-session state from an explicitly selected alternate store", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-isolated-store-");
	temporaryDirectories.push(directory);
	const previousHome = process.env.HOME;
	process.env.HOME = directory;
	try {
		const alternatePath = join(directory, ".pi", "tasks", "v2", "tasks.sqlite");
		const alternateCore = await createWolfpackTaskCore({ baseUrl, sessionName: "alternate-writer", path: alternatePath, ids: sequence("alternate") });
		const alternateTask = await alternateCore.createTask({ target: { relay: "wolfpack-pi-tasks-v2", id: "receiver" }, task: "alternate state", timeoutMs: 1_000 });
		const sessionCore = await createWolfpackTaskCore({ baseUrl, sessionName: "session-isolation", ids: sequence("session") });

		expect(sessionCore.getTask(alternateTask.taskId)).toBeUndefined();
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("renews an expired Wolfpack registration through the real connect route", async () => {
	const relay = createWolfpackTaskRelay({ baseUrl, sessionName: "renewing-sender", generation: "renewing-process" });

	const first = await relay.endpoint();
	await Bun.sleep(30);
	const second = await relay.endpoint();

	expect(first).toEqual(second);
	expect(requests.filter((request) => request.path === "/api/task-relay/v2/connect" && (request.body as { readonly callerSession?: string }).callerSession === "renewing-sender")).toHaveLength(2);
});

test("renews a normal Wolfpack lease early enough for request latency and poll cadence", async () => {
	const originalNow = Date.now;
	let now = 1_000_000;
	Date.now = (): number => now;
	try {
		const relay = createWolfpackTaskRelay({ baseUrl, sessionName: "proactive-sender", generation: "proactive-process" });
		const first = await relay.endpoint();
		now += 40_000;
		const second = await relay.endpoint();

		expect(first).toEqual(second);
		expect(requests.filter((request) => request.path === "/api/task-relay/v2/connect" && (request.body as { readonly callerSession?: string }).callerSession === "proactive-sender")).toHaveLength(2);
	} finally {
		Date.now = originalNow;
	}
});

test("sizes the maximum-timeout lease across delayed registration and renewal requests", async () => {
	const originalNow = Date.now;
	let now = 1_000_000;
	let priorLeaseExpiry = 0;
	let expiryGap = false;
	const requestedLeases: number[] = [];
	let connects = 0;
	Date.now = (): number => now;
	try {
		const requestFetch = Object.assign(async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
			connects += 1;
			const body = JSON.parse(String(init?.body)) as { readonly leaseMs: number };
			requestedLeases.push(body.leaseMs);
			if (connects === 1) {
				priorLeaseExpiry = now + body.leaseMs;
				const leaseExpiresAt = new Date(priorLeaseExpiry).toISOString();
				now += 60_000;
				return Response.json({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "maximum-timeout-sender" }, leaseExpiresAt });
			}
			now += 60_000;
			expiryGap = now > priorLeaseExpiry;
			priorLeaseExpiry = now + body.leaseMs;
			return Response.json({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "maximum-timeout-sender" }, leaseExpiresAt: new Date(priorLeaseExpiry).toISOString() });
		}, { preconnect: fetch.preconnect });
		const relay = createWolfpackTaskRelay({ baseUrl, sessionName: "maximum-timeout-sender", generation: "maximum-timeout-process", requestTimeoutMs: 60_000, fetch: requestFetch });

		await relay.endpoint();
		now += 5_000;
		await relay.endpoint();

		expect(requestedLeases).toEqual([130_000, 130_000]);
		expect(expiryGap).toBeFalse();
	} finally {
		Date.now = originalNow;
	}
});

test("preserves relay error code and retryability from rejected sends", async () => {
	const relay = createWolfpackTaskRelay({ baseUrl, sessionName: "structured-error-sender", generation: "structured-error-process" });
	const endpoint = await relay.endpoint();

	await expect(relay.send({
		envelopeId: "rejected-envelope",
		protocolVersion: TASK_PROTOCOL_VERSION,
		source: endpoint,
		target: { relay: "wolfpack-pi-tasks-v2", id: "inactive" },
		taskId: "task-1",
		kind: "assignment",
		payload: "{}",
	})).rejects.toMatchObject({ code: "TARGET_NOT_REGISTERED", retryable: false });
});

test("propagates caller cancellation and bounds stalled relay HTTP requests", async () => {
	const stalledFetch = ((_request: string | URL | Request, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
		init?.signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
	})) as typeof fetch;
	const cancelled = new AbortController();
	const abortRelay = createWolfpackTaskRelay({ sessionName: "abort-sender", fetch: stalledFetch });
	const pendingAbort = abortRelay.endpoint(cancelled.signal);
	cancelled.abort();
	await expect(pendingAbort).rejects.toMatchObject({ code: "ABORTED" });

	const timeoutRelay = createWolfpackTaskRelay({ sessionName: "timeout-sender", fetch: stalledFetch, requestTimeoutMs: 10 });
	await expect(timeoutRelay.endpoint()).rejects.toMatchObject({ code: "RELAY_TIMEOUT" });
});

test("propagates caller cancellation while parsing a streamed relay response body", async () => {
	const response = streamedConnectResponse();
	const relay = createWolfpackTaskRelay({
		sessionName: "abort-stream-sender",
		fetch: ((_request: string | URL | Request, init?: RequestInit) => {
			response.attach(init?.signal);
			return Promise.resolve(response.value);
		}) as typeof fetch,
	});
	const cancelled = new AbortController();
	const pending = relay.endpoint(cancelled.signal);

	await response.jsonStarted;
	cancelled.abort();

	await expect(pending).rejects.toMatchObject({ code: "ABORTED" });
});

test("propagates relay deadlines while parsing a streamed response body", async () => {
	const response = streamedConnectResponse();
	const relay = createWolfpackTaskRelay({
		sessionName: "timeout-stream-sender",
		requestTimeoutMs: 10,
		fetch: ((_request: string | URL | Request, init?: RequestInit) => {
			response.attach(init?.signal);
			return Promise.resolve(response.value);
		}) as typeof fetch,
	});
	const pending = relay.endpoint();

	await response.jsonStarted;

	await expect(pending).rejects.toMatchObject({ code: "RELAY_TIMEOUT" });
});

function streamedConnectResponse(): { readonly value: Response; readonly jsonStarted: Promise<void>; attach(signal: AbortSignal | null | undefined): void } {
	let signal: AbortSignal | undefined;
	let startJson: (() => void) | undefined;
	let consumed = false;
	const jsonStarted = new Promise<void>((resolve) => { startJson = resolve; });
	const stream = new ReadableStream<Uint8Array>({
		pull(controller): void {
			if (consumed) return;
			consumed = true;
			startJson?.();
			const complete = setTimeout(() => {
				controller.enqueue(new TextEncoder().encode(JSON.stringify({ ok: true, endpoint: { relay: "wolfpack-pi-tasks-v2", id: "stream-sender" }, leaseExpiresAt: "2099-01-01T00:00:00.000Z" })));
				controller.close();
			}, 50);
			signal?.addEventListener("abort", () => {
				clearTimeout(complete);
				controller.error(new Error("stream aborted"));
			}, { once: true });
		},
	});
	return {
		value: new Response(stream),
		jsonStarted,
		attach(value: AbortSignal | null | undefined): void {
			signal = value ?? undefined;
		},
	};
}

function registrationFetch(endpoint: TaskEndpoint): typeof fetch {
	return Object.assign(async (): Promise<Response> => Response.json({
		ok: true,
		endpoint,
		leaseExpiresAt: "2099-01-01T00:00:00.000Z",
	}), { preconnect: fetch.preconnect }) as typeof fetch;
}

function assignment(envelopeId: string, source: TaskEndpoint, targetId: string): RelayEnvelope {
	return {
		envelopeId,
		protocolVersion: TASK_PROTOCOL_VERSION,
		source,
		target: { relay: source.relay, id: targetId },
		taskId: `task-${envelopeId}`,
		kind: "assignment",
		payload: "{}",
	};
}

function sequence(prefix = "task"): () => string {
	let number = 0;
	return (): string => `${prefix}-${++number}`;
}
