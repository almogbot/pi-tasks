import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { createWolfpackTaskRelay } from "../src/wolfpack-task-relay";

// Optional cross-repository gate. Explicitly select trusted source and its exact
// revision; never use an installed/default Wolfpack server or a live broker.
const wolfpack = process.env.PI_TASKS_WOLFPACK_SOURCE;
const revision = process.env.PI_TASKS_WOLFPACK_REVISION;

test.skipIf(!wolfpack)("real core + adapter + HTTP relay: lost response and adapter/store restart preserve exact retry content", async () => {
	expect(isAbsolute(wolfpack!)).toBe(true);
	expect(revision).toMatch(/^[0-9a-f]{40}$/);
	expect(execFileSync("git", ["rev-parse", "HEAD"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe(revision!);
	expect(execFileSync("git", ["status", "--porcelain", "--untracked-files=no"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe("");
	const { TaskRelayGateway } = await import(join(wolfpack!, "src/task-relay/gateway.ts"));
	const root = mkdtempSync(join(tmpdir(), "pi-tasks-real-relay-retry-"));
	const gateway = new TaskRelayGateway({ root: join(root, "relay"), inspectSession: async (session: string) => ({
		ok: true, session, sessionId: session, projectPath: root, harness: "pi", alive: true,
	}) });
	const sent: Array<{ envelope: Record<string, unknown>; response: Record<string, unknown> }> = [];
	const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
		const url = new URL(request.url);
		const body = request.method === "POST" ? await request.json() : undefined;
		let response;
		switch (url.pathname) {
			case "/api/task-relay/v2/connect": response = await gateway.connect(body); break;
			case "/api/task-relay/v2/resolve": response = await gateway.resolve(body); break;
			case "/api/task-relay/v2/send":
				response = await gateway.send(body);
				sent.push({ envelope: structuredClone((body as { envelope: Record<string, unknown> }).envelope), response });
				break;
			default: throw new Error(`unexpected fixture route ${url.pathname}`);
		}
		return Response.json(response, { status: response.ok ? 200 : 409 });
	} });
	const baseUrl = `http://127.0.0.1:${server.port}`;
	const storePath = join(root, "origin.sqlite");
	let store = createTaskStore({ path: storePath });
	let drop = true;
	const lossyFetch = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
		const response = await fetch(input, init);
		if (new URL(String(input)).pathname === "/api/task-relay/v2/send" && drop) {
			drop = false;
			await response.json(); // Real destination accepted; originating adapter never sees that response.
			throw new Error("injected response loss after acceptance");
		}
		return response;
	}) as typeof fetch;
	try {
		const receiver = await gateway.connect({ callerSession: "receiver", generation: "receiver-generation", protocolVersions: [2] });
		expect(receiver.ok).toBe(true);
		const options = { baseUrl, sessionName: "origin", generation: "stable-generation" };
		const relay = createWolfpackTaskRelay({ ...options, fetch: lossyFetch });
		const endpoint = await relay.endpoint();
		const initial = createTaskCore({ endpoint, relay, store });
		await initial.connect();
		await expect(initial.createTask({ target: receiver.endpoint, task: "retry unchanged", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "RELAY_UNAVAILABLE" });
		expect(store.outbox("pending").length).toBe(1);
		const pending = store.outbox("pending")[0]!.envelope;
		expect(sent[0]!.response.kind).toBe("accepted");
		store.close();
		await Bun.sleep(20); // Distinct wall-clock time; no monkeypatched clock or timestamp cache.
		store = createTaskStore({ path: storePath });
		const resumedRelay = createWolfpackTaskRelay(options);
		const resumed = createTaskCore({ endpoint, relay: resumedRelay, store });
		await resumed.connect();
		await resumed.flushOutbox();
		expect(sent.length).toBe(2);
		expect(sent[1]!.envelope).toEqual(sent[0]!.envelope);
		expect(sent[1]!.response).toMatchObject({ kind: "duplicate", acceptanceId: sent[0]!.response.acceptanceId });
		expect(store.outbox("pending")).toEqual([]);
		expect(store.outbox("accepted")[0]!.envelope.createdAt).toBe(sent[0]!.envelope.createdAt as string);
		expect(pending.createdAt).toBe(sent[0]!.envelope.createdAt as string);
		const page = await gateway.receive({ callerSession: "receiver", cursor: "0" });
		expect(page.ok).toBe(true); expect(page.envelopes.length).toBe(1);
		expect(page.envelopes[0].envelopeId).toBe(pending.envelopeId);
		for (const change of [{ payload: { changed: true } }, { createdAt: new Date(Date.parse(pending.createdAt!) + 1).toISOString() }]) {
			const conflict = await fetch(`${baseUrl}/api/task-relay/v2/send`, { method: "POST", headers: { "content-type": "application/json" },
				body: JSON.stringify({ callerSession: "origin", envelope: { ...sent[0]!.envelope, ...change } }) });
			expect(await conflict.json()).toMatchObject({ ok: false, error: { code: "ENVELOPE_CONFLICT" } });
		}
		expect((await gateway.acknowledgeDelivery({ callerSession: "receiver", envelopeId: pending.envelopeId })).ok).toBe(true);
	} finally {
		store.close(); await server.stop(true); gateway.close(); rmSync(root, { recursive: true, force: true });
	}
});
