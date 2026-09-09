import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { createTaskCore } from "../src/task-core";
import { createTaskStore } from "../src/task-store";
import { createWolfpackTaskRelay } from "../src/wolfpack-task-relay";

// Explicit trusted source only. Private actual HTTP/core/SQLite/gateways, but
// synthetic broker/topology: not production auth, workers, Tailnet or Pi execution.
const wolfpack = process.env.PI_TASKS_WOLFPACK_SOURCE;
const revision = process.env.PI_TASKS_WOLFPACK_REVISION;

test.skipIf(!wolfpack)("actual peer cores: assignment, ACK loss/reopen, intent, canonical fanout and completion", async () => {
  expect(isAbsolute(wolfpack!)).toBe(true);
  expect(revision).toMatch(/^[0-9a-f]{40}$/);
  expect(execFileSync("git", ["rev-parse", "HEAD"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe(revision!);
  expect(execFileSync("git", ["status", "--porcelain", "--untracked-files=no"], { cwd: wolfpack, encoding: "utf8" }).trim()).toBe("");
  const { TaskRelayGateway } = await import(join(wolfpack!, "src/task-relay/gateway.ts"));
  const root = mkdtempSync(join(tmpdir(), "relay-core-peer-gate-"));
  const servers: ReturnType<typeof Bun.serve>[] = [];
  const gateways: Array<{ close(): void }> = [];
  const stores: ReturnType<typeof createTaskStore>[] = [];
  const peers = new Map<string, string>();
  const make = (name: string) => {
    const origin = `https://${name}.tail123.ts.net`;
    const gateway = new TaskRelayGateway({ root: join(root, name), peerOrigin: origin,
      inspectSession: async (selector: string) => ({ ok: true, session: selector, sessionId: selector, projectPath: root, harness: "pi", alive: true }),
      peerFetch: async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
        const url = new URL(String(input)), target = peers.get(url.origin);
        if (!target) throw new Error("fixture origin not configured");
        return fetch(target + url.pathname, init);
      },
    }); gateways.push(gateway);
    const server = Bun.serve({ hostname: "127.0.0.1", port: 0, maxRequestBodySize: 64 * 1024, async fetch(request) {
      const url = new URL(request.url), body: any = request.method === "POST" ? await request.json() : undefined;
      let result;
      switch (url.pathname) {
        case "/api/task-relay/v2/connect": result = await gateway.connect(body); break;
        case "/api/task-relay/v2/resolve": result = await gateway.resolve(body); break;
        case "/api/task-relay/v2/send": result = await gateway.send(body); break;
        case "/api/task-relay/v2/receive": result = await gateway.receive({ callerSession: url.searchParams.get("callerSession")!, cursor: url.searchParams.get("cursor")! }); break;
        case "/api/task-relay/v2/delivery-ack": result = await gateway.acknowledgeDelivery(body); break;
        case "/api/task-relay/v2/peer/receive": result = await gateway.receivePeer(body); break;
        default: throw new Error("unexpected fixture route");
      }
      return Response.json(result, { status: result.ok ? 200 : 409 });
    } }); servers.push(server);
    const baseUrl = `http://127.0.0.1:${server.port}`; peers.set(origin, baseUrl);
    return { gateway, origin, baseUrl };
  };
  try {
    const a = make("a"), b = make("b");
    const ar = createWolfpackTaskRelay({ baseUrl: a.baseUrl, sessionName: "origin", generation: "g" });
    const ackBodies: string[] = [];
    let loseAck = true;
    const lossy = Object.assign(async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      const response = await fetch(input, init);
      if (new URL(String(input)).pathname.endsWith("/delivery-ack")) {
        ackBodies.push(String(init?.body));
        if (loseAck) { loseAck = false; await response.json(); throw new Error("accepted ACK response lost"); }
      }
      return response;
    }, { preconnect: fetch.preconnect }) as typeof fetch;
    const bOptions = { baseUrl: b.baseUrl, sessionName: "receiver", generation: "g", fetch: lossy };
    const br = createWolfpackTaskRelay(bOptions);
    const aStore = createTaskStore({ path: join(root, "a.sqlite") }), bStore = createTaskStore({ path: join(root, "b.sqlite") }); stores.push(aStore, bStore);
    const ac = createTaskCore({ endpoint: await ar.endpoint(), relay: ar, store: aStore });
    let bc = createTaskCore({ endpoint: await br.endpoint(), relay: br, store: bStore });
    await ac.connect(); await bc.connect();
    const target = await a.gateway.resolvePeerEndpoint({ origin: b.origin, endpoint: bc.endpoint });
    if (!target.ok) throw new Error(target.error.code);
    await ac.createTask({ target: target.endpoint, task: "synthetic cross-peer assignment", timeoutMs: 60_000 });
    const page = await b.gateway.receive({ callerSession: "receiver", cursor: "0" });
    if (!page.ok) throw new Error(page.error.code);
    const wire = page.envelopes[0]!;
    const payload = wire.payload as any;
    expect(wire.source.relay).not.toBe(payload.payload.task.origin.relay);
    expect(wire.target.relay).not.toBe(payload.payload.task.target.relay);
    const delivered = await bc.receive();
    expect(delivered).toHaveLength(1);
    const assignment = delivered[0]!;
    const taskId = assignment.envelope.taskId;
    expect(bc.getTask(taskId)).toMatchObject({ origin: wire.source, target: bc.endpoint });
    expect(ac.getTask(taskId)).toMatchObject({ origin: ac.endpoint, target: target.endpoint });
    await expect(bc.acknowledgeRelayDelivery(assignment.cursor)).rejects.toThrow("accepted ACK response lost");
    expect(bStore.getReceiveCursor()).toBe("0");
    bStore.close();
    stores[1] = createTaskStore({ path: join(root, "b.sqlite") });
    const reopened = createWolfpackTaskRelay(bOptions);
    bc = createTaskCore({ endpoint: await reopened.endpoint(), relay: reopened, store: stores[1]! });
    await bc.connect(); // Replays only the already-persisted ACK intent.
    expect(stores[1]!.getReceiveCursor()).toBe(assignment.cursor);
    expect(ackBodies).toHaveLength(2);
    expect(ackBodies[0]).toBe(ackBodies[1]);
    const appValue = { relay: "wolfpack-pi-tasks-v2", id: "opaque-application-reference" };
    await bc.submitIntent({ taskId, type: "task.information", payload: { message: "progress", appValue } });
    expect(await ac.receive()).toEqual([]); // Intent is internal; canonical fanout follows.
    const information = await bc.receive();
    expect(information).toHaveLength(1);
    for (const delivery of information) await bc.acknowledgeRelayDelivery(delivery.cursor);
    expect(bc.getTask(taskId)!.events.at(-1)!.payload.appValue).toEqual(appValue);
    await bc.submitIntent({ taskId, type: "task.completed", payload: { summary: "synthetic completed" } });
    // Includes an earlier origin/self canonical event before the completion intent.
    for (const delivery of await ac.receive()) await ac.acknowledgeRelayDelivery(delivery.cursor);
    for (const delivery of await ac.receive()) await ac.acknowledgeRelayDelivery(delivery.cursor);
    const terminal = await bc.receive();
    expect(terminal).toHaveLength(1);
    for (const delivery of terminal) await bc.acknowledgeRelayDelivery(delivery.cursor);
    expect(ac.getTask(taskId)!.status).toBe("completed");
    expect(bc.getTask(taskId)!.status).toBe("completed");
    expect(bc.getTask(taskId)!.events.map(e => e.eventId)).toEqual(ac.getTask(taskId)!.events.map(e => e.eventId));
    expect(bc.getTask(taskId)!.events).toHaveLength(3);
    expect(await ac.receive()).toEqual([]);
    expect(await bc.receive()).toEqual([]);
  } finally {
    for (const store of stores) store.close();
    for (const gateway of gateways) gateway.close();
    await Promise.all(servers.map(server => server.stop(true)));
    rmSync(root, { recursive: true, force: true });
  }
});
