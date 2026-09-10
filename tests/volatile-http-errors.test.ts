import { expect, test } from "bun:test";
import { createTaskStore } from "../src/task-store";
import { createVolatileTaskSession } from "../src/volatile-task-session";
const profile = "volatile-v1", epoch = "00000000-0000-4000-8000-000000000001";
const endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };
const reply = (value: unknown) => Response.json({ ok: true, profile, epoch, value });
const connected = () => reply({ kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() });
function setup(handler: (body: any) => Response) {
  const store = createTaskStore(); let calls = 0;
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => { calls++; return handler(JSON.parse(String(init?.body))); }, { preconnect: fetch.preconnect }) as typeof fetch;
  const options = { url: "http://127.0.0.1:1/volatile", callerSession: "fixture", store, fetch: fetcher };
  const session = createVolatileTaskSession(options);
  return { store, session, options, calls: () => calls, close: () => { session.close(); store.close(); } };
}

test("permanent profile refusal retires/quarantines only the current lifetime without an epoch or downgrade", async () => {
  for (const selected of ["durable-v2", profile]) {
    let connects = 0;
    const f = setup(body => body.operation === "connect" ? (++connects, connected()) : body.operation === "resolve" ? reply({ kind: "resolved", endpoint })
      : Response.json({ ok: false, profile: selected, error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }));
    try {
      const core = await f.session.connect();
      await expect(core.createTask({ target: endpoint, task: "preserve identity", timeoutMs: 60_000 })).rejects.toMatchObject({ code: "RELAY_RESET", retryable: false });
      expect(f.session.status().state).toBe("reset"); expect(f.store.getRelayTransportBinding()).toMatchObject({ epoch, endpoint, reset: true });
      expect(f.store.outbox("pending")).toEqual([]); expect(f.store.outbox("accepted")).toEqual([]); expect(f.store.quarantinedOutbox()).toHaveLength(1);
      expect(core.listTasks()).toHaveLength(1);
      const reopened = createVolatileTaskSession(f.options), before = f.calls();
      try { await expect(reopened.connect()).rejects.toMatchObject({ code: "RELAY_REBIND_REQUIRED", retryable: false }); expect(f.calls()).toBe(before); }
      finally { reopened.close(); }
      expect(connects).toBe(1);
    } finally { f.close(); }
  }
  for (const selected of ["durable-v2", profile]) {
    const unbound = setup(() => Response.json({ ok: false, profile: selected, error: { code: "RELAY_PROFILE_REQUIRED", retryable: false } }, { status: 409 }));
    try { await expect(unbound.session.connect()).rejects.toMatchObject({ code: "RELAY_PROFILE_REQUIRED", retryable: false }); expect(unbound.store.getRelayTransportBinding()).toBeUndefined(); }
    finally { unbound.close(); }
  }
});

test("bounded pre-admission epochless errors retain their codes but never authorize success or arbitrary errors", async () => {
  for (const [code, retryable] of [["RELAY_CAPACITY", true], ["RELAY_UNAVAILABLE", true], ["INVALID_REQUEST", false], ["PEER_POLICY_REQUIRED", false]] as const) {
    const f = setup(body => body.operation === "connect" ? connected() : Response.json({ ok: false, profile, error: { code, retryable: false } }, { status: 503 }));
    try {
      const core = await f.session.connect();
      await expect(core.receive()).rejects.toMatchObject({ code, retryable, ...(code === "RELAY_UNAVAILABLE" && { details: { mayHaveBeenDelivered: true } }) });
      expect(f.store.getReceiveCursor()).toBe("0"); expect(f.session.status().state).toBe("ready");
    } finally { f.close(); }
  }
  for (const invalid of [
    { ok: false, profile, error: { code: "UNRECOGNIZED", retryable: false } },
    { ok: false, profile, epoch: "malformed", error: { code: "RELAY_CAPACITY", retryable: true } },
    { ok: true, profile, value: { kind: "page", deliveries: [], nextCursor: "0", hasMore: false } },
  ]) {
    const f = setup(body => body.operation === "connect" ? connected() : Response.json(invalid));
    try { const core = await f.session.connect(); await expect(core.receive()).rejects.toMatchObject({ code: "INVALID_RESPONSE", retryable: true }); expect(f.store.getReceiveCursor()).toBe("0"); }
    finally { f.close(); }
  }
});
