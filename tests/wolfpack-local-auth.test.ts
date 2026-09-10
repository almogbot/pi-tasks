import { expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { wolfpackLocalHeaders } from "../src/wolfpack-local-auth";
import { createTaskStore } from "../src/task-store";
import { createVolatileTaskSession } from "../src/volatile-task-session";

const env = { WOLFPACK_JWT_SECRET: "local-test-only-secret-at-least-thirty-two-characters", WOLFPACK_JWT_ISSUER: "issuer", WOLFPACK_JWT_AUDIENCE: "audience" };
test("local configured JWT uses current short-lived claims and is never sent to arbitrary relay origins", () => {
  const header = wolfpackLocalHeaders("http://127.0.0.1:1/api/task-relay/volatile-v1", undefined, env).get("authorization")!;
  const [head, payload, signature] = header.slice("Bearer ".length).split(".");
  expect(signature).toBe(createHmac("sha256", env.WOLFPACK_JWT_SECRET).update(`${head}.${payload}`).digest("base64url"));
  expect(JSON.parse(Buffer.from(head!, "base64url").toString())).toEqual({ alg: "HS256", typ: "JWT" });
  const claims = JSON.parse(Buffer.from(payload!, "base64url").toString());
  expect(claims.iss).toBe("issuer"); expect(claims.aud).toBe("audience"); expect(claims.exp - claims.iat).toBe(60);
  expect(claims.iat).toBeLessThanOrEqual(Math.floor(Date.now() / 1000));
  expect(wolfpackLocalHeaders("https://peer.tail123.ts.net/relay", undefined, env).has("authorization")).toBe(false);
  expect(wolfpackLocalHeaders("http://127.0.0.1.evil.invalid/relay", undefined, env).has("authorization")).toBe(false);
  expect(wolfpackLocalHeaders("http://localhost:1", { authorization: "Bearer explicit" }, env).get("authorization")).toBe("Bearer explicit");
  expect(() => wolfpackLocalHeaders("http://localhost:1", undefined, { WOLFPACK_JWT_SECRET: "short" })).toThrow("at least 32");
});

test("an HTTP 401 reports authentication failure without retiring or rebinding a healthy relay lifetime", async () => {
  const store = createTaskStore();
  const epoch = "00000000-0000-4000-8000-000000000001", endpoint = { relay: "wolfpack-pi-tasks-v2", id: "00000000-0000-4000-8000-000000000002" };
  const fetcher = Object.assign(async (_url: unknown, init?: RequestInit) => JSON.parse(String(init?.body)).operation === "connect"
    ? Response.json({ ok: true, profile: "volatile-v1", epoch, value: { kind: "connected", endpoint, leaseExpiresAt: new Date(Date.now() + 60_000).toISOString() } })
    : Response.json({ error: "unauthorized" }, { status: 401 }), { preconnect: fetch.preconnect }) as typeof fetch;
  const session = createVolatileTaskSession({ url: "http://127.0.0.1:1/relay", callerSession: "fixture", store, fetch: fetcher });
  try {
    const core = await session.connect(); const binding = store.getRelayTransportBinding();
    await expect(core.receive()).rejects.toMatchObject({ code: "RELAY_AUTH_REQUIRED", retryable: false });
    expect(store.getRelayTransportBinding()).toEqual(binding); expect(session.status().state).toBe("ready"); expect(store.getReceiveCursor()).toBe("0");
  } finally { session.close(); store.close(); }
});
