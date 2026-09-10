import { createHmac, randomBytes } from "node:crypto";
import { TaskProtocolError } from "./task-protocol";

/** Existing owner-provided JWT configuration, only for the local control server.
 * Never copy this credential to an arbitrary programmatic HTTPS relay URL. */
export function wolfpackLocalHeaders(url: string, input?: RequestInit["headers"], env: Readonly<Record<string, string | undefined>> = process.env): Headers {
  const headers = new Headers(input), target = new URL(url);
  if (headers.has("authorization") || target.protocol !== "http:" || !["127.0.0.1", "[::1]", "localhost"].includes(target.hostname)) return headers;
  const secret = env.WOLFPACK_JWT_SECRET?.trim();
  if (!secret) return headers;
  if (secret.length < 32) throw new TaskProtocolError("RELAY_AUTH_REQUIRED", "configured Wolfpack JWT secret must contain at least 32 characters", { retryable: false });
  const now = Math.floor(Date.now() / 1000);
  const payload = { iat: now, exp: now + 60, jti: randomBytes(8).toString("hex"),
    ...(env.WOLFPACK_JWT_ISSUER?.trim() && { iss: env.WOLFPACK_JWT_ISSUER.trim() }),
    ...(env.WOLFPACK_JWT_AUDIENCE?.trim() && { aud: env.WOLFPACK_JWT_AUDIENCE.trim() }) };
  const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const data = `${encode({ alg: "HS256", typ: "JWT" })}.${encode(payload)}`;
  headers.set("authorization", `Bearer ${data}.${createHmac("sha256", secret).update(data).digest("base64url")}`);
  return headers;
}
