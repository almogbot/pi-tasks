# pi-tasks

`pi-tasks` is an endpoint-owned Pi extension for Wolfpack relay v2. It keeps task state in each Pi endpoint's local SQLite store and uses Wolfpack as a content-blind relay. It never writes assignments to terminals. Wolfpack's [relay v2 control-api contract](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary) is canonical for the transport boundary.

## endpoint-owned relay

The default extension uses the memory-owned local Wolfpack relay, never the in-memory conformance fixture or a silent legacy fallback. It negotiates `volatile-v1` at `POST /api/task-relay/volatile-v1`, and keeps endpoint task state at `~/.pi/tasks/v2/sessions/<sha256(WOLFPACK_SESSION_NAME)>/tasks.sqlite`. No transport opt-in flag is needed.

**Coordinated cutover branch, not a released installation:** this extension requires a compatible Wolfpack memory-owned server. Server default selection, discovery/readiness, verified federation, packaged release and installed rollout must be coordinated before publication as a normal release. An old/durable server is refused, not silently adopted.

Set `WOLFPACK_SESSION_NAME` for every Pi process. The adapter uses `WOLFPACK_PORT` when the local control port differs from `18790`; `WOLFPACK_SESSION_NAME` resolves the active Pi process to its relay endpoint. After the target extension registers, run `wolfpack session status <session> --json` and read its `taskEndpoint`. Pass that opaque `{ relay, id }` value unchanged; do not derive it from a session name, broker ID, terminal label, output, or prose.

### minimal valid v2 send envelope

```json
{
  "to": {
    "relay": "wolfpack-pi-tasks-v2",
    "id": "target-opaque-endpoint-id"
  },
  "task": "implement the narrow change and run focused tests"
}
```

The `agent_task_send` schema is exactly `to`, `task`, and optional `timeoutMs`. Unsupported fields are rejected before persistence: a pre-persistence validation rejection creates no task. If creation status is uncertain after a transport failure, idempotency remains necessary; inspect the structured task ID rather than creating an unrelated replacement.

Wolfpack owns bounded in-memory mailbox delivery and destination-confirmed forwarding; a relay restart can lose even accepted mail. Pi owns lifecycle, logical event order, receipts, and local SQLite history; that history is not relay recovery. `agent_task_send` returns after relay acceptance only, not Pi insertion or model execution. The receiver inserts model-visible events as structured `pi-tasks-event` custom messages through Pi's safe `deliverAs: "followUp"` queue and records structured `{ taskId, eventId }` insertion evidence. Replay after Pi has structurally recorded an event cannot create another logical receipt. `createInMemoryTaskRelay` remains exported solely as a deterministic conformance fixture.

## retry-content stability (0.1.9)

New core-originated internal `RelayEnvelope` records persist an immutable ISO
`createdAt` alongside their outbox payload, before relay submission. Assignment,
intent and canonical-event envelopes all use that stored value on every Wolfpack
send, including after adapter/core/store restart. This is **not** a new
`agent_task_send` tool argument or a change to canonical task event authority.
The existing outbox serialization owns the metadata; no new database/schema or
relay spool is introduced. Wolfpack still hashes the full wire envelope, including
its timestamp: genuinely changed timestamp or payload must continue to conflict.

**Upgrade caveat:** pending envelopes written by older versions have no original
wire timestamp. The old adapter generated it on each attempt, so neither a new
wall-clock value nor an in-memory cache can reconstruct an already accepted
request safely. Missing/invalid metadata now fails before any relay request with
non-retryable `INVALID_RELAY_METADATA`; the core quarantines that pending envelope
unchanged using its existing delivery-blocked mechanism. Already accepted outbox
records and canonical task status are not rewritten. Inspect affected work and
its possibly-accepted outcome before choosing an explicit replacement; do not
blindly mint a new identity or silently backfill timestamps. Existing endpoint
rotation handling remains separate.

This correction alone does not activate Wolfpack's memory-owned transport. Sparse
cursor negotiation, live-process epoch reset and volatile forwarding outcomes
still require coordinated changes with Wolfpack #351; old forwarding acceptance
and cursor assumptions are not claimed fixed here.

### real-relay retry regression

The ordinary tests use private fixtures. An optional cross-repository test also
runs the real core and adapter against a privately rooted real Wolfpack gateway
over loopback HTTP, loses a response after acceptance, reopens the endpoint store
and recreates the adapter, then verifies duplicate acceptance and genuine content
conflicts. Select a trusted, tracked-clean Wolfpack checkout and exact revision:

```bash
PI_TASKS_WOLFPACK_SOURCE=/absolute/wolfpack-checkout \
PI_TASKS_WOLFPACK_REVISION=<full-commit-id> \
bun test tests/wolfpack-real-relay-retry.test.ts
```

Without that explicit source selection the cross-repository test is skipped.
It does not contact an installed server, live broker or Tailnet peer, and is not
an end-to-end Pi/model execution, two-host, or volatile-profile test.

## endpoint-view and delivery-checkpoint corrections (0.1.9)

Peer relay aliases are local routing names, not globally portable endpoint
identities. On receipt, the adapter projects only protocol-defined assignment and
canonical-event references into the recipient's namespace. The transport header
supplies the authenticated source peer route and local destination; sender-local
source IDs, peer-target IDs, and assignment/event reference agreement must match
before projection. Arbitrary application payloads, event IDs/order, outgoing wire
bytes, and historical task records are unchanged. Core ownership comparisons stay
strict. This relies on Wolfpack enforcing its trusted-peer ingress policy; the
adapter does not authenticate a peer by trusting a payload label.

Delivery ACKs acknowledge one envelope, **not** a cursor prefix. The core now
tracks observed pending cursor→envelope bindings and a conservative checkpoint in
endpoint-scoped `relay_state` metadata (existing SQLite schema 5, no migration).
A later automatic intent ACK cannot advance the checkpoint past an earlier
unacknowledged assignment. ACK intents are persisted before the request and only
those requested ACKs are retried on connect/receive after a response loss or
reopen. They use the durable envelope ID, not a receive-time RAM cache. Pending
bindings are removed after confirmation; the checkpoint/high-water mark does not
retain an individual completed-ACK history. Retired endpoint bindings are fenced.
Existing potentially unsafe checkpoints are **not** silently rewound or repaired.

The in-memory conformance fixture now models individual ACKs too. Optional
`tests/wolfpack-real-peer-core.test.ts` uses the same explicit trusted Wolfpack
source/revision variables as the retry test above. It exercises actual adapters,
cores, SQLite and two current v2 gateways over private loopback HTTP, including ACK
response loss/reopen, both-direction task messages and canonical/self fanout.
Broker/topology are synthetic: this is not live Tailnet/auth, compiled-worker,
physical-device or Pi/model execution proof.

Those 0.1.9 corrections alone did not activate the memory-owned profile. This
cutover branch now uses the session below in the normal extension lifecycle.

## memory-owned transport lifecycle

`createConfiguredTaskCore()` owns the normal transport and its SQLite store.
Startup connects, the extension polls every five seconds and at agent settlement,
and shutdown fences new work, aborts transport requests, waits for active core
calls to settle, then closes SQLite. Late setup cannot resurrect a stopped
lifecycle. A fresh start reopens the persisted binding, not an indefinitely cached
core. Shutdown does not claim remote delivery or ACKs; the remote lease expires
without renewal.

After a relay reset (or upgrade from an existing legacy endpoint), task tools stay
stopped and the status points to `/task-relay-rebind`. Run it without arguments to
read the loss warning. Only `/task-relay-rebind --accept-relay-loss` explicitly
retires the old scope and binds a fresh endpoint. Pending old-source work is
quarantined; accepted/history records remain unchanged. This command cannot
recover accepted mail, adopt historical tasks or rearm exhausted envelope IDs.
Inspect unresolved work first. Automatic polling never invokes rebind.

`createVolatileTaskSession({ url, callerSession, store })` remains the lower-level
API for caller-owned stores. The URL must be explicitly trusted HTTPS or loopback
endpoint ingress. Neither factory falls back to a different transport.

- `connect()` negotiates and durably binds profile, epoch, endpoint, generation,
  caller and URL before exposing a task core. A matching process reopen renews the
  same binding; every operation includes the exact epoch and endpoint.
- Inbox deliveries retain their actual decimal cursors, including sparse values
  beyond JavaScript's safe integer range. Requests are capped at 50 deliveries;
  malformed, oversized or inconsistent pages are rejected, never sliced with a
  cursor advanced past omitted deliveries.
- Successful sends require a matching destination-confirmed acceptance, never a
  pending-forwarding receipt. Terminal unconfirmed/expired/conflicting delivery
  errors use existing outbox quarantine and preserve unknown-outcome evidence.
- Reset/expired registration, changed endpoint/epoch, or an explicit permanent
  profile refusal from a known server profile stops that session and persists a
  reset marker (including rollback to a durable server without a volatile epoch). Pending prior-source envelopes are quarantined unchanged;
  accepted records and task authority/history are retained.
- `rebind()` is an **explicit owner action**, not an automatic retry. Inspect the
  reset and possibly delivered work first. It retires the old binding and creates
  a fresh generation/endpoint with a fresh cursor scope. Retained old core handles
  issued by this API cannot mutate the store; the successor cannot issue intents for historical tasks
  it does not own. No old task identities or exhausted envelope IDs are rearmed.
- `status()` reports local binding state, not model readiness. `close()` stops the
  transport; the caller must then close its own store. Request/body deadlines and
  byte/concurrency bounds also apply when injected transports ignore abort.
- Known pre-admission HTTP errors may omit an epoch: capacity, unavailable,
  invalid request and disabled peer policy remain errors, never confirmations.
  Unavailable outcomes remain possibly delivered. Success, unknown errors and
  malformed epoch values do not gain an epoch-validation bypass.

Late replies and competing controllers cannot overwrite a successor binding or
quarantine its work. This is not an exclusive cross-process broker-registration
lease; a racing registration can still force an explicit reset on a subsequent
operation. No availability or task recovery across such races is promised.

Profile-bound stores cannot silently reopen through the legacy `createWolfpackTaskCore` factory. Delivery
checkpoints are now scoped by profile/epoch as well as endpoint. These are metadata
in the existing endpoint SQLite store, not relay recovery storage or a schema
migration. No terminal input, installed extension, provider, broker or service is
changed by importing the new API.

The optional `tests/volatile-real-worker.test.ts` uses the existing explicit
trusted-source/revision test variables. It exercises actual adapter/core/SQLite,
loopback HTTP and two real volatile workers, including withheld peer confirmation,
sparse ACK gaps, ACK response loss/reopen, worker epoch replacement, durable reset
and explicit rebind. HTTP ingress and broker/topology are private fixtures: this
is not production auth/discovery/schema, compiled release packaging, physical
Tailnet or Pi/model execution proof. Production cutover and full-path performance
measurements remain separate work. `tests/volatile-extension-worker.test.ts` also
uses the pinned source variables to run normal default extension hooks/tools and
explicit rebind against a real memory worker with a private HTTP/broker fixture.
It checks shutdown/reopen, actual insertion/ACK, epoch reset and preserved history;
it is not a real Pi process/model or production HTTP-auth test.

## delegation workflow

Configure Pi role models with `WOLFPACK_IMPLEMENTER_MODEL` and `WOLFPACK_REVIEWER_MODEL`; they default to `openai-codex/gpt-5.6-terra` and `openai-codex/gpt-5.6-sol`. Explicit user or project choices override those defaults.

```bash
IMPLEMENTER_MODEL="${WOLFPACK_IMPLEMENTER_MODEL:-openai-codex/gpt-5.6-terra}"
REVIEWER_MODEL="${WOLFPACK_REVIEWER_MODEL:-openai-codex/gpt-5.6-sol}"
```

1. create or select a role session. For a new endpoint worker, use its explicit root and resolved role model without a startup assignment: `wolfpack agent spawn --project-dir /absolute/worktree --name <task-role> --model "$IMPLEMENTER_MODEL" --task-worker --readiness-timeout-ms 30000 --json` (or `"$REVIEWER_MODEL"` for review). Put all worker instructions in `agent_task_send.task`. This Pi-only mode rejects prompts/plans and `--notify-parent`.
2. read the ready `taskEndpoint` from creation success, or verify an existing role's structured liveness, root, harness, and endpoint through `wolfpack session status <stable-session-id> --json`. `TASK_WORKER_PREFLIGHT_FAILED` precedes creation; `TASK_WORKER_NOT_READY` retains `createdSession` and `cleanup`. Inspect an `unconfirmed` cleanup by exact stable ID before retrying. Registration is not model/task execution evidence; report unsupported readiness instead of silently falling back.
3. call `agent_task_send` with that endpoint and the complete instructions. Continue useful independent coordinator work; when none remains, yield the current turn and let structured task follow-up wake the parent. Do not poll `agent_task_status` or `agent_task_inbox` merely to wait; reserve them for concrete progress evidence or recovery, and call `agent_task_wait` only when explicitly asked to block.
4. use `agent_task_message` for durable questions, answers, and information. The receiver calls `agent_task_done` as its final action; no completion prose follows.
5. independently verify the result, call `agent_task_ack({ taskId })` once for that terminal task, then explicitly retain or close only the role sessions the parent spawned.

Coordinator-capable agents may delegate further when justified, and the spawning coordinator owns each child's lifecycle. Endpoint assignments require terminal completion and one `agent_task_ack`; full-startup children have no task ID, so use their explicit completion/block notification and parent verification instead. Then deliberately retain the child or run `wolfpack kill <stable-session-id> --json`, and verify that exact ID is absent from `wolfpack list --json`. Never use `wolfpack session send`, `/exit`, or `/quit` for cleanup; terminal input is not Wolfpack teardown.

## worker-only execution gate

Set `PI_TASK_WORKER=1` only when launching a task-only worker. The exact value enables a fail-closed model `tool_call` gate; an absent value or any other value leaves ordinary interactive Pi behavior unchanged. Before assignment, only `agent_task_inbox`, `agent_task_status`, and `agent_task_wait` are allowed. Other current and future model tools are blocked with `PI_TASK_WORKER_ASSIGNMENT_REQUIRED`. Explicit user `!`/`!!` shell commands are outside Pi's model `tool_call` event and are not intercepted.

`PI_TASK_WORKER=1` sessions are leaf roles. Workers cannot call `agent_task_send`, `agent_task_cancel`, or `agent_task_ack`; those attempts are blocked with `PI_TASK_WORKER_COORDINATION_FORBIDDEN`. `agent_task_message` is limited to the eligible incorporated assignment named by its input `taskId`. Generic role-orchestration guidance applies only to non-worker coordinators.

The gate opens only when the current Pi session contains a structured `pi-tasks-event` entry for `task.created` and the same event belongs to a locally persisted active task targeted to the current endpoint. Rendered prompt text, terminal output, unknown events, foreign tasks, and stale terminal tasks do not authorize execution. On restart, durable session evidence reopens the gate only while the matching local task remains eligible.

Preflighting `agent_task_done` marks that task as closing before sibling calls are preflighted. Ordinary tools remain blocked for a closing, pending-terminal, accepted-terminal, or `delivery_blocked` task; an idempotent `agent_task_done` retry for that same assigned task remains allowed. Another independently active assignment can still authorize work.

## terminal delivery and acknowledgment

A receiver task snapshot reports terminal transport separately as `terminalDelivery`: `not_submitted`, `pending`, `accepted`, or `delivery_blocked`. The blocked variant includes stable intent/envelope identities, origin endpoint, timestamp, and structured non-retryable relay error. Canonical task `status` remains origin-owned and is never changed to `delivery_blocked`. Retryable failures keep the same pending terminal intent and envelope; permanent failures remain inspectable and are not rebound to a successor endpoint without a separate authenticated Wolfpack contract.

`agent_task_ack` is valid only for a terminal origin-owned task. Sequential, concurrent, and restart retries reuse one durable `task.parent_acknowledged` event and the same destination envelope identities while retrying pending physical delivery.

Report source modifications in `result.changedFiles`. Artifacts are receiver-project-relative regular files for a parent to inspect, not a changed-file list:

```json
{ "result": { "changedFiles": ["src/extension.ts"] }, "artifacts": [{ "path": "verification/task-2.md" }] }
```

## development

```bash
bun install
bun test
bun run typecheck
```
