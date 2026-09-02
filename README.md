# pi-tasks

`pi-tasks` is a Pi extension client for Wolfpack task transports. The default v2 adapter keeps endpoint-owned Pi task state locally and uses Wolfpack as a content-blind relay; the retained v1 adapter uses Wolfpack's machine-global `~/.wolfpack/tasks` store. Neither adapter writes assignments to terminals. Wolfpack's [relay v2 control-api contract](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary) is canonical for the default transport, while its [task gateway guide](https://github.com/almogdepaz/wolfpack/blob/main/docs/task-gateway.md) documents the explicit v1 compatibility transport.

## v2 endpoint-owned relay

The default extension uses the configured local Wolfpack relay v2 adapter, never the in-memory conformance relay. It registers an opaque endpoint with `POST /api/task-relay/v2/connect`, stores endpoint-owned task state at the deterministic per-session path `~/.pi/tasks/v2/sessions/<sha256(WOLFPACK_SESSION_NAME)>/tasks.sqlite`, and exchanges opaque relay envelopes through Wolfpack. The adapter needs a Wolfpack release exposing the stable [relay v2 control-api contract](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary); a v1-only gateway is incompatible and does not silently fall back.

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

The default `agent_task_send` schema is exactly `to`, `task`, and optional `timeoutMs`. v1-only context, role, metadata, preflight, idempotency, and completion-prompt fields are not silently accepted or translated.

Wolfpack's relay owns durable mailbox delivery and peer forwarding; Pi owns task lifecycle, logical event order, receipts, and its local SQLite state. `agent_task_send` returns after relay acceptance only, not Pi insertion or model execution. The receiver inserts model-visible events as structured `pi-tasks-event` custom messages through Pi's safe `deliverAs: "followUp"` queue and records structured `{ taskId, eventId }` insertion evidence. Replay after Pi has structurally recorded an event cannot create an additional logical receipt. `createInMemoryTaskRelay` remains exported solely as a deterministic conformance fixture.

### v2 delegation workflow

Configure Pi role models with `WOLFPACK_IMPLEMENTER_MODEL` and `WOLFPACK_REVIEWER_MODEL`; they default to `openai-codex/gpt-5.6-terra` and `openai-codex/gpt-5.6-sol`, respectively. Explicit user or project choices override those defaults.

```bash
IMPLEMENTER_MODEL="${WOLFPACK_IMPLEMENTER_MODEL:-openai-codex/gpt-5.6-terra}"
REVIEWER_MODEL="${WOLFPACK_REVIEWER_MODEL:-openai-codex/gpt-5.6-sol}"
```

1. create or select a role session. For a disposable worker, omit an initial assignment prompt and pass the resolved role model: `wolfpack agent spawn <project> --name <task-role> --model "$IMPLEMENTER_MODEL" --json` (or `"$REVIEWER_MODEL"` for review). Put all worker instructions in `agent_task_send.task` so the new Pi process can become idle before assignment.
2. verify structured session readiness, wait for extension registration, and read `taskEndpoint` from `wolfpack session status <session> --json`.
3. call `agent_task_send` with that endpoint and the complete task instructions. Keep working; use `agent_task_status` or `agent_task_inbox` for structured evidence, and call `agent_task_wait` only when the user explicitly asks to block.
4. use `agent_task_message` for durable questions, answers, and information. The receiver calls `agent_task_done` as its final action; no completion prose follows.
5. independently verify the result, call `agent_task_ack({ taskId })` once for that terminal task, then explicitly retain or close only the role sessions the parent spawned.

Coordinator-capable agents may delegate further when justified, and the spawning coordinator owns each child's lifecycle. After terminal completion and acknowledgment, deliberately retain the child or run `wolfpack kill <stable-session-id> --json`, then verify that exact ID is absent from `wolfpack list --json`. Never use `wolfpack session send`, `/exit`, or `/quit` for cleanup; terminal input is not Wolfpack teardown.

### v2 worker-only execution gate

Set `PI_TASK_WORKER=1` only when launching a task-only worker. The exact value enables a fail-closed model `tool_call` gate; an absent value or any other value leaves ordinary interactive Pi behavior unchanged. Before assignment, only `agent_task_inbox`, `agent_task_status`, and `agent_task_wait` are allowed. Other current and future model tools are blocked with the stable reason `PI_TASK_WORKER_ASSIGNMENT_REQUIRED`. Explicit user `!`/`!!` shell commands are outside Pi's model `tool_call` event and are not intercepted.

`PI_TASK_WORKER=1` sessions are leaf roles. Workers cannot call `agent_task_send`; workers cannot call `agent_task_cancel`; workers cannot call `agent_task_ack`. These coordinator-tool attempts are blocked with the stable reason `PI_TASK_WORKER_COORDINATION_FORBIDDEN`, not the pre-assignment reason. `agent_task_message` is limited to the eligible incorporated assignment named by its input `taskId`. Generic role-orchestration guidance applies only to non-worker coordinators.

The gate opens only when the current Pi session contains a structured `pi-tasks-event` entry for `task.created` and the same event belongs to a locally persisted active task targeted to the current endpoint. Rendered prompt text, terminal output, unknown events, foreign tasks, and stale terminal tasks do not authorize execution. On restart, durable session evidence reopens the gate only while the matching local task remains eligible.

Preflighting `agent_task_done` marks that task as closing before sibling calls are preflighted. Ordinary tools remain blocked for a closing, pending-terminal, accepted-terminal, or `delivery_blocked` task; an idempotent `agent_task_done` retry for that same assigned task remains allowed. Another independently active assignment can still authorize work.

### v2 terminal delivery and acknowledgment

A receiver task snapshot reports terminal transport separately as `terminalDelivery`: `not_submitted`, `pending`, `accepted`, or `delivery_blocked`. The blocked variant includes the stable intent/envelope identities, origin endpoint, timestamp, and structured non-retryable relay error. Canonical task `status` remains origin-owned and is never changed to `delivery_blocked`. Retryable failures keep the same pending terminal intent and envelope; permanent failures remain inspectable and are not rebound to a successor endpoint without a separate authenticated Wolfpack contract.

`agent_task_ack` is valid only for a terminal origin-owned task. Sequential, concurrent, and restart retries reuse one durable `task.parent_acknowledged` event and the same destination envelope identities while still retrying any pending physical delivery.

## v1 gateway adapter (retained)

The v1 gateway client is available only through the published `@sgtbeatdown/pi-tasks/v1-compat-extension` entry. This is an explicit opt-in: after resolving the installed package root, add `src/v1-compat-extension.ts` as an extension path in Pi settings instead of loading the package default. The package manifest loads only `src/extension.ts` (v2); v2 does not fall back to v1 when its relay is unavailable or incompatible. Its v1 behavior and operational requirements are documented below.

## requirements and trust boundary

- a reachable local Wolfpack v1 task gateway on every participating machine;
- this package loaded in every participating Pi process;
- `WOLFPACK_SESSION_NAME` set by Wolfpack so the local gateway can resolve the caller to a stable broker ID; and
- `WOLFPACK_PORT` when the local control port is not the default `18790`.

The boundary is trusted local processes and trusted Tailnet machines. Pi only calls its own local gateway. The gateways perform direct fetch federation; Pi never fetches a peer directly. v1 adds no task capability authentication. Normal Wolfpack global JWT behavior is not bypassed, and JWT federation is unsupported: peer delivery fails clearly when credentials would be required.

## task address and send

A target is always `{ machine, sessionId }`, where `sessionId` is the stable opaque broker ID returned by Wolfpack session control. Use `machine: "local"` for same-machine work. For a peer, use the receiver's canonical HTTPS Tailnet origin, for example `https://worker.example.ts.net`; do not supply a hostname label, path, query, fragment, credentials, unexpected port, or arbitrary HTTPS URL.

```json
{
  "to": { "machine": "local", "sessionId": "receiver-broker-id" },
  "task": "implement the narrow change and run focused tests",
  "context": {
    "summary": "## constraints and preferences\n- keep scope narrow\n## key decisions\n- preserve the public contract",
    "refs": [{ "path": "src/extension.ts", "purpose": "affected tool wiring" }]
  },
  "metadata": { "phaseId": "phase-1", "issueId": "task-3", "verificationTier": "focused" },
  "onCompletePrompt": "review the receiver diff before reporting completion",
  "timeoutMs": 1800000
}
```

### v1 minimal valid send envelope

```json
{
  "to": {
    "machine": "local",
    "sessionId": "receiver-broker-id"
  },
  "task": "implement the narrow change and run focused tests"
}
```

`INVALID_REQUEST.error.path` is an optional RFC 6901 JSON Pointer to the rejected send field, for example `/preflight/requiredProject`; use it rather than parsing error prose. A pre-persistence validation rejection creates no task, so correct the field and retry. If creation status is uncertain, idempotency remains necessary.

Remote addressing changes only `to.machine`:

```json
{ "to": { "machine": "https://worker.example.ts.net", "sessionId": "receiver-broker-id" }, "task": "review the cited diff" }
```

Context is optional, curated Markdown plus deliberately selected ref metadata. The extension renders refs but never reads or copies their contents. Relative refs resolve in the receiver project; absolute refs are same-machine only and must remain inside the authoritative parent or receiver project root. Missing refs are warnings, not copied content or transcript transfer.

Initial limits are 16 KiB UTF-8 each for task instructions and context summary, 48 KiB for the assignment envelope, and 64 KiB for an HTTP request body. The initial size limits require representative payload benchmarking before adjustment.

`agent_task_send` waits for durable gateway acceptance, not adapter insertion or task execution; delivery remains pending until `task.delivered`. A remote initial send has one initial attempt: the receiver persists a provisional receipt, then receives sender confirmation before Pi can see the assignment. Later peer confirmation, messages, terminal updates, cancellation, delivery notices, and parent acknowledgment have four total attempts: the initial attempt plus retries around 1, 2, and 4 seconds with jitter. Exhaustion is a visible local delivery failure, not a background queue or offline dispatch promise.

Normal delegation is fire-and-forget. Use `agent_task_wait` only when the user explicitly asks to block.

## tools, messages, and acknowledgment

| tool | purpose |
| --- | --- |
| `agent_task_send` | create a durable gateway assignment |
| `agent_task_status` | read task state, history, result, and warnings |
| `agent_task_wait` | explicitly poll a task until terminal or timeout |
| `agent_task_inbox` | read task events without acknowledgment |
| `agent_task_ack` | acknowledge one independently verified terminal task |
| `agent_task_message` | send a durable `question`, `answer`, or `information` event |
| `agent_task_cancel` | request cancellation |
| `agent_task_done` | receiver-only terminal completion; terminates the tool turn |

Use `agent_task_message` for clarification instead of terminal prose. Only one question can be unresolved per task; answers link to that question. Information is durable and does not change task state. An accepted receiver question terminates that receiver turn; a parent question does not.

Report source modifications in `result.changedFiles`. Artifacts are receiver-project-relative regular files for a parent to inspect, not a changed-file list:

```json
{ "result": { "changedFiles": ["src/extension.ts"] }, "artifacts": [{ "path": "verification/task-2.md" }] }
```

See the canonical Wolfpack artifact contract for provenance, containment, and warning behavior.

The sender gateway owns canonical event order, timeout, and terminal state. The first accepted terminal event wins. Sender timeout triggers best-effort remote cancellation; late terminals remain diagnostics. After independently verifying one terminal result, call `agent_task_ack({ taskId })`. Remote acknowledgment is two-phase: the receiver durably confirms pending acknowledgment before the sender records final parent acknowledgment. If that delivery fails, the task remains visible and a later explicit acknowledgment repairs it with the same event ID.

## structured delivery and replay

The extension polls the local gateway every five seconds. With no pending message, it submits each visible event as a structured custom message using Pi's safe `deliverAs: "followUp"` mode: idle Pi starts immediately, while an active turn finishes before the queued event starts the next turn. It never steers or interrupts active work. Delivery remains pending until `task.delivered`. The `{ taskId, eventId }` details let task context participate in the session without parsing prose.

On start or resume it rebuilds incorporated IDs from the full durable session entry set, not only active prompt context. A session gets only missing events; a new Pi session in the same Wolfpack PTY gets complete active history. The receiver records gateway delivery only after the structured insertion exists. A restart after insertion and before acknowledgment therefore acknowledges the existing entry without duplicate injection. Unknown model-visible events fail closed without advancing the cursor. Gateway delivery is at-least-once; this does not claim exactly-once model execution.

## parent workflow

1. create or select a Wolfpack Pi session with the canonical session-control workflow and retain its stable broker ID. Spawn a disposable worker without an initial assignment prompt using `wolfpack agent spawn <project> --name <task-role> --model "$IMPLEMENTER_MODEL" --json` (or `"$REVIEWER_MODEL"` for review) so it reaches idle before assignment; put worker instructions in `agent_task_send.task`.
2. before remote dispatch, complete Wolfpack's [Live-peer readiness checklist](https://github.com/almogdepaz/wolfpack/blob/main/docs/task-gateway.md#live-peer-readiness-checklist) and retain operator-recorded package/reload evidence; only then check the target's local requirements and send compact instructions with curated context and selected refs when they save receiver investigation.
3. keep working; use `agent_task_status` or `agent_task_inbox` for structured follow-up, and `agent_task_message` for questions, answers, and decisions.
4. verify cited files, diffs, tests, and paths-only artifact metadata independently before reporting success.
5. acknowledge the accepted terminal result, then clean up only sessions this parent spawned. Keep a reusable implementer alive while review or correction remains possible; never make workers close their own sessions.

Isolated coverage is the deterministic acceptance gate, but a specific live peer still requires current readiness at the time of use. If the checklist cannot pass, stop before task creation and report fixture-only verification. Do not use direct peer URLs from Pi.

See `skills/wolfpack-pi-task-delegation/SKILL.md` for the operational workflow and `skills/task-context-summary/SKILL.md` for recovery/reuse summaries.

## deferred follow-up

These are documented operational debt, not production TODOs: exact Pi runtime registration and heartbeat/capability leases; durable offline initial dispatch; JWT/authenticated peer federation; artifact byte transfer and retention; representative payload benchmarking; automated recovery summaries; and summary caching only after measured repeated-generation waste. v1 has no queue, scheduler, artifact transfer, or transcript transfer.

## legacy historical metrics

`task-metrics` and the task board remain read-only analyzers for already-existing historical `.pi/tasks` directories. They are not part of the runtime task protocol and must not create, mutate, or deliver v1 gateway tasks. Replacing or retiring that historical reporting surface requires a product decision about gateway-wide query/metrics ownership.

## development

```bash
bun install
bun test
bun run typecheck
```
