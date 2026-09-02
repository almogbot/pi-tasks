# pi-tasks

`pi-tasks` is an endpoint-owned Pi extension for Wolfpack relay v2. It keeps task state in each Pi endpoint's local SQLite store and uses Wolfpack as a content-blind relay. It never writes assignments to terminals. Wolfpack's [relay v2 control-api contract](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary) is canonical for the transport boundary.

## endpoint-owned relay

The default extension uses the configured local Wolfpack relay, never the in-memory conformance relay. It registers an opaque endpoint with `POST /api/task-relay/v2/connect`, stores task state at the deterministic per-session path `~/.pi/tasks/v2/sessions/<sha256(WOLFPACK_SESSION_NAME)>/tasks.sqlite`, and exchanges opaque relay envelopes through Wolfpack. The adapter requires a Wolfpack release exposing the stable [relay v2 control-api contract](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary).

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

Wolfpack owns durable mailbox delivery and peer forwarding; Pi owns lifecycle, logical event order, receipts, and local SQLite state. `agent_task_send` returns after relay acceptance only, not Pi insertion or model execution. The receiver inserts model-visible events as structured `pi-tasks-event` custom messages through Pi's safe `deliverAs: "followUp"` queue and records structured `{ taskId, eventId }` insertion evidence. Replay after Pi has structurally recorded an event cannot create another logical receipt. `createInMemoryTaskRelay` remains exported solely as a deterministic conformance fixture.

## delegation workflow

Configure Pi role models with `WOLFPACK_IMPLEMENTER_MODEL` and `WOLFPACK_REVIEWER_MODEL`; they default to `openai-codex/gpt-5.6-terra` and `openai-codex/gpt-5.6-sol`. Explicit user or project choices override those defaults.

```bash
IMPLEMENTER_MODEL="${WOLFPACK_IMPLEMENTER_MODEL:-openai-codex/gpt-5.6-terra}"
REVIEWER_MODEL="${WOLFPACK_REVIEWER_MODEL:-openai-codex/gpt-5.6-sol}"
```

1. create or select a role session. For a disposable worker, omit an initial assignment prompt and pass the resolved role model: `wolfpack agent spawn <project> --name <task-role> --model "$IMPLEMENTER_MODEL" --json` (or `"$REVIEWER_MODEL"` for review). Put all worker instructions in `agent_task_send.task` so the new Pi process can become idle before assignment.
2. verify structured session readiness, wait for extension registration, and read `taskEndpoint` from `wolfpack session status <session> --json`.
3. call `agent_task_send` with that endpoint and the complete instructions. Keep working; use `agent_task_status` or `agent_task_inbox` for structured evidence, and call `agent_task_wait` only when explicitly asked to block.
4. use `agent_task_message` for durable questions, answers, and information. The receiver calls `agent_task_done` as its final action; no completion prose follows.
5. independently verify the result, call `agent_task_ack({ taskId })` once for that terminal task, then explicitly retain or close only the role sessions the parent spawned.

Coordinator-capable agents may delegate further when justified, and the spawning coordinator owns each child's lifecycle. After terminal completion and acknowledgment, deliberately retain the child or run `wolfpack kill <stable-session-id> --json`, then verify that exact ID is absent from `wolfpack list --json`. Never use `wolfpack session send`, `/exit`, or `/quit` for cleanup; terminal input is not Wolfpack teardown.

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
