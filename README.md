# pi-tasks

Endpoint-owned task coordination for Pi over Wolfpack's memory-only relay. **Both relay mail and endpoint task bookkeeping live in RAM.** Pi's existing session history is the historical record; there is no SQLite database, task-history directory, migration, disk outbox or startup replay.

The [relay control API](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary) defines the transport boundary.

## endpoint-owned relay

The default extension requires a compatible Wolfpack memory-owned server. It negotiates `volatile-v1` at `POST /api/task-relay/volatile-v1`. No transport opt-in flag is needed. The old durable relay adapter is removed, not retained as a compatibility mode.

Set `WOLFPACK_SESSION_NAME` for each Pi process. `WOLFPACK_PORT` defaults to `18790`; `WOLFPACK_SESSION_NAME` resolves the active Pi process to its relay endpoint. Read `taskEndpoint` from structured session creation/status and pass its opaque `{ relay, id }` unchanged. Never derive it from a name, terminal output or broker ID.

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

The `agent_task_send` schema is exactly `to`, `task`, and optional `timeoutMs`. Unsupported fields are rejected before admission: a pre-admission validation rejection creates no task. If an outcome is uncertain, idempotency remains necessary within the current lifetime; inspect the returned task ID rather than blindly creating another assignment.

## history versus live state

- Pi requests task-processing turns through its safe `deliverAs: "followUp"` queue. Pi session messages and tool calls/results preserve historical context. Non-waking receipts, parent ACKs and late-terminal facts are archived as `pi-tasks-event-record` custom entries before transport ACK, without waking the model. Received model-visible task events include the complete structured event in `pi-tasks-event` details, including results—not just a rendered summary.
- Active tasks, outbox intentions, individual ACK checkpoints, deduplication and delivery-blocked evidence are RAM-owned. Each endpoint has lower-only limits of16,384 records/32MiB encoded state. Admission fails before exceeding those bounds; they are not a process-RSS guarantee.
- `agent_task_send` confirms relay acceptance, **not execution**. `receiver_recorded` confirms receiver RAM receipt, **not durable persistence**. Pi insertion and wake acceptance are separate evidence, never proof of model execution.
- Session history is not an operational journal. A new process/lifecycle starts empty with a fresh endpoint/generation; it does not reconstruct tasks, replay envelopes, reuse old ACKs or reopen a worker gate from old history.
- A relay restart can lose even accepted mail. A Pi restart loses that endpoint's active task state. Prior outcomes can be unknown; do not label them delivered, failed or recovered without evidence.

## memory-owned lifecycle

`createConfiguredTaskCore()` owns its transport and `createTaskStore()` RAM state. Startup registers; polling runs every five seconds and at agent settlement. Shutdown fences new work, aborts requests, drains active calls and discards state. Late callbacks cannot mutate a successor lifetime.

A live endpoint detecting relay reset stops. `/task-relay-rebind` explains the loss boundary; only `/task-relay-rebind --accept-relay-loss` explicitly discards old state and binds a fresh scope. Automatic polling never invokes rebind. Session history remains untouched, but no historical task becomes active again.

`createVolatileTaskSession({ url, callerSession, store })` is the lower-level API for a caller-owned RAM store. Transport reconnect within that same lifetime retains pending immutable retries; `rebind()` clears the store, while `close()` stops transport and the caller then closes its state. Persistence/path options are rejected. Old database files are neither read nor deleted.

Full immutable content, including the original creation timestamp, is reused for retries. Destination-confirmed acceptance is required. Conflicting content, exhausted delivery and reset cannot become successful acceptance. Individual ACKs preserve sparse bigint cursors and cannot skip earlier pending mail. Bounds/deadlines apply through complete response bodies, including noncooperative injected transports.

## trusted Tailnet and optional authentication

The intended deployment trusts its Tailnet machines; Tailscale access control is the network boundary. Do not expose owner APIs through the public internet/Funnel or an untrusted proxy. No additional JWT or peer-signature scheme is required by the task protocol.

Existing global Wolfpack JWT enforcement remains **optional**. If the owner configures `WOLFPACK_JWT_SECRET`, the configured Pi factory supports it with short-lived tokens only for HTTP loopback. It never discovers/distributes secrets or automatically forwards them to arbitrary HTTPS URLs. HTTP401 reports `RELAY_AUTH_REQUIRED`, not an epoch reset.

Remote task endpoints must be locally resolved aliases. The paired CLI qualifies a selected remote endpoint through the local coordinator; remote lists expose explicit remote metadata. Failed qualification preserves any created session/ID but removes unsafe `taskEndpoint` output and reports `taskEndpointError`.

## delegation workflow

Configure `WOLFPACK_IMPLEMENTER_MODEL` and `WOLFPACK_REVIEWER_MODEL`; defaults are `openai-codex/gpt-5.6-terra` and `openai-codex/gpt-5.6-sol`. Explicit user or project choices override those defaults.

```bash
IMPLEMENTER_MODEL="${WOLFPACK_IMPLEMENTER_MODEL:-openai-codex/gpt-5.6-terra}"
REVIEWER_MODEL="${WOLFPACK_REVIEWER_MODEL:-openai-codex/gpt-5.6-sol}"
```

1. Use one assignment mode: full-startup instructions, or an endpoint assignment—not both. For a task worker: `wolfpack agent spawn --project-dir /absolute/worktree --name <task-role> --model "$IMPLEMENTER_MODEL" --task-worker --readiness-timeout-ms 30000 --json` (or `"$REVIEWER_MODEL"`). This mode rejects prompts/plans and `--notify-parent`; put all instructions in `agent_task_send.task`.
2. Verify exact session ID/root/Pi harness and registered `taskEndpoint`. `TASK_WORKER_PREFLIGHT_FAILED` precedes creation. `TASK_WORKER_NOT_READY` retains `createdSession`/`cleanup`; inspect `unconfirmed` cleanup by exact ID before retry. Registration is not model execution.
3. Submit the assignment and continue useful work. When none remains, yield to structured follow-up. Do not poll `agent_task_status` or `agent_task_inbox` merely to wait; use `agent_task_wait` only when explicitly asked to block.
4. Use `agent_task_message` for questions, answers and information. The receiver's last action is `agent_task_done`, not completion prose.
5. Independently verify, then acknowledge the terminal task and deliberately retain or clean up the parent-owned role.

Endpoint assignments require terminal completion and one `agent_task_ack`; full-startup children have no task ID, so use explicit completion/block notification and parent verification instead. Close only sessions the parent spawned using `wolfpack kill <stable-session-id> --json`, then verify that ID absent from `wolfpack list --json`. Never use `wolfpack session send`, `/exit`, or `/quit` for cleanup.

## worker-only execution gate

The exact value `PI_TASK_WORKER=1` enables a fail-closed model tool gate. Before assignment only inbox/status/wait are allowed; other tools receive `PI_TASK_WORKER_ASSIGNMENT_REQUIRED`. Ordinary interactive Pi and explicit user shell commands are outside this gate.

`PI_TASK_WORKER=1` sessions are leaf roles. Generic role-orchestration guidance applies only to non-worker coordinators. The spawning coordinator owns each child's complete lifecycle.

Workers cannot call `agent_task_send`, `agent_task_cancel`, or `agent_task_ack` (`PI_TASK_WORKER_COORDINATION_FORBIDDEN`). `agent_task_message` must name their eligible incorporated assignment. A structured session event alone is insufficient: its task must also be active in this endpoint's current RAM state. Old session history never authorizes work after restart.

Preflighting `agent_task_done` marks that task as closing before sibling calls are preflighted. Ordinary tools remain blocked for a closing, pending-terminal, accepted-terminal, or `delivery_blocked` task; an idempotent `agent_task_done` retry for that same assigned task remains allowed. Another independently active assignment can still authorize work.

## terminal delivery and acknowledgment

During the active lifetime `terminalDelivery` reports `not_submitted`, `pending`, `accepted`, or `delivery_blocked`. The blocked variant includes stable intent/envelope identities, origin endpoint, timestamp, and structured non-retryable relay error. Canonical task status is never changed to `delivery_blocked`. Retryable failures reuse the original intent; permanent failures are not rearmed. This evidence is lost on endpoint restart.

`agent_task_ack` is terminal-only and origin-owned. Sequential/concurrent retries in one lifetime reuse the logical event and envelope IDs. There is no restart recovery. Report changed source paths in `result.changedFiles`; artifacts are receiver-project-relative regular files, not the changed-file list.

```json
{ "result": { "changedFiles": ["src/extension.ts"] }, "artifacts": [{ "path": "verification/task-2.md" }] }
```

## development and verification

```bash
bun install
bun test
bun run typecheck
PI_TASKS_WOLFPACK_SOURCE=/absolute/clean/wolfpack-checkout \
PI_TASKS_WOLFPACK_REVISION=<full-commit-id> \
bun test tests/volatile-real-worker.test.ts tests/volatile-extension-worker.test.ts
```

Wolfpack worker tests require its supported Bun runtime (currently stable>=1.4.2). Optional source-selected checks use private HTTP/workers, not installed services. They test same-lifetime retry/ACK, explicit reset/rebind, fresh endpoint state and history-only evidence. `createInMemoryTaskRelay` is a deterministic conformance fixture, never the default runtime transport. Physical two-machine qualification, final-path performance, release packaging and live activation remain separate gates.
