---
name: wolfpack-pi-task-delegation
description: Use when opening, selecting, delegating to, checking, or cleaning up a Wolfpack Pi subagent through the endpoint-owned agent task tools.
---

# wolfpack pi task delegation

use `wolfpack-tailnet-control` for session control. this skill covers endpoint-owned v2 task lifecycle, structured handoffs, and parent verification. The package default is the v2 extension described by Wolfpack's [relay v2 control-api contract](https://github.com/almogdepaz/wolfpack/blob/main/docs/control-api-schema.md#pi-tasks-relay-v2-boundary).

## v2 requirements and addressing

- load this package's default extension in every participating Pi process and set `WOLFPACK_SESSION_NAME`. Set `WOLFPACK_PORT` only when the local Wolfpack control port differs from `18790`.
- address `agent_task_send` targets only as `to: { relay, id }`. For the default Wolfpack adapter, the relay is `wolfpack-pi-tasks-v2` and the ID is opaque.
- create a disposable worker without an initial model prompt: `wolfpack agent spawn <project> --name <task-role> --json`. Do not start a disposable worker with a blocking “wait for assignments” prompt. Put the complete instructions in `agent_task_send.task`.
- after the target extension registers, run structured session control (`wolfpack session status <session> --json`) and read its `taskEndpoint`. Do not derive endpoint IDs from session names, broker IDs, terminal labels, output, or prose.
- pass the returned endpoint without translation:

```json
{
  "to": { "relay": "wolfpack-pi-tasks-v2", "id": "target-opaque-endpoint-id" },
  "task": "implement the narrow change and run focused tests",
  "timeoutMs": 1800000
}
```

The default `agent_task_send` schema is exactly `to`, `task`, and optional `timeoutMs`. v1-only fields such as context, role, metadata, preflight, idempotency keys, and completion prompts are not silently accepted or translated.

## phase roles and handoffs

For a multi-step project phase, retain one persistent implementer and one persistent read-only reviewer. Reuse a healthy role session for corrections and follow-up review; a completed task finishes one assignment, not the underlying session. Do not rotate a healthy role session for routine corrections. Rotate only for phase completion, material context degradation, harness failure, or required specialist independence. Keep at most one active assignment per role unless the user explicitly approves parallel work.

`PI_TASK_WORKER=1` sessions are leaf roles. Workers cannot call `agent_task_send`; workers cannot call `agent_task_cancel`; workers cannot call `agent_task_ack`. These coordinator-tool attempts are blocked with the stable reason `PI_TASK_WORKER_COORDINATION_FORBIDDEN`, not the pre-assignment reason. They may use `agent_task_message` only for the eligible incorporated assignment named by its input `taskId`. Generic role-orchestration guidance applies only to non-worker coordinators. Task workers must not create, rotate, or close Wolfpack sessions through shell or session-control tools.

After every verified and acknowledged terminal task, explicitly retain a parent-spawned session only when concrete follow-up is likely; otherwise close it through canonical Wolfpack session control. Workers never close their own sessions. terminal `send` is only for explicit human steering; it is not task state, completion evidence, or a substitute for `agent_task_message`.

## delegation lifecycle

| event | owner | command | session outcome |
| --- | --- | --- | --- |
| assignment completion | worker | `agent_task_done` | unchanged |
| task acknowledgment | parent | `agent_task_ack` | unchanged |
| role session retention | parent | `none` | retained for reuse |
| role session teardown | parent | `wolfpack kill <session-or-id> --json` | exact stable session id terminated |
| teardown verification | parent | `wolfpack list --json` | exact stable session id absent |
| Pi exit | none | `/quit` | not Wolfpack teardown |

Assignment completion ends the assigned task, not the reusable role session. When no further reuse is likely, the parent—not the worker—must pass the exact stable session ID to `wolfpack kill <session-or-id> --json`, then inspect `wolfpack list --json` and verify that ID is absent. `/quit` exits Pi; it is not Wolfpack teardown. Workers never close themselves.

## v2 workflow

1. create or select the role session, verify its structured project and terminal readiness, then obtain its registered `taskEndpoint`.
2. call `agent_task_send` with the opaque endpoint and concise instructions in `task`. The tool returns after relay acceptance only, not Pi insertion or model execution. If submission reports a retryable delivery error after local persistence, use the structured task ID from the error and inspect it rather than creating an unrelated replacement.
3. keep working. Use `agent_task_status` or `agent_task_inbox` for structured evidence. Call `agent_task_wait` only when the user explicitly asks to block.
4. use `agent_task_message` for durable `question`, `answer`, or `information` flow. Do not use rendered text, terminal output, or logs as lifecycle evidence.
5. assignees call `agent_task_done` as their final action with the assigned task ID, terminal status, concise summary, and optional structured result, error, and artifact declarations. Report source modifications in `result.changedFiles`; artifacts are receiver-project-relative regular files for a parent to inspect, not changed-file lists: `{ "result": { "changedFiles": ["src/extension.ts"] }, "artifacts": [{ "path": "verification/task-2.md" }] }`. No prose completion afterward.
6. the parent independently verifies files, diff, tests, and artifacts. Then call `agent_task_ack({ taskId })` for that one terminal task and explicitly retain or close the spawned role session.

## v2 delivery and recovery

Wolfpack v2 is a content-blind relay; each Pi endpoint owns task lifecycle, canonical event order, receipts, and SQLite state. Relay acceptance does not prove model execution. The receiver inserts model-visible events as structured `pi-tasks-event` custom messages with Pi's safe `deliverAs: "followUp"` queue, then records insertion through structured `{ taskId, eventId }` evidence. Restart recovery reads durable session entries and local task state; it never parses rendered prompts or terminal prose.

Receiver terminal submission has one task-wide logical identity. Same-status retries reuse it; a conflicting terminal status fails closed. Canonical status remains origin-owned, while `terminalDelivery` separately exposes `not_submitted`, `pending`, `accepted`, or `delivery_blocked`. Parent acknowledgment is terminal-only and retries reuse one logical event and stable envelope identities.

Set `PI_TASK_WORKER=1` only for task-only workers. Before a valid structured assignment matches a locally owned active task, the worker gate allows only inbox/status/wait inspection and blocks other model tools with `PI_TASK_WORKER_ASSIGNMENT_REQUIRED`.

## v1 compatibility

The retained v1 workflow is available only through the explicit `@sgtbeatdown/pi-tasks/v1-compat-extension` entrypoint. It does not load by default, and v2 never falls back or translates its targets.

v1 uses a reachable local Wolfpack task gateway and addresses targets as `to: { machine, sessionId }`. Use `machine: "local"` for same-machine work; remote work uses only the receiver's canonical HTTPS Tailnet origin. v1 trusts local processes and trusted Tailnet machines, has no JWT federation, and follows Wolfpack's [task gateway guide](https://github.com/almogdepaz/wolfpack/blob/main/docs/task-gateway.md).

Before v1 remote dispatch, complete the [Live-peer readiness checklist](https://github.com/almogdepaz/wolfpack/blob/main/docs/task-gateway.md#live-peer-readiness-checklist), including operator-recorded package/reload evidence. If it cannot pass, stop before task creation and report fixture-only verification. Isolated coverage is the deterministic acceptance gate, but it does not prove a specific live peer.

Only v1 supports curated `context.summary`/refs, role, metadata, preflight, idempotency keys, and completion prompts. Use `context.summary` only for constraints, decisions, and recovery state. Refs are metadata, not copied files or transcript. A v1 remote initial send gets one initial attempt; later peer events get four total attempts. Retry exhaustion is surfaced; v1 has no offline queue or background dispatch promise.

## do not

- do not use terminal output, rendered prompts, logs, or error prose as task state.
- do not ask workers to complete in prose or close their own sessions.
- do not steer or interrupt an active Pi turn with task context; use the adapter's follow-up queue.
- Do not copy plans, source contents, or transcripts into task context.
- do not mix v1 and v2 target shapes or silently translate between them.
- do not promise JWT federation, artifact byte transfer, exactly-once model execution, or successor endpoint rebinding.
