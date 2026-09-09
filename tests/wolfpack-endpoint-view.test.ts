import { expect, test } from "bun:test";
import { wolfpackEndpointView } from "../src/wolfpack-endpoint-view";

const local = "wolfpack-pi-tasks-v2";
const peer = (n: string) => `${local}:peer:00000000-0000-4000-8000-${n.padStart(12, "0")}`;
const source = { relay: peer("1"), id: "origin" }, target = { relay: local, id: "receiver" };
const originalSource = { relay: local, id: source.id }, originalTarget = { relay: peer("2"), id: target.id };
const assignment = () => ({
  task: { origin: originalSource, target: originalTarget, task: "opaque task" },
  event: { source: originalSource, target: originalTarget, payload: { endpoint: originalSource } },
});

test("projection is role-limited, non-mutating, and does not translate opaque application content", () => {
  const input = assignment();
  const before = JSON.stringify(input);
  const viewed = wolfpackEndpointView(source, target, "assignment", input);
  expect(viewed).toEqual({ task: { ...input.task, origin: source, target }, event: { ...input.event, source, target } });
  expect(JSON.stringify(input)).toBe(before);
  const intent = { taskId: "t", payload: { endpoint: originalSource } };
  expect(wolfpackEndpointView(source, target, "intent", intent)).toBe(intent);
  const canonical = { source: originalSource, target: originalTarget, payload: { endpoint: originalTarget } };
  expect(wolfpackEndpointView(source, target, "canonical_event", canonical)).toEqual({ ...canonical, source, target });
  // Origin/self fanout remains in the origin namespace, including remote task target.
  expect(wolfpackEndpointView(originalSource, originalSource, "canonical_event", canonical)).toBe(canonical);
});

test("peer input cannot smuggle another source, assignee, namespace, or inconsistent event reference", () => {
  const mutations = [
    (p: ReturnType<typeof assignment>) => { p.task.origin = { ...originalSource, id: "other-origin" }; },
    (p: ReturnType<typeof assignment>) => { p.task.origin = { ...originalSource, relay: peer("3") }; },
    (p: ReturnType<typeof assignment>) => { p.task.origin = source; },
    (p: ReturnType<typeof assignment>) => { p.event.source = { ...originalSource, id: "other-origin" }; },
    (p: ReturnType<typeof assignment>) => { p.task.target = { ...originalTarget, id: "other-receiver" }; },
    (p: ReturnType<typeof assignment>) => { p.task.target = target; },
    (p: ReturnType<typeof assignment>) => { p.event.target = { ...originalTarget, relay: peer("3") }; },
    (p: ReturnType<typeof assignment>) => { p.event.target = { ...originalTarget, id: "other-receiver" }; },
  ];
  for (const mutate of mutations) {
    const payload = assignment(); mutate(payload);
    expect(() => wolfpackEndpointView(source, target, "assignment", payload)).toThrow("endpoint roles");
  }
  expect(() => wolfpackEndpointView({ ...source, relay: "untrusted-label" }, target, "assignment", assignment())).toThrow("endpoint roles");
  expect(() => wolfpackEndpointView(source, { ...target, relay: peer("4") }, "assignment", assignment())).toThrow("endpoint roles");
});

test("changed authenticated peer route changes only the received view, not old records", () => {
  const payload = assignment();
  const first = wolfpackEndpointView(source, target, "assignment", payload);
  const newSource = { ...source, relay: peer("5") };
  expect(wolfpackEndpointView(newSource, target, "assignment", payload)).toMatchObject({ task: { origin: newSource } });
  expect(first).toMatchObject({ task: { origin: source } });
  expect(payload.task.origin).toEqual(originalSource);
});
