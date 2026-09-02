import { afterEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";

import { createTaskStore } from "../src/task-store";
import type { RelayEnvelope } from "../src/task-protocol";

const temporaryDirectories: string[] = [];
const nodeHandleProbe = `
	import { readdirSync } from "node:fs";
	const { createTaskStore } = await import(process.argv[1]);
	const countOpenDescriptors = () => readdirSync("/dev/fd").length;
	const before = countOpenDescriptors();
	let failures = 0;
	for (let attempt = 0; attempt < 8; attempt += 1) {
		try {
			createTaskStore({ path: process.argv[2] });
			process.exitCode = 1;
		} catch {
			failures += 1;
		}
	}
	console.log(JSON.stringify({ before, after: countOpenDescriptors(), failures }));
`;

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

test("creates an owner-only current-schema sqlite store and reopens it", () => {
	const directory = mkdtempSync("/tmp/pi-tasks-store-");
	temporaryDirectories.push(directory);
	chmodSync(directory, 0o700);
	const path = join(directory, "v2", "tasks.sqlite");
	const store = createTaskStore({ path });
	store.putTask({
		taskId: "task-1", protocolVersion: "pi-tasks/v2", origin: { relay: "relay", id: "origin" }, target: { relay: "relay", id: "target" },
		task: "survives restart", createdAt: 1, expiresAt: 2, status: "active",
	});
	store.close();

	expect(statSync(dirname(path)).mode & 0o777).toBe(0o700);
	expect(statSync(path).mode & 0o777).toBe(0o600);
	const reopened = createTaskStore({ path });
	expect(reopened.getTask("task-1")?.task).toBe("survives restart");
	reopened.close();
	const checked = new Database(path, { readonly: true });
	expect((checked.query("PRAGMA user_version").get() as { readonly user_version: number }).user_version).toBe(5);
	checked.close();

	const unsafe = join(directory, "unsafe");
	mkdirSync(unsafe);
	chmodSync(unsafe, 0o755);
	expect(() => createTaskStore({ path: join(unsafe, "tasks.sqlite") })).toThrow("not owner-only");
});

test("rejects every non-current schema version without changing stored data", () => {
	const directory = mkdtempSync("/tmp/pi-tasks-store-");
	temporaryDirectories.push(directory);
	chmodSync(directory, 0o700);
	const path = join(directory, "tasks.sqlite");
	const store = createTaskStore({ path });
	store.putTask({
		taskId: "preserved-task", protocolVersion: "pi-tasks/v2", origin: { relay: "relay", id: "origin" }, target: { relay: "relay", id: "target" },
		task: "do not mutate", createdAt: 1, expiresAt: 2, status: "active",
	});
	store.close();

	for (const version of [4, 6]) {
		const database = new Database(path);
		database.exec(`PRAGMA user_version = ${version};`);
		database.close();

		expect(() => createTaskStore({ path })).toThrow("task store schema version is unsupported");
		const checked = new Database(path, { readonly: true });
		expect((checked.query("PRAGMA user_version").get() as { readonly user_version: number }).user_version).toBe(version);
		expect(checked.query("SELECT task, status FROM tasks WHERE task_id = 'preserved-task'").get()).toEqual({ task: "do not mutate", status: "active" });
		checked.close();
	}
});

test("closes Node database handles after repeated unsupported-schema failures", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-store-");
	temporaryDirectories.push(directory);
	chmodSync(directory, 0o700);
	const path = join(directory, "unsupported.sqlite");
	const database = new Database(path);
	database.exec("PRAGMA user_version = 4;");
	database.close();
	chmodSync(path, 0o600);
	const bundlePath = join(directory, "task-store.mjs");
	const build = Bun.spawn(["bun", "build", new URL("../src/task-store.ts", import.meta.url).pathname, "--target=node", "--outfile", bundlePath], {
		cwd: new URL("..", import.meta.url).pathname,
		stdout: "pipe",
		stderr: "pipe",
	});
	expect(await build.exited).toBe(0);

	const subprocess = Bun.spawn(["node", "--input-type=module", "--eval", nodeHandleProbe, bundlePath, path], { stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout] = await Promise.all([subprocess.exited, new Response(subprocess.stdout).text()]);
	const counts = JSON.parse(stdout) as { readonly before: number; readonly after: number; readonly failures: number };

	expect(exitCode).toBe(0);
	expect(counts.failures).toBe(8);
	expect(counts.after).toBe(counts.before);
});

test("atomically rejects a zero-version database with a conflicting schema object", () => {
	const directory = mkdtempSync("/tmp/pi-tasks-store-");
	temporaryDirectories.push(directory);
	chmodSync(directory, 0o700);
	const path = join(directory, "conflicting.sqlite");
	const database = new Database(path);
	database.exec("CREATE TABLE events (marker TEXT PRIMARY KEY); INSERT INTO events VALUES ('preserved');");
	database.close();
	chmodSync(path, 0o600);

	for (let attempt = 0; attempt < 2; attempt += 1) {
		expect(() => createTaskStore({ path })).toThrow();
		const checked = new Database(path, { readonly: true });
		expect((checked.query("PRAGMA user_version").get() as { readonly user_version: number }).user_version).toBe(0);
		expect(checked.query("SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all()).toEqual([{ type: "table", name: "events" }]);
		expect(checked.query("SELECT marker FROM events").get()).toEqual({ marker: "preserved" });
		checked.close();
	}
});

test("atomically reserves one durable task operation identity", () => {
	const store = createTaskStore({ path: ":memory:" });
	store.putTask({
		taskId: "task-1", protocolVersion: "pi-tasks/v2", origin: { relay: "relay", id: "origin" }, target: { relay: "relay", id: "target" },
		task: "reserve", createdAt: 1, expiresAt: 2, status: "completed",
	});

	const first = store.reserveTaskOperation({ taskId: "task-1", operation: "parent_acknowledgment", logicalId: "event-1", logicalType: "task.parent_acknowledged", envelopeIds: ["target-envelope", "origin-envelope"] });
	const repeated = store.reserveTaskOperation({ taskId: "task-1", operation: "parent_acknowledgment", logicalId: "event-2", logicalType: "task.parent_acknowledged", envelopeIds: ["new-target-envelope", "new-origin-envelope"] });

	expect(first).toEqual({ created: true, record: { taskId: "task-1", operation: "parent_acknowledgment", logicalId: "event-1", logicalType: "task.parent_acknowledged", envelopeIds: ["target-envelope", "origin-envelope"] } });
	expect(repeated).toEqual({ created: false, record: first.record });
	store.close();
});

test("persists endpoint binding and structured quarantine while removing live outbox work", () => {
	const store = createTaskStore({ path: ":memory:" });
	const endpoint = { relay: "relay", id: "origin" };
	const envelope = assignment("envelope-1", endpoint, "target");

	expect(store.getEndpointBinding()).toBeUndefined();
	store.setEndpointBinding(endpoint);
	store.putOutbox(envelope);
	store.transaction(() => {
		store.quarantineOutbox(envelope.envelopeId, {
			errorCode: "TARGET_NOT_REGISTERED",
			reason: "target is inactive",
			details: { targetId: "target" },
			quarantinedAt: 456,
		});
	});

	expect(store.getEndpointBinding()).toEqual(endpoint);
	expect(store.outbox("pending")).toEqual([]);
	expect(store.quarantinedOutbox()).toEqual([{
		envelope,
		errorCode: "TARGET_NOT_REGISTERED",
		reason: "target is inactive",
		details: { targetId: "target" },
		quarantinedAt: 456,
		priorState: "pending",
	}]);
	store.close();
});

test("preserves an existing quarantine audit row when removing a colliding live outbox record", () => {
	const store = createTaskStore({ path: ":memory:" });
	const endpoint = { relay: "relay", id: "origin" };
	const auditedEnvelope = assignment("colliding-envelope", endpoint, "audited-target");
	store.putOutbox(auditedEnvelope);
	store.transaction(() => {
		store.quarantineOutbox(auditedEnvelope.envelopeId, {
			errorCode: "OPERATOR_QUARANTINE",
			reason: "preserve this audit",
			details: { ticket: "audit-1" },
			quarantinedAt: 123,
		});
	});
	store.putOutbox(assignment("colliding-envelope", endpoint, "live-target"));

	store.transaction(() => {
		store.quarantineOutbox("colliding-envelope", {
			errorCode: "TARGET_NOT_REGISTERED",
			reason: "do not overwrite",
			details: { targetId: "live-target" },
			quarantinedAt: 456,
		});
	});

	expect(store.quarantinedOutbox()).toEqual([{
		envelope: auditedEnvelope,
		errorCode: "OPERATOR_QUARANTINE",
		reason: "preserve this audit",
		details: { ticket: "audit-1" },
		quarantinedAt: 123,
		priorState: "pending",
	}]);
	expect(store.outbox("pending")).toEqual([]);
	store.close();
});

function assignment(envelopeId: string, source: { readonly relay: string; readonly id: string }, targetId: string): RelayEnvelope {
	return {
		envelopeId,
		protocolVersion: "pi-tasks/v2",
		source,
		target: { relay: source.relay, id: targetId },
		taskId: `task-${envelopeId}`,
		kind: "assignment",
		payload: "{}",
	};
}

function dirname(path: string): string {
	const slash = path.lastIndexOf("/");
	return path.slice(0, slash);
}
