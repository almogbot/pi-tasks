import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";

const packageJsonPath = new URL("../package.json", import.meta.url);
const readmePath = new URL("../README.md", import.meta.url);
const delegationSkillPath = new URL("../skills/wolfpack-pi-task-delegation/SKILL.md", import.meta.url);
const summarySkillPath = new URL("../skills/task-context-summary/SKILL.md", import.meta.url);

describe("gateway package documentation", () => {
	test("publishes the extension and package skills", async () => {
		const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8")) as {
			readonly description?: string;
			readonly pi?: { readonly extensions?: readonly string[]; readonly skills?: readonly string[] };
		};
		expect(packageJson.description).toContain("Wolfpack task gateway");
		expect(packageJson.description).not.toContain("pluggable stores and transports");
		expect(packageJson.pi?.extensions).toEqual(["./src/extension.ts"]);
		expect(packageJson.pi?.skills).toContain("./skills");
	});

	test("publishes v1 only as an explicit compatibility entry while v2 remains the default", async () => {
		const [packageJson, readme, v2Extension] = await Promise.all([
			readFile(packageJsonPath, "utf8").then((value) => JSON.parse(value) as { readonly exports?: Record<string, string>; readonly pi?: { readonly extensions?: readonly string[] } }),
			readFile(readmePath, "utf8"),
			readFile(new URL("../src/extension.ts", import.meta.url), "utf8"),
		]);
		expect(packageJson.pi?.extensions).toEqual(["./src/extension.ts"]);
		expect(packageJson.exports?.["./v1-compat-extension"]).toBe("./src/v1-compat-extension.ts");
		expect(readme).toContain("@sgtbeatdown/pi-tasks/v1-compat-extension");
		expect(readme).toContain("explicit opt-in");
		expect(readme).toContain("does not fall back");
		expect(v2Extension).not.toContain("legacy-extension");
	});

	test("documents deterministic per-session v2 storage", async () => {
		const readme = await readFile(readmePath, "utf8");
		expect(readme).not.toContain("~/.pi/tasks/v2/tasks.sqlite");
		expect(readme).toContain("~/.pi/tasks/v2/sessions/<sha256(WOLFPACK_SESSION_NAME)>/tasks.sqlite");
		expect(readme).toContain("deterministic per-session path");
	});

	test("makes endpoint-owned v2 guidance primary and scopes v1 addressing to compatibility docs", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		const defaultReadme = readme.split("## v1 gateway adapter (retained)")[0] ?? readme;
		const defaultSkill = skill.split("## v1 compatibility")[0] ?? skill;
		for (const document of [defaultReadme, defaultSkill]) {
			expect(document).toContain("wolfpack-pi-tasks-v2");
			expect(document).toContain("{ relay, id }");
			expect(document).toContain("taskEndpoint");
			expect(document).not.toContain("{ machine, sessionId }");
			expect(document).not.toContain('"machine"');
			expect(document).not.toContain('"sessionId"');
		}
		const compatibility = `${readme.split("## v1 gateway adapter (retained)")[1] ?? ""}\n${skill.split("## v1 compatibility")[1] ?? ""}`;
		expect(compatibility).toContain("@sgtbeatdown/pi-tasks/v1-compat-extension");
		expect(compatibility).toContain("{ machine, sessionId }");
		expect(compatibility).toContain("canonical HTTPS Tailnet origin");
		expect(`${readme}\n${skill}`).not.toContain("task-adapter-contract.md");
	});

	test("documents prompt-free disposable workers, role models, and safe active-turn insertion", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		for (const document of [readme, skill]) {
			expect(document).toContain('wolfpack agent spawn <project> --name <task-role> --model "$IMPLEMENTER_MODEL" --json');
			expect(document).toContain("WOLFPACK_IMPLEMENTER_MODEL");
			expect(document).toContain("WOLFPACK_REVIEWER_MODEL");
			expect(document).toContain("openai-codex/gpt-5.6-terra");
			expect(document).toContain("openai-codex/gpt-5.6-sol");
			expect(document).toContain("agent_task_send.task");
			expect(document).toContain('`deliverAs: "followUp"`');
		}
		expect(skill).toContain("Do not start a disposable worker with a blocking");
	});

	test("documents the exact default v2 send schema and retains v1 correction guidance", async () => {
		const readme = await readFile(readmePath, "utf8");
		const minimalEnvelope = readme.match(/### minimal valid v2 send envelope\n\n```json\n([\s\S]+?)\n```/);

		expect(minimalEnvelope?.[1]).toBe(JSON.stringify({
			to: { relay: "wolfpack-pi-tasks-v2", id: "target-opaque-endpoint-id" },
			task: "implement the narrow change and run focused tests",
		}, null, 2));
		expect(readme).toContain("`INVALID_REQUEST.error.path`");
		expect(readme).toContain("RFC 6901 JSON Pointer");
		expect(readme).toContain("rejected send field");
		expect(readme).toContain("pre-persistence validation rejection creates no task");
		expect(readme).toContain("idempotency remains necessary");
		expect(readme).toContain("creation status is uncertain");
	});

	test("references only existing canonical Wolfpack documents", async () => {
		const documents = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		const links = documents.flatMap((document) => [...document.matchAll(/https:\/\/github\.com\/almogdepaz\/wolfpack\/blob\/main\/docs\/([^\s)#]+\.md)(?:#[^\s)]+)?/g)].map((match) => match[1]));
		expect(links.length).toBeGreaterThan(0);
		expect(new Set(links)).toEqual(new Set(["control-api-schema.md", "task-gateway.md"]));
	});

	test("separates changed-file reporting from receiver-project artifact declarations", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		for (const document of [readme, skill]) {
			expect(document).toContain("result.changedFiles");
			expect(document).toContain("receiver-project-relative regular files");
			expect(document).toContain('"artifacts": [{ "path": "verification/task-2.md" }]');
		}
		expect(readme).toContain("canonical Wolfpack artifact contract");
	});

	test("requires current live-peer readiness before remote dispatch", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		for (const document of [readme, skill]) {
			expect(document).toContain("Live-peer readiness checklist");
			expect(document).toContain("operator-recorded package/reload evidence");
			expect(document).toContain("fixture-only verification");
			expect(document).not.toContain("A real second peer is not currently available");
		}
		expect(readme).toContain("Isolated coverage is the deterministic acceptance gate");
		expect(skill).toContain("Isolated coverage is the deterministic acceptance gate");
	});

	test("keeps phase role reuse, compact handoffs, and parent verification in the delegation workflow", async () => {
		const skill = await readFile(delegationSkillPath, "utf8");
		for (const detail of [
			"one persistent implementer and one persistent read-only reviewer",
			"Do not rotate a healthy role session for routine corrections",
			"context.summary` only for constraints, decisions, and recovery state",
			"Do not copy plans, source contents, or transcripts",
			"terminal `send` is only for explicit human steering",
			"independently verifies files, diff, tests, and artifacts",
			"agent_task_ack({ taskId })",
		]) expect(skill).toContain(detail);
	});

	test("documents task workers as leaf roles rather than nested coordinators", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		for (const document of [readme, skill]) {
			const normalized = document.toLowerCase();
			expect(document).toContain("`PI_TASK_WORKER=1` sessions are leaf roles");
			expect(normalized).toContain("generic role-orchestration guidance applies only to non-worker coordinators");
			expect(document).toContain("PI_TASK_WORKER_COORDINATION_FORBIDDEN");
			for (const tool of ["agent_task_send", "agent_task_cancel", "agent_task_ack"]) expect(normalized).toContain(`workers cannot call \`${tool}\``);
		}
		expect(skill).toContain("must not create, rotate, or close Wolfpack sessions through shell or session-control tools");
	});

	test("ships a recovery-only context summary workflow", async () => {
		const skill = await readFile(summarySkillPath, "utf8");
		expect(skill).toContain("parent authors normal summaries");
		expect(skill).toContain("docs/code");
		expect(skill).toContain("session summary");
		expect(skill).toContain("16KiB");
		expect(skill).toContain("disagreement");
	});
});
