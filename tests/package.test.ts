import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";

const packageJsonPath = new URL("../package.json", import.meta.url);
const readmePath = new URL("../README.md", import.meta.url);
const delegationSkillPath = new URL("../skills/wolfpack-pi-task-delegation/SKILL.md", import.meta.url);
const summarySkillPath = new URL("../skills/task-context-summary/SKILL.md", import.meta.url);
const removedPaths = [
	"../src/v1-compat-extension.ts",
	"../src/legacy-extension.ts",
	"../src/legacy-task-inbox.ts",
	"../src/gateway-client.ts",
	"../src/task-artifacts.ts",
	"../src/metrics.ts",
	"../src/metrics-cli.ts",
	"../src/task-board.ts",
	"../tests/gateway-client.test.ts",
	"../tests/extension.test.ts",
	"../tests/task-inbox.test.ts",
	"../tests/metrics.test.ts",
] as const;

describe("v2-only package", () => {
	test("publishes only the v2 extension, root export, and supported scripts", async () => {
		const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8")) as {
			readonly description?: string;
			readonly exports?: Record<string, string>;
			readonly pi?: { readonly extensions?: readonly string[]; readonly skills?: readonly string[] };
			readonly scripts?: Record<string, string>;
		};

		expect(packageJson.description).toContain("endpoint-owned");
		expect(packageJson.pi?.extensions).toEqual(["./src/extension.ts"]);
		expect(packageJson.pi?.skills).toContain("./skills");
		expect(packageJson.exports).toEqual({ ".": "./src/index.ts" });
		expect(packageJson.scripts).not.toHaveProperty("task-metrics");
	});

	test("excludes removed runtime, reporting, and dedicated-test files", () => {
		for (const path of removedPaths) expect(existsSync(new URL(path, import.meta.url))).toBe(false);
	});

	test("documents only endpoint-owned relay operation", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		for (const document of [readme, skill]) {
			expect(document).toContain("wolfpack-pi-tasks-v2");
			expect(document).toContain("{ relay, id }");
			expect(document).toContain("taskEndpoint");
			// The volatile-v1 transport and legacy cutover warning are not the removed v1 task runtime.
			expect(document).not.toMatch(/\b(?:pi-tasks\/v1|v1-compat-extension|legacy-extension|task-metrics|task board|historical reporting)\b/i);
			expect(document).toContain("volatile-v1");
			expect(document).toContain("/task-relay-rebind --accept-relay-loss");
			expect(document).toContain("can lose even accepted mail");
		}
		expect(readme).toContain("requires a compatible Wolfpack memory-owned server");
		expect(readme).toContain("No transport opt-in flag is needed");
		expect(readme).toContain("`WOLFPACK_SESSION_NAME` resolves the active Pi process to its relay endpoint");
		expect(`${readme}\n${skill}`).not.toContain("task-gateway.md");
	});

	test("documents prompt-free workers, role model defaults and overrides, exact sends, and artifact declarations", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		const minimalEnvelope = readme.match(/### minimal valid v2 send envelope\n\n```json\n([\s\S]+?)\n```/);

		expect(minimalEnvelope?.[1]).toBe(JSON.stringify({
			to: { relay: "wolfpack-pi-tasks-v2", id: "target-opaque-endpoint-id" },
			task: "implement the narrow change and run focused tests",
		}, null, 2));
		for (const document of [readme, skill]) {
			expect(document).toContain('wolfpack agent spawn --project-dir /absolute/worktree --name <task-role> --model "$IMPLEMENTER_MODEL" --task-worker --readiness-timeout-ms 30000 --json');
			expect(document).toContain("TASK_WORKER_PREFLIGHT_FAILED");
			expect(document).toContain("TASK_WORKER_NOT_READY");
			expect(document).toContain("createdSession");
			expect(document).toContain("unconfirmed");
			expect(document).toContain("rejects prompts/plans and `--notify-parent`");
			expect(document).toContain("WOLFPACK_IMPLEMENTER_MODEL");
			expect(document).toContain("WOLFPACK_REVIEWER_MODEL");
			expect(document).toContain("openai-codex/gpt-5.6-terra");
			expect(document).toContain("openai-codex/gpt-5.6-sol");
			expect(document).toContain("agent_task_send.task");
			expect(document).toContain('`deliverAs: "followUp"`');
			expect(document).toContain("result.changedFiles");
			expect(document).toContain("receiver-project-relative regular files");
			expect(document).toContain('"artifacts": [{ "path": "verification/task-2.md" }]');
		}
		expect(readme).toContain("Explicit user or project choices override those defaults.");
		expect(skill).toContain("an explicit user or project model choice overrides the environment/default.");
		expect(skill).toContain("do not start a blocking “wait for assignments” prompt");
		expect(readme).toContain("pre-admission validation rejection creates no task");
		expect(readme).toContain("idempotency remains necessary");
	});

	test("references only the relay-v2 control API", async () => {
		const documents = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		const links = documents.flatMap((document) => [...document.matchAll(/https:\/\/github\.com\/almogdepaz\/wolfpack\/blob\/main\/docs\/([^\s)#]+\.md)(?:#[^\s)]+)?/g)].map((match) => match[1]));
		expect(new Set(links)).toEqual(new Set(["control-api-schema.md"]));
	});

	test("keeps role reuse, parent verification and acknowledgment, child teardown, and worker containment", async () => {
		const [readme, skill] = await Promise.all([readFile(readmePath, "utf8"), readFile(delegationSkillPath, "utf8")]);
		for (const document of [readme, skill]) {
			expect(document).toContain("spawning coordinator");
			expect(document).toContain("wolfpack kill <stable-session-id> --json");
			expect(document).toContain("wolfpack list --json");
			expect(document).toContain("Never use `wolfpack session send`, `/exit`, or `/quit` for cleanup");
			expect(document).toContain("`PI_TASK_WORKER=1` sessions are leaf roles");
			expect(document).toContain("PI_TASK_WORKER_COORDINATION_FORBIDDEN");
			expect(document).toContain("Generic role-orchestration guidance applies only to non-worker coordinators.");
		}
		expect(readme).toContain("Endpoint assignments require terminal completion and one `agent_task_ack`");
		expect(readme).toContain("full-startup children have no task ID");
		expect(skill).toContain("Full-startup children have no endpoint task ID");
		for (const detail of [
			"one persistent implementer and one persistent read-only reviewer",
			"Do not rotate a healthy role session for routine corrections",
			"terminal `send` is only for explicit human steering",
			"independently verifies files, diff, tests, and artifacts",
			"agent_task_ack({ taskId })",
			"must not create, rotate, or close Wolfpack sessions through shell or session-control tools",
		]) expect(skill).toContain(detail);
	});

	test("documents terminal preflight and blocked-delivery invariants", async () => {
		const readme = await readFile(readmePath, "utf8");
		expect(readme).toContain("Preflighting `agent_task_done` marks that task as closing before sibling calls are preflighted.");
		expect(readme).toContain("Ordinary tools remain blocked for a closing, pending-terminal, accepted-terminal, or `delivery_blocked` task");
		expect(readme).toContain("idempotent `agent_task_done` retry for that same assigned task remains allowed");
		expect(readme).toContain("Another independently active assignment can still authorize work.");
		expect(readme).toContain("stable intent/envelope identities, origin endpoint, timestamp, and structured non-retryable relay error");
		expect(readme).toContain("is never changed to `delivery_blocked`");
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
