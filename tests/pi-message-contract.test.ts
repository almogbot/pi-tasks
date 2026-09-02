import { expect, test } from "bun:test";
import { createAgentSession, DefaultResourceLoader, SessionManager } from "@earendil-works/pi-coding-agent";
import { mkdtempSync, rmSync } from "node:fs";

test("idle semantic messages persist synchronously and a triggered wake becomes durable", async () => {
	const directory = mkdtempSync("/tmp/pi-tasks-message-contract-");
	const sessionManager = SessionManager.inMemory();
	const resourceLoader = new DefaultResourceLoader({
		cwd: directory,
		agentDir: directory,
		noExtensions: true,
		noSkills: true,
		noPromptTemplates: true,
		noThemes: true,
		noContextFiles: true,
	});
	const { session } = await createAgentSession({
		cwd: directory,
		agentDir: directory,
		resourceLoader,
		sessionManager,
		noTools: "all",
	});

	try {
		const sending = session.sendCustomMessage({
			customType: "pi-tasks-event",
			content: "assignment",
			display: true,
			details: { taskId: "task-1", eventId: "event-1" },
		}, { triggerTurn: false });

		expect(sessionManager.getEntries()).toContainEqual(expect.objectContaining({
			type: "custom_message",
			customType: "pi-tasks-event",
			details: { taskId: "task-1", eventId: "event-1" },
		}));
		await sending;

		const waking = session.sendCustomMessage({
			customType: "pi-tasks-wake",
			content: "Process the pending Pi task event.",
			display: false,
			details: { taskId: "task-1", eventId: "event-1" },
		}, { triggerTurn: true, deliverAs: "followUp" });
		expect(sessionManager.getEntries().filter((entry) => entry.type === "custom_message" && entry.customType === "pi-tasks-wake")).toHaveLength(0);
		await waking;
		expect(sessionManager.getEntries()).toContainEqual(expect.objectContaining({
			type: "custom_message",
			customType: "pi-tasks-wake",
			details: { taskId: "task-1", eventId: "event-1" },
		}));
		expect(sessionManager.getEntries().filter((entry) => entry.type === "custom_message" && entry.customType === "pi-tasks-wake")).toHaveLength(1);
	} finally {
		session.dispose();
		rmSync(directory, { recursive: true, force: true });
	}
});
