#!/usr/bin/env node
/**
 * Two-agent demo: a "librarian" agent (async, slow) and a "summarizer" agent
 * (fast). A plain client talks to both — showing discovery, sync send,
 * streaming, task continuation, and cooperative cancel.
 *
 *   node examples/two-openclaw-agents.mjs
 */
import { serveAgent, connectA2A, textOf } from "../packages/a2a-nats-client/dist/index.js";

const LIBRARIAN_WORK_MS = 1200;

// ---------------------------------------------------------------------------
// Agent 1: "librarian" — simulates an OpenClaw session with a slow LLM call:
// streams its reasoning progress, returns artifacts, supports cancel.
// ---------------------------------------------------------------------------
const librarian = await serveAgent(
  {
    agentId: "claw-librarian",
    name: "claw-librarian (OpenClaw session)",
    description:
      "Reference agent that mimics an OpenClaw agent session: streams progress, produces artifacts, cooperatively cancellable.",
    skills: [
      {
        id: "research",
        name: "Research a topic",
        description: "Collects sources and drafts a structured summary for a topic.",
        tags: ["research", "summary"],
        examples: ["summarize the A2A protocol", "sources on NATS JetStream"],
      },
    ],
  },
  async (ctx) => {
    const topic = textOf(ctx.message.parts);
    await ctx.working(`planning research on "${topic}"`);

    // Stream two "sources" as artifact chunks (like an agent emitting progress).
    await ctx.artifact(
      "sources",
      [{ text: `source-1: spec section about ${topic}` }],
      { name: "sources", append: false, lastChunk: false },
    );
    await ctx.working("reading source 1…");

    if (ctx.cancelRequested()) {
      await ctx.fail("canceled before finishing");
      return;
    }
    await new Promise((r) => setTimeout(r, LIBRARIAN_WORK_MS));

    await ctx.artifact("sources", [{ text: `source-2: tutorial about ${topic}` }], {
      name: "sources",
      append: true,
      lastChunk: true,
    });
    await ctx.complete(`research on "${topic}" finished (2 sources)`);
  },
);

// ---------------------------------------------------------------------------
// Agent 2: "summarizer" — fast agent that consumes the librarian's artifacts.
// Continuation example: second message reuses the same task id.
// ---------------------------------------------------------------------------
const summarizer = await serveAgent(
  {
    agentId: "claw-summarizer",
    name: "claw-summarizer (OpenClaw session)",
    description: "Fast agent that summarizes the latest research artifacts.",
    skills: [
      {
        id: "summarize",
        name: "Summarize artifacts",
        description: "Condenses research artifacts into a TL;DR.",
        tags: ["summary"],
        examples: ["TL;DR the sources"],
      },
    ],
  },
  async (ctx) => {
    const input = textOf(ctx.message.parts);
    // Continuation: on the second turn, echo what we know about the task so far.
    const prior = (ctx.task.artifacts ?? []).map((a) => a.name).join(", ");
    await ctx.complete(
      prior
        ? `TL;DR of [${prior}]: ${input} — condensed (turn 2, task continued)`
        : `TL;DR: ${input}`,
    );
  },
);

console.log("agents online:");
console.log("  ", librarian.card.name);
console.log("  ", summarizer.card.name);

const client = await connectA2A({ clientId: "orchestrator-demo" });

// ---------------------------------------------------------------------------
// Conversation
// ---------------------------------------------------------------------------

console.log("\n== 1. discovery ==");
const card = await client.getAgentCard("claw-librarian");
console.log(`librarian card: ${card.name} — skills: ${card.skills.map((s) => s.id).join(", ")}`);

console.log("\n== 2. streaming send to librarian ==");
let sourcesSoFar = "";
let taskId;
for await (const ev of client.sendMessageStream("claw-librarian", "the A2A protocol")) {
  if (ev.kind === "status_update") {
    console.log(`  [status] ${ev.statusUpdate?.status.state}`);
  }
  if (ev.kind === "artifact_update") {
    const chunk = textOf(ev.artifactUpdate?.artifact.parts ?? []);
    sourcesSoFar += chunk + " | ";
    console.log(`  [artifact+${ev.artifactUpdate?.append ? "append" : "new"}] ${chunk}`);
  }
  if (ev.kind === "task") taskId = ev.task?.id ?? taskId;
}

console.log("\n== 3. hand artifacts to summarizer (sync send) ==");
const { task: sumTask } = await client.sendMessage(
  "claw-summarizer",
  `please summarize: ${sourcesSoFar}`,
);
console.log(`  summarizer: ${sumTask?.status.message?.parts?.[0]?.text}`);

console.log("\n== 4. follow-up: NEW task in the SAME context (A2A task immutability) ==");
const { task: sumTask2 } = await client.sendMessage("claw-summarizer", "make it shorter", {
  contextId: sumTask?.contextId,
});
console.log(`  summarizer (follow-up): ${sumTask2?.status.message?.parts?.[0]?.text}`);
console.log(`  new task ${sumTask2?.id} shares context ${sumTask2?.contextId} with ${sumTask?.id}`);

console.log("\n== 5. fire-and-forget to librarian, then cancel ==");
const { task: longTask } = await client.sendMessage("claw-librarian", "a very deep topic", {
  returnImmediately: true,
});
await client.cancelTask("claw-librarian", longTask?.id ?? "");
await new Promise((r) => setTimeout(r, 300));
const after = await client.getTask("claw-librarian", longTask?.id ?? "");
console.log(`  librarian task state after cancel: ${after.status.state}`);

await client.close();
await librarian.stop();
await summarizer.stop();
console.log("\ndone.");
process.exit(0);
