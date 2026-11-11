/**
 * Reference echo agent — the smallest possible A2A-over-NATS agent.
 * Start it in one terminal; run the demo against it from another.
 */
import { serveAgent } from "../index.js";

const served = await serveAgent(
  {
    agentId: process.env.AGENT_ID ?? "echo-agent",
    name: process.env.AGENT_NAME ?? "Echo Agent (TS)",
    description: "Reference A2A-over-NATS agent: echoes your message back, optionally reversed.",
    skills: [
      {
        id: "echo",
        name: "Echo",
        description: "Echoes the input text back; appends a timestamp.",
        tags: ["demo", "echo"],
        examples: ["hello world"],
      },
    ],
    natsUrl: process.env.NATS_URL,
  },
  async (ctx) => {
    const text = ctx.message.parts.find((p) => "text" in p)?.text ?? "";
    await ctx.working(`echoing "${text}"`);
    // Demonstrate a streamed artifact chunk before completing.
    await ctx.artifact("echo-artifact", [{ text: `ECHO[${new Date().toISOString()}]: ${text}` }], {
      name: "echo-result",
      lastChunk: true,
    });
    await ctx.complete(`echoed: ${text}`);
  },
);

console.log(`[echo-agent] serving as ${served.card.name}`);
console.log(`[echo-agent] interface: ${served.card.supportedInterfaces[0]?.url}`);

const shutdown = async () => {
  await served.stop();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
