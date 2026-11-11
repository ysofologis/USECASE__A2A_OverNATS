/**
 * Demo: discovery + sync send + streaming send against an echo agent.
 * Start `pnpm echo-agent` first (or run `pnpm demo` — it spawns its own).
 */
import { spawn } from "node:child_process";
import { connectA2A, textOf } from "./index.js";

const NATS_URL = process.env.NATS_URL ?? "nats://nats.my:4222";

async function withEchoAgent<T>(fn: () => Promise<T>): Promise<T> {
  // Spawn the echo agent unless told otherwise (external agent already running).
  let child: ReturnType<typeof spawn> | undefined;
  if (process.env.USE_EXTERNAL_AGENT !== "1") {
    const agentPath = new URL("./agents/echo-agent.js", import.meta.url).pathname;
    child = spawn(process.execPath, [agentPath], {
      stdio: ["ignore", "inherit", "inherit"],
      env: { ...process.env, AGENT_ID: "echo-agent" },
    });
    // Give the agent a moment to subscribe.
    await new Promise((r) => setTimeout(r, 1500));
  }
  try {
    return await fn();
  } finally {
    child?.kill("SIGTERM");
  }
}

async function main(): Promise<void> {
  await withEchoAgent(async () => {
    const client = await connectA2A({ clientId: "demo-client", natsUrl: NATS_URL });

    // 1. Discovery
    const card = await client.getAgentCard("echo-agent");
    console.log("== discovery ==");
    console.log(`name:     ${card.name}`);
    console.log(`binding:  ${card.supportedInterfaces[0]?.protocolBinding}`);
    console.log(`skills:   ${card.skills.map((s) => s.id).join(", ")}`);

    // 2. Sync send (waits for terminal state)
    console.log("\n== message/send ==");
    const { task } = await client.sendMessage("echo-agent", "hello over NATS");
    console.log(`task:     ${task?.id}`);
    console.log(`state:    ${task?.status.state}`);
    console.log(`artifact: ${task?.artifacts?.[0]?.parts?.[0]?.text ?? "(none)"}`);

    // 3. Streaming send
    console.log("\n== message/stream ==");
    let seq = 0;
    for await (const ev of client.sendMessageStream("echo-agent", "stream me please")) {
      seq += 1;
      if (ev.kind === "task") console.log(`#${seq} task snapshot: state=${ev.task?.status.state}`);
      if (ev.kind === "status_update") console.log(`#${seq} status: ${ev.statusUpdate?.status.state}`);
      if (ev.kind === "artifact_update") {
        const text = textOf(ev.artifactUpdate?.artifact.parts ?? []);
        console.log(`#${seq} artifact: ${text}`);
      }
    }

    // 4. Live agents on the mesh
    console.log("\n== discovery scan (2 heartbeat windows) ==");
    const agents = await client.listLiveAgents();
    for (const a of agents) console.log(`live: ${a.name} @ ${a.supportedInterfaces[0]?.url}`);

    await client.close();
  });
}

main().catch((err) => {
  console.error("demo failed:", err);
  process.exit(1);
});
