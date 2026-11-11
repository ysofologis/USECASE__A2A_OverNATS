/**
 * a2a-nats-client — the "least effort" integration layer for vendor agents.
 *
 * Two entry points:
 *
 * 1. `A2AClient`  — call other agents (send/stream/tasks/discovery).
 * 2. `serveAgent` — expose your handler as a discoverable A2A agent.
 *
 * Everything below the integration surface (envelopes, subjects, heartbeats,
 * ProtoJSON) is handled by `a2a-nats-binding`.
 */
import {
  A2ANatsAgent,
  A2ANatsClient,
  type AgentSkillDef,
  type TaskContext,
  type TaskHandler,
  type SendOptions,
  type SendMessageResult,
  type StreamEvent,
  type ClientOptions,
} from "a2a-nats-binding";
export type {
  AgentCard,
  AgentSkill,
  Message,
  Part,
  Task,
  TaskState,
  TaskStatusUpdateEvent,
  TaskArtifactUpdateEvent,
} from "a2a-nats-binding";
export { A2AOperationError, A2ATimeoutError, A2AProtocolError, TaskFailed } from "a2a-nats-binding";

export { A2ANatsClient };
export type { SendOptions, SendMessageResult, StreamEvent, ClientOptions };

/** Options for `serveAgent` — the minimal vendor-facing surface. */
export interface ServeAgentOptions {
  /** Stable id; becomes the NATS subject token `a2a.v1.agent.<agentId>`. */
  agentId: string;
  /** Human-readable name for the AgentCard. */
  name: string;
  description: string;
  /** Declared skills; other agents discover you by these. */
  skills: AgentSkillDef[];
  /** Broker URL, e.g. `nats://nats.my:4222`. Default: NATS_URL env or nats://nats.my:4222. */
  natsUrl?: string;
  version?: string;
}

export interface ServedAgent {
  /** AgentCard currently advertised. */
  card: ReturnType<A2ANatsAgent["buildAgentCard"]>;
  /** Stop serving (drain connection). */
  stop(): Promise<void>;
  /** Internal agent handle for advanced use. */
  agent: A2ANatsAgent;
}

/**
 * Serve a task handler as an A2A agent over NATS.
 *
 * ```ts
 * const served = await serveAgent(
 *   { agentId: "my-agent", name: "My Agent", description: "...", skills: [...] },
 *   async (ctx) => {
 *     const text = ctx.message.parts.find(p => "text" in p)?.text ?? "";
 *     await ctx.complete(`you said: ${text}`);
 *   },
 * );
 * ```
 */
export async function serveAgent(
  opts: ServeAgentOptions,
  handler: TaskHandler,
): Promise<ServedAgent> {
  const natsUrl = opts.natsUrl ?? process.env.NATS_URL ?? "nats://nats.my:4222";
  const agent = new A2ANatsAgent({
    agentId: opts.agentId,
    humanName: opts.name,
    description: opts.description,
    skills: opts.skills,
    natsUrl,
    ...(opts.version !== undefined ? { version: opts.version } : {}),
  });
  agent.onTask(handler);
  await agent.start();
  return {
    card: agent.buildAgentCard(),
    agent,
    stop: () => agent.stop(),
  };
}

/** Default broker URL used by client helpers. */
export function defaultNatsUrl(): string {
  return process.env.NATS_URL ?? "nats://nats.my:4222";
}

/** Convenience factory: connected client with sane defaults. */
export async function connectA2A(opts: Partial<ClientOptions> = {}): Promise<A2ANatsClient> {
  return A2ANatsClient.connect(opts.natsUrl ?? defaultNatsUrl(), {
    ...(opts.clientId !== undefined ? { clientId: opts.clientId } : {}),
    ...(opts.requestTimeoutMs !== undefined ? { requestTimeoutMs: opts.requestTimeoutMs } : {}),
  });
}

/** Extract the first text part of a message (the 90% case for vendor agents). */
export function textOf(parts: ReadonlyArray<Record<string, unknown> | { text?: string; mediaType?: string }>): string {
  const part = parts.find((p) => typeof p.text === "string");
  return part && typeof part.text === "string" ? part.text : "";
}

export type { TaskContext, TaskHandler, AgentSkillDef };
