import { connect, JSONCodec, syncIterator } from "nats";
import type { ConnectionOptions, Msg, NatsConnection } from "nats";
import {
  DISCOVERY,
  HEARTBEAT_INTERVAL_MS,
  HDR_TASK_ID,
  OP,
  isPubEvent,
  isPubMsg,
  isTerminalState,
  subjects,
} from "./contract.js";
import { A2AOperationError, A2AProtocolError, A2ATimeoutError } from "./errors.js";
import type {
  
  AgentCard,
  CancelTaskRequest,
  GetTaskRequest,
  ListTasksRequest,
  ListTasksResponse,
  Message,
  SendMessageRequest,
  SendMessageResponse,
  Task,
  TaskArtifactUpdateEvent,
  TaskStatusUpdateEvent,
} from "./types.js";
import type { A2AError } from "./contract.js";
import {
  makeHeaders,
  makePubMsg,
  sanitizeAgentId,
  uuid,
} from "./util.js";

const jc = JSONCodec();

export interface ClientOptions {
  /** NATS server URL(s), e.g. `nats://nats.my:4222`. */
  natsUrl: string;
  /** Publisher identity used in envelopes/headers. */
  clientId?: string;
  /** Default request timeout in ms (default 120000 — agents may be slow). */
  requestTimeoutMs?: number;
}

export interface SendOptions {
  /** Fail fast: resolve as soon as the task is accepted (default false = wait terminal state). */
  returnImmediately?: boolean;
  /** Override the default timeout for this call. */
  timeoutMs?: number;
  /** Accepted output media types hint for the agent. */
  acceptedOutputModes?: string[];
  /** Continue an existing task / start a named task. */
  taskId?: string;
  contextId?: string;
}

export interface SendMessageResult {
  task?: Task;
  message?: Message;
}

export interface StreamEvent {
  kind: "task" | "message" | "status_update" | "artifact_update";
  task?: Task;
  message?: Message;
  statusUpdate?: TaskStatusUpdateEvent;
  artifactUpdate?: TaskArtifactUpdateEvent;
  seq: number;
}

function isA2AError(v: Record<string, unknown>): boolean {
  return typeof v.code === "string" && typeof v.message === "string" && !("status" in v);
}

/**
 * Client for talking to A2A agents over the NATS binding.
 *
 * ```ts
 * const client = await A2ANatsClient.connect("nats://nats.my:4222");
 * const card = await client.getAgentCard("echo-agent");
 * const { task } = await client.sendMessage("echo-agent", "hello");
 * ```
 */
export class A2ANatsClient {
  private nc?: NatsConnection;
  private readonly clientId: string;
  private readonly requestTimeoutMs: number;
  private readonly natsUrl: string;
  private cardCache = new Map<string, { card: AgentCard; seenAt: number }>();

  private constructor(opts: ClientOptions) {
    this.natsUrl = opts.natsUrl;
    this.clientId = opts.clientId ?? `client-${uuid().slice(0, 8)}`;
    this.requestTimeoutMs = opts.requestTimeoutMs ?? 120_000;
  }

  /** Connect to the broker. */
  static async connect(natsUrl: string, opts: Omit<ClientOptions, "natsUrl"> = {}): Promise<A2ANatsClient> {
    const client = new A2ANatsClient({ ...opts, natsUrl });
    const connOpts: ConnectionOptions = { servers: [natsUrl], name: `a2a-client-${client.clientId}` };
    client.nc = await connect(connOpts);
    return client;
  }

  /** Drain and close. */
  async close(): Promise<void> {
    await this.nc?.drain();
    this.nc = undefined;
  }

  get connection(): NatsConnection | undefined {
    return this.nc;
  }

  // ------------------------------------------------------------------
  // Discovery
  // ------------------------------------------------------------------

  /**
   * Fetch the AgentCard of a specific agent (request/reply, cached for one
   * heartbeat window). Works even when the agent does not advertise.
   */
  async getAgentCard(agentId: string, opts: { timeoutMs?: number; noCache?: boolean } = {}): Promise<AgentCard> {
    const id = sanitizeAgentId(agentId);
    if (!opts.noCache) {
      const hit = this.cardCache.get(id);
      if (hit && Date.now() - hit.seenAt < HEARTBEAT_INTERVAL_MS) return hit.card;
    }
    const payload = await this.requestPubMsg(
      DISCOVERY.card(id),
      "discovery/card",
      {},
      opts.timeoutMs ?? 10_000,
    );
    const card = payload as unknown as AgentCard;
    if (!card || !Array.isArray(card.supportedInterfaces) || !card.name) {
      throw new A2AProtocolError(`malformed agent card from "${id}"`);
    }
    this.cardCache.set(id, { card, seenAt: Date.now() });
    return card;
  }

  /**
   * Discover live agents by listening to advertise broadcasts for `windowMs`
   * (default two heartbeat windows). Cards are deduplicated by interface url.
   *
   * Note: nats.js subscription `timeout` does not terminate the async
   * iterator on inactivity, so collection is bounded with an explicit race.
   */
  async listLiveAgents(windowMs = HEARTBEAT_INTERVAL_MS * 2): Promise<AgentCard[]> {
    if (!this.nc) throw new A2AProtocolError("client not connected");
    const found = new Map<string, AgentCard>();
    const sub = this.nc.subscribe(DISCOVERY.advertise);
    const collect = (async () => {
      for await (const msg of sub) {
        try {
          const body = jc.decode(msg.data) as unknown;
          if (!isPubMsg(body)) continue;
          const card = body.payload as unknown as AgentCard;
          if (card && Array.isArray(card.supportedInterfaces) && card.name) {
            const iface = card.supportedInterfaces[0]?.url ?? card.name;
            found.set(iface, card);
          }
        } catch {
          /* skip malformed adverts */
        }
      }
    })();
    const timer = new Promise<void>((resolve) => setTimeout(resolve, windowMs));
    await Promise.race([collect, timer]);
    sub.unsubscribe();
    await sub.closed;
    return [...found.values()];
  }

  // ------------------------------------------------------------------
  // message/send
  // ------------------------------------------------------------------

  /**
   * Send a message and wait for the outcome. By default resolves at a
   * terminal/interrupted task state; with `returnImmediately` resolves as
   * soon as the task is accepted (then poll via getTask / subscribe events).
   */
  async sendMessage(
    agentId: string,
    text: string | Message,
    opts: SendOptions = {},
  ): Promise<SendMessageResult> {
    const payload = await this.sendMessageRaw(agentId, text, opts);
    if (isA2AError(payload as unknown as Record<string, unknown>)) {
      throw new A2AOperationError(payload as unknown as A2AError);
    }
    const res = payload as unknown as SendMessageResponse;
    if (!res.task && !res.message) {
      throw new A2AProtocolError("agent returned neither task nor message");
    }
    return res;
  }

  /** Low-level send: returns the raw reply payload (response or A2AError). */
  async sendMessageRaw(
    agentId: string,
    text: string | Message,
    opts: SendOptions = {},
  ): Promise<Record<string, unknown>> {
    const id = sanitizeAgentId(agentId);
    const message: Message =
      typeof text === "string"
        ? { messageId: uuid(), role: "ROLE_USER", parts: [{ text, mediaType: "text/plain" }] }
        : text;
    if (opts.taskId !== undefined) message.taskId = opts.taskId;
    if (opts.contextId !== undefined) message.contextId = opts.contextId;

    const request: SendMessageRequest = {
      message,
      ...(opts.acceptedOutputModes || opts.returnImmediately !== undefined
        ? {
            configuration: {
              ...(opts.acceptedOutputModes ? { acceptedOutputModes: opts.acceptedOutputModes } : {}),
              ...(opts.returnImmediately !== undefined ? { returnImmediately: opts.returnImmediately } : {}),
            },
          }
        : {}),
    };

    return this.requestPubMsg(
      subjects(id).rpc,
      OP.sendMessage,
      request as unknown as Record<string, unknown>,
      opts.timeoutMs ?? this.requestTimeoutMs,
      opts.taskId !== undefined ? { [HDR_TASK_ID]: opts.taskId } : {},
    );
  }

  // ------------------------------------------------------------------
  // message/stream
  // ------------------------------------------------------------------

  /**
   * Send with streaming. Yields events (initial Task snapshot, status
   * updates, artifact chunks) and stops at the terminal state update.
   *
   * ```ts
   * for await (const ev of client.sendMessageStream("agent", "do it")) {
   *   if (ev.kind === "artifact_update") console.log(ev.artifactUpdate?.artifact);
   * }
   * ```
   */
  async *sendMessageStream(
    agentId: string,
    text: string | Message,
    opts: SendOptions = {},
  ): AsyncGenerator<StreamEvent, void, unknown> {
    if (!this.nc) throw new A2AProtocolError("client not connected");
    const id = sanitizeAgentId(agentId);
    const correlationId = `c-${uuid()}`;
    const timeoutMs = opts.timeoutMs ?? this.requestTimeoutMs;
    const inbox = `_a2a.stream.${correlationId}`;

    const message: Message =
      typeof text === "string"
        ? { messageId: uuid(), role: "ROLE_USER", parts: [{ text, mediaType: "text/plain" }] }
        : text;
    if (opts.taskId !== undefined) message.taskId = opts.taskId;
    if (opts.contextId !== undefined) message.contextId = opts.contextId;
    const request: SendMessageRequest = { message };

    const sub = this.nc.subscribe(inbox);
    // nats.js Subscription is iterator-only; syncIterator() exposes a
    // null-terminating next() we can race against a deadline, so a silent
    // agent cannot hang the consumer forever.
    const iter = syncIterator(sub);
    // The deadline timer MUST be cleared on exit or it keeps the node event
    // loop alive for the full timeout (test runner / CLI hang).
    let timerRef: NodeJS.Timeout | undefined;
    const deadline = new Promise<"deadline">((resolve) => {
      timerRef = setTimeout(() => resolve("deadline"), timeoutMs);
      (timerRef as NodeJS.Timeout & { unref?: () => void }).unref?.();
    });
    try {
      // Fire the request with reply = dedicated inbox; agent streams PubEvents there.
      this.nc.publish(
        subjects(id).rpc,
        jc.encode(makePubMsg(this.clientId, request as unknown as Record<string, unknown>)),
        {
          headers: makeHeaders(
            this.clientId,
            correlationId,
            {
              "X-A2A-Op": OP.sendStreamingMessage,
              ...(opts.taskId !== undefined ? { [HDR_TASK_ID]: opts.taskId } : {}),
            },
          ),
          reply: inbox,
        },
      );

      while (true) {
        const pulled = await Promise.race([iter.next(), deadline]);
        if (pulled === "deadline") {
          throw new A2ATimeoutError(inbox, timeoutMs);
        }
        if (pulled === null || pulled === undefined) return; // subscription closed
        const msg = pulled as Msg;
        let body: unknown;
        try {
          body = jc.decode(msg.data);
        } catch {
          continue;
        }
        if (!isPubEvent(body)) continue;
        yield mapEvent(body);
        if (body.event === "status_update") {
          const st = (body.payload as unknown as TaskStatusUpdateEvent).status?.state;
          if (st && isTerminalState(st)) return;
        }
      }
    } catch (err) {
      if (err instanceof A2ATimeoutError) throw err;
      if (err instanceof Error && /timeout/i.test(err.message)) {
        throw new A2ATimeoutError(inbox, timeoutMs);
      }
      throw err;
    } finally {
      if (timerRef) clearTimeout(timerRef);
      sub.unsubscribe();
    }
  }

  // ------------------------------------------------------------------
  // tasks/*
  // ------------------------------------------------------------------

  async getTask(agentId: string, taskId: string, opts: { timeoutMs?: number } = {}): Promise<Task> {
    const id = sanitizeAgentId(agentId);
    const request: GetTaskRequest = { id: taskId };
    const payload = await this.requestPubMsg(
      subjects(id).rpc,
      OP.getTask,
      request as unknown as Record<string, unknown>,
      opts.timeoutMs ?? 15_000,
      { [HDR_TASK_ID]: taskId },
    );
    if (isA2AError(payload as unknown as Record<string, unknown>)) throw new A2AOperationError(payload as unknown as A2AError);
    return payload as unknown as Task;
  }

  async listTasks(
    agentId: string,
    filter: Omit<ListTasksRequest, "tenant"> = {},
    opts: { timeoutMs?: number } = {},
  ): Promise<ListTasksResponse> {
    const id = sanitizeAgentId(agentId);
    const payload = await this.requestPubMsg(
      subjects(id).rpc,
      OP.listTasks,
      filter as unknown as Record<string, unknown>,
      opts.timeoutMs ?? 15_000,
    );
    if (isA2AError(payload as unknown as Record<string, unknown>)) throw new A2AOperationError(payload as unknown as A2AError);
    return payload as unknown as ListTasksResponse;
  }

  /** Request cooperative cancellation. Returns the (possibly stale) task snapshot. */
  async cancelTask(agentId: string, taskId: string, opts: { timeoutMs?: number } = {}): Promise<Task> {
    const id = sanitizeAgentId(agentId);
    const request: CancelTaskRequest = { id: taskId };
    const payload = await this.requestPubMsg(
      subjects(id).rpc,
      OP.cancelTask,
      request as unknown as Record<string, unknown>,
      opts.timeoutMs ?? 15_000,
      { [HDR_TASK_ID]: taskId },
    );
    if (isA2AError(payload as unknown as Record<string, unknown>)) throw new A2AOperationError(payload as unknown as A2AError);
    return payload as unknown as Task;
  }

  // ------------------------------------------------------------------
  // internals
  // ------------------------------------------------------------------

  private async requestPubMsg(
    subject: string,
    op: string,
    payload: Record<string, unknown>,
    timeoutMs: number,
    extraHeaders: Record<string, string> = {},
  ): Promise<Record<string, unknown>> {
    if (!this.nc) throw new A2AProtocolError("client not connected");
    const correlationId = `c-${uuid()}`;
    const envelope = makePubMsg(this.clientId, payload);
    try {
      const reply = await this.nc.request(subject, jc.encode(envelope), {
        timeout: timeoutMs,
        headers: makeHeaders(this.clientId, correlationId, { "X-A2A-Op": op, ...extraHeaders }),
      });
      const body = jc.decode(reply.data) as unknown;
      if (!isPubMsg(body)) {
        throw new A2AProtocolError(`reply on ${subject} is not a PubMsg envelope`);
      }
      return body.payload as Record<string, unknown>;
    } catch (err) {
      if (err instanceof Error) {
        const code = (err as Error & { code?: string }).code;
        const noResponders =
          code === "503" || err.message.includes("responders") || err.message.includes("503");
        if (noResponders || /timeout/i.test(err.message)) {
          throw new A2ATimeoutError(subject, timeoutMs);
        }
      }
      throw err;
    }
  }
}

function mapEvent(body: { event: string; seq: number; payload: unknown }): StreamEvent {
  switch (body.event) {
    case "task":
      return { kind: "task", task: body.payload as unknown as Task, seq: body.seq };
    case "message":
      return { kind: "message", message: body.payload as unknown as Message, seq: body.seq };
    case "status_update":
      return {
        kind: "status_update",
        statusUpdate: body.payload as unknown as TaskStatusUpdateEvent,
        seq: body.seq,
      };
    case "artifact_update":
      return {
        kind: "artifact_update",
        artifactUpdate: body.payload as unknown as TaskArtifactUpdateEvent,
        seq: body.seq,
      };
    default:
      return { kind: "status_update", seq: body.seq };
  }
}
