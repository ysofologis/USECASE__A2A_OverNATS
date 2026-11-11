import { connect, JSONCodec } from "nats";
import type { ConnectionOptions, Msg, NatsConnection, Subscription } from "nats";
import {
  A2A_PROTOCOL_VERSION,
  BINDING_VERSION,
  DISCOVERY,
  HEARTBEAT_INTERVAL_MS,
  HDR_CORRELATION_ID,
  HDR_TASK_ID,
  NATS_BINDING_URI,
  OP,
  SUBJECT_PREFIX,
  isPubMsg,
  isTerminalState,
  subjects,
} from "./contract.js";
import type { A2AError } from "./contract.js";
import type {
  CancelTaskRequest,
  Artifact,
  GetTaskRequest,
  ListTasksRequest,
  ListTasksResponse,
  Message,
  Part,
  SendMessageRequest,
  SendMessageResponse,
  Task,
  TaskArtifactUpdateEvent,
  TaskStatusUpdateEvent,
} from "./types.js";
import {
  getHeader,
  makeA2AError,
  makeHeaders,
  makePubEvent,
  makePubMsg,
  natsInterfaceUrl,
  nowIso,
  publishJson,
  sanitizeAgentId,
  uuid,
} from "./util.js";

const jc = JSONCodec();

/** Max tasks kept in memory (oldest terminal tasks are pruned first). */
const MAX_TASKS = 1000;

export interface AgentSkillDef {
  id: string;
  name: string;
  description: string;
  tags: string[];
  examples?: string[];
  inputModes?: string[];
  outputModes?: string[];
}

export interface AgentOptions {
  /** Unique agent id (becomes a NATS subject token). */
  agentId: string;
  humanName: string;
  description: string;
  skills: AgentSkillDef[];
  /** NATS server URL(s), e.g. `nats://nats.my:4222`. */
  natsUrl: string;
  /** Extra AgentCard fields. */
  provider?: { organization: string; url: string };
  version?: string;
  /** Default media types. */
  inputModes?: string[];
  outputModes?: string[];
  /** Publish discovery adverts (default true). */
  advertise?: boolean;
}

/**
 * Handler-facing context: the task + helpers to stream progress without
 * knowing about NATS. The handler must either return a result (auto-completed
 * as a text/JSON artifact), or call complete()/fail() itself.
 */
export interface TaskContext {
  readonly task: Readonly<Task>;
  readonly message: Readonly<Message>;
  /** Publish a working status update (does not end the task). */
  working(message?: string): Promise<void>;
  /** Stream an artifact. Chunked: `append: true` per chunk, `lastChunk: true` on the last. */
  artifact(
    artifactId: string,
    parts: Part[],
    opts?: { name?: string; append?: boolean; lastChunk?: boolean },
  ): Promise<void>;
  /** Terminate the task as completed. */
  complete(message?: string, artifacts?: Array<{ id: string; parts: Part[]; name?: string }>): Promise<void>;
  /** Terminate the task as failed. */
  fail(message: string): Promise<void>;
  /** True if tasks/cancel was received for this task (cooperative cancel). */
  cancelRequested(): boolean;
}

export type TaskHandler = (ctx: TaskContext) => Promise<void | Message | Part[]>;

/** Thrown by handlers to fail a task with a specific A2A error code. */
export class TaskFailed extends Error {
  constructor(
    message: string,
    public readonly code: A2AError["code"] = "INTERNAL_ERROR",
  ) {
    super(message);
    this.name = "TaskFailed";
  }
}

type PubEventType = "task" | "message" | "status_update" | "artifact_update";

/**
 * An A2A agent exposed over NATS via the pub-msg binding.
 *
 * Operations implemented:
 * - message/send       request/reply (waits for terminal/interrupted state by default)
 * - message/stream     request + streamed PubEvents on the reply subject
 * - tasks/get, tasks/list, tasks/cancel (cooperative)
 * - discovery: agent card request/reply + periodic advertise broadcasts
 */
export class A2ANatsAgent {
  private nc?: NatsConnection;
  private subs: Subscription[] = [];
  private tasks = new Map<string, Task>();
  private cancelRequested = new Set<string>();
  private advertiseTimer?: NodeJS.Timeout;
  private running = false;
  private handler?: TaskHandler;
  readonly agentId: string;
  private readonly opts: AgentOptions;

  constructor(opts: AgentOptions) {
    this.opts = opts;
    this.agentId = sanitizeAgentId(opts.agentId);
  }

  /** Register the task handler. Call before start(). */
  onTask(handler: TaskHandler): this {
    this.handler = handler;
    return this;
  }

  /** Connect, subscribe, start advertising. */
  async start(): Promise<void> {
    if (this.running) return;
    const connOpts: ConnectionOptions = {
      servers: [this.opts.natsUrl],
      name: `a2a-agent-${this.agentId}`,
    };
    this.nc = await connect(connOpts);
    this.running = true;

    const s = subjects(this.agentId);
    this.subs.push(
      this.nc.subscribe(s.rpc, {
        callback: (err, m) => {
          if (err) return;
          void this.onRpc(m);
        },
      }),
    );

    if (this.opts.advertise !== false) {
      await this.publishAdvert();
      this.advertiseTimer = setInterval(() => void this.publishAdvert(), HEARTBEAT_INTERVAL_MS);
      this.subs.push(
        this.nc.subscribe(DISCOVERY.card(this.agentId), {
          callback: (err, m) => {
            if (err) return;
            this.onCardRequest(m);
          },
        }),
      );
    }
  }

  /** Stop advertising, drop subscriptions, drain the connection. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    if (this.advertiseTimer) clearInterval(this.advertiseTimer);
    for (const sub of this.subs) sub.unsubscribe();
    this.subs = [];
    await this.nc?.drain();
    this.nc = undefined;
  }

  get connection(): NatsConnection | undefined {
    return this.nc;
  }

  // ------------------------------------------------------------------
  // AgentCard + discovery
  // ------------------------------------------------------------------

  buildAgentCard() {
    return {
      name: this.opts.humanName,
      description: this.opts.description,
      supportedInterfaces: [
        {
          url: natsInterfaceUrl(this.opts.natsUrl, this.agentId),
          protocolBinding: NATS_BINDING_URI,
          protocolVersion: A2A_PROTOCOL_VERSION,
        },
      ],
      ...(this.opts.provider ? { provider: this.opts.provider } : {}),
      version: this.opts.version ?? BINDING_VERSION,
      capabilities: {
        streaming: true,
        pushNotifications: false, // bus subscriptions replace webhooks
      },
      defaultInputModes: this.opts.inputModes ?? ["text/plain", "application/json"],
      defaultOutputModes: this.opts.outputModes ?? ["text/plain", "application/json"],
      skills: this.opts.skills,
    };
  }

  private async publishAdvert(): Promise<void> {
    if (!this.nc) return;
    await publishJson(
      this.nc,
      DISCOVERY.advertise,
      makePubMsg(this.agentId, this.buildAgentCard()),
      makeHeaders(this.agentId, "advertise", { "X-A2A-Op": "discovery/advertise" }),
    );
  }

  private onCardRequest(msg: Msg): void {
    if (!msg.reply) return;
    msg.respond(jc.encode(makePubMsg(this.agentId, this.buildAgentCard())));
  }

  // ------------------------------------------------------------------
  // RPC dispatch
  // ------------------------------------------------------------------

  private async onRpc(msg: Msg, err?: unknown): Promise<void> {
    if (err) return;
    let body: unknown;
    try {
      body = jc.decode(msg.data);
    } catch {
      this.replyError(msg, makeA2AError("INVALID_REQUEST", "body is not JSON"));
      return;
    }
    if (!isPubMsg(body)) {
      this.replyError(msg, makeA2AError("INVALID_REQUEST", "body is not a PubMsg envelope"));
      return;
    }

    const op = getHeader(msg, "X-A2A-Op") ?? "";
    switch (op) {
      case OP.sendMessage:
        await this.onSendMessage(msg, body as { payload: unknown }, false);
        return;
      case OP.sendStreamingMessage:
        await this.onSendMessage(msg, body as { payload: unknown }, true);
        return;
      case OP.getTask:
        this.onGetTask(msg, body as { payload: unknown });
        return;
      case OP.listTasks:
        this.onListTasks(msg, body as { payload: unknown });
        return;
      case OP.cancelTask:
        this.onCancelTask(msg, body as { payload: unknown });
        return;
      default:
        this.replyError(msg, makeA2AError("UNSUPPORTED_OPERATION", `unknown op "${op}"`));
    }
  }

  private replyError(msg: Msg, err: A2AError): void {
    if (!msg.reply) return;
    try {
      msg.respond(jc.encode(makePubMsg(this.agentId, err)));
    } catch {
      /* reply subject gone */
    }
  }

  // ------------------------------------------------------------------
  // message/send + message/stream
  // ------------------------------------------------------------------

  private async onSendMessage(msg: Msg, body: { payload: unknown }, streaming: boolean): Promise<void> {
    if (!this.handler) {
      this.replyError(msg, makeA2AError("UNSUPPORTED_OPERATION", "agent has no task handler"));
      return;
    }
    const req = body.payload as unknown as SendMessageRequest;
    const userMsg = req?.message;
    if (!userMsg || !Array.isArray(userMsg.parts)) {
      this.replyError(msg, makeA2AError("INVALID_REQUEST", "missing message/parts"));
      return;
    }

    const taskId = userMsg.taskId ?? uuid();
    const contextId = userMsg.contextId ?? uuid();
    const correlationId = getHeader(msg, HDR_CORRELATION_ID) ?? uuid();
    const replyTo = msg.reply;
    const returnImmediately = Boolean(req.configuration?.returnImmediately);

    const existing = this.tasks.get(taskId);
    if (existing && isTerminalState(existing.status.state)) {
      this.replyError(
        msg,
        makeA2AError("UNSUPPORTED_OPERATION", `task ${taskId} is in terminal state ${existing.status.state}`, existing),
      );
      return;
    }

    const task: Task = existing ?? {
      id: taskId,
      contextId,
      status: { state: "TASK_STATE_SUBMITTED", timestamp: nowIso() },
      history: [],
    };

    const recorded: Message = { ...userMsg, taskId, contextId };
    task.history = [...(task.history ?? []), recorded].slice(-100);
    if (!existing) this.tasks.set(taskId, task);
    this.pruneTasks();

    // ---- event plumbing -------------------------------------------------
    let seq = 0;
    const nextSeq = () => ++seq;

    const publishEvent = async (event: PubEventType, payload: unknown): Promise<void> => {
      if (!this.nc || !replyTo) return;
      await publishJson(
        this.nc,
        replyTo,
        makePubEvent(this.agentId, correlationId, taskId, nextSeq(), event, payload),
        makeHeaders(this.agentId, correlationId, { [HDR_TASK_ID]: taskId }),
      );
    };

    const broadcast = async (event: PubEventType, payload: unknown): Promise<void> => {
      if (!this.nc) return;
      await publishJson(
        this.nc,
        subjects(this.agentId).events,
        makePubEvent(this.agentId, correlationId, taskId, nextSeq(), event, payload),
        makeHeaders(this.agentId, correlationId, { [HDR_TASK_ID]: taskId }),
      );
    };

    const statusUpdate = async (
      state: Task["status"]["state"],
      message?: Message,
    ): Promise<void> => {
      // Cooperative cancel: a requested cancel converts the next update to
      // CANCELED unless the handler reached a terminal state first.
      if (this.cancelRequested.has(taskId) && !isTerminalState(state) && state !== "TASK_STATE_CANCELED") {
        state = "TASK_STATE_CANCELED";
        message = {
          messageId: uuid(),
          role: "ROLE_AGENT",
          parts: [{ text: "canceled by client request" }],
        };
        this.cancelRequested.delete(taskId);
      }
      const status = { state, ...(message ? { message } : {}), timestamp: nowIso() };
      task.status = status;
      const ev: TaskStatusUpdateEvent = { taskId, contextId, status };
      // Stream events only go to the reply subject in streaming mode; in
      // request/reply mode the single reply must stay reserved for the final
      // SendMessageResponse, otherwise nc.request() resolves on the first event.
      if (streaming) await publishEvent("status_update", ev);
      await broadcast("status_update", ev);
    };

    // ---- task context handed to the handler -----------------------------
    const ctx: TaskContext = {
      task,
      message: recorded,
      working: async (message?: string) => {
        await statusUpdate(
          "TASK_STATE_WORKING",
          message ? { messageId: uuid(), role: "ROLE_AGENT", parts: [{ text: message }] } : undefined,
        );
      },
      artifact: async (artifactId, parts, opts = {}) => {
        const ev: TaskArtifactUpdateEvent = {
          taskId,
          contextId,
          artifact: { artifactId, parts, ...(opts.name ? { name: opts.name } : {}) },
          ...(opts.append !== undefined ? { append: opts.append } : {}),
          ...(opts.lastChunk !== undefined ? { lastChunk: opts.lastChunk } : {}),
        };
        // Accumulate into the authoritative task so tasks/get and the final
        // SendMessageResponse reflect streamed artifacts.
        upsertArtifact(ev);
        if (streaming) await publishEvent("artifact_update", ev);
        await broadcast("artifact_update", ev);
      },
      complete: async (message?, artifacts?) => {
        if (artifacts?.length) {
          task.artifacts = [
            ...(task.artifacts ?? []),
            ...artifacts.map((a) => ({ artifactId: a.id, parts: a.parts, ...(a.name ? { name: a.name } : {}) })),
          ];
        }
        await statusUpdate(
          "TASK_STATE_COMPLETED",
          message ? { messageId: uuid(), role: "ROLE_AGENT", parts: [{ text: message }] } : undefined,
        );
      },
      fail: async (message: string) => {
        await statusUpdate("TASK_STATE_FAILED", {
          messageId: uuid(),
          role: "ROLE_AGENT",
          parts: [{ text: message }],
        });
      },
      cancelRequested: () => this.cancelRequested.has(taskId),
    };

    const upsertArtifact = (ev: TaskArtifactUpdateEvent): void => {
      task.artifacts = task.artifacts ?? [];
      const idx = task.artifacts.findIndex((a) => a.artifactId === ev.artifact.artifactId);
      if (idx === -1) {
        task.artifacts.push({ ...ev.artifact });
      } else if (ev.append) {
        const existing = task.artifacts[idx] as Artifact;
        task.artifacts[idx] = { ...existing, parts: [...existing.parts, ...ev.artifact.parts] };
      } else {
        task.artifacts[idx] = { ...ev.artifact }; // replace
      }
    };

    const respond = (payload: SendMessageResponse | A2AError): void => {
      if (!replyTo || !this.nc) return;
      try {
        this.nc.publish(replyTo, jc.encode(makePubMsg(this.agentId, payload)));
      } catch {
        /* reply subject gone */
      }
    };

    // ---- run the handler (lazy: reply strategy controls when it starts) --
    const runFn = async (): Promise<void> => {
      try {
        await statusUpdate("TASK_STATE_WORKING");
        const handler = this.handler as TaskHandler;
        const result = await handler(ctx);
        if (task.status.state === "TASK_STATE_WORKING" || task.status.state === "TASK_STATE_SUBMITTED") {
          if (result !== undefined) {
            const parts: Part[] = Array.isArray(result)
              ? result
              : [{ text: JSON.stringify(result), mediaType: "application/json" }];
            task.artifacts = [...(task.artifacts ?? []), { artifactId: uuid(), parts }];
            await publishEvent("artifact_update", {
              taskId,
              contextId,
              artifact: task.artifacts[task.artifacts.length - 1],
              lastChunk: true,
            });
          }
          await statusUpdate("TASK_STATE_COMPLETED");
        }
      } catch (err) {
        if (err instanceof TaskFailed && err.code !== "INTERNAL_ERROR") {
          await statusUpdate("TASK_STATE_FAILED", {
            messageId: uuid(),
            role: "ROLE_AGENT",
            parts: [{ text: err.message }],
          });
        } else {
          await statusUpdate("TASK_STATE_FAILED", {
            messageId: uuid(),
            role: "ROLE_AGENT",
            parts: [{ text: err instanceof Error ? err.message : String(err) }],
          });
        }
      }
    };

    // ---- reply strategies per mode ---------------------------------------
    if (!streaming && !returnImmediately) {
      await runFn();
      respond({ task });
    } else if (!streaming && returnImmediately) {
      respond({ task }); // submitted snapshot; processing continues
      await runFn();
      await broadcast("status_update", { taskId, contextId, status: task.status });
    } else if (streaming) {
      // Initial task snapshot MUST precede handler events; runFn is lazy so
      // ordering is guaranteed.
      await publishEvent("task", task);
      await runFn();
      await publishEvent("task", task);
    } else {
      // Non-streaming: no events were written to the reply subject.
      respond({ task });
    }
  }

  // ------------------------------------------------------------------
  // tasks/get, tasks/list, tasks/cancel
  // ------------------------------------------------------------------

  private onGetTask(msg: Msg, body: { payload: unknown }): void {
    const req = body.payload as unknown as GetTaskRequest;
    const task = this.tasks.get(req?.id ?? "");
    if (!task) {
      this.replyError(msg, makeA2AError("TASK_NOT_FOUND", `task ${req?.id} not found`));
      return;
    }
    if (msg.reply) msg.respond(jc.encode(makePubMsg(this.agentId, task)));
  }

  private onListTasks(msg: Msg, body: { payload: unknown }): void {
    const req = (body.payload ?? {}) as unknown as ListTasksRequest;
    let list = [...this.tasks.values()];
    if (req.contextId) list = list.filter((t) => t.contextId === req.contextId);
    if (req.status) list = list.filter((t) => t.status.state === req.status);
    const pageSize = Math.max(1, Math.min(req.pageSize ?? 50, 100));
    const total = list.length;
    const page = list.slice(0, pageSize);
    const response: ListTasksResponse = {
      tasks: page,
      nextPageToken: page.length < total ? String(page.length) : "",
      pageSize: page.length,
      totalSize: total,
    };
    if (msg.reply) msg.respond(jc.encode(makePubMsg(this.agentId, response)));
  }

  private onCancelTask(msg: Msg, body: { payload: unknown }): void {
    const req = body.payload as unknown as CancelTaskRequest;
    const task = this.tasks.get(req?.id ?? "");
    if (!task) {
      this.replyError(msg, makeA2AError("TASK_NOT_FOUND", `task ${req?.id} not found`));
      return;
    }
    if (isTerminalState(task.status.state)) {
      this.replyError(
        msg,
        makeA2AError("TASK_NOT_CANCELABLE", `task ${task.id} already ${task.status.state}`, task),
      );
      return;
    }
    this.cancelRequested.add(task.id);
    if (msg.reply) msg.respond(jc.encode(makePubMsg(this.agentId, task)));
  }

  // ------------------------------------------------------------------
  // internals
  // ------------------------------------------------------------------

  private pruneTasks(): void {
    if (this.tasks.size <= MAX_TASKS) return;
    const terminalFirst = [...this.tasks.entries()].sort((a, b) => {
      const at = a[1].status.state;
      const bt = b[1].status.state;
      const aTerm = isTerminalState(at) ? 0 : 1;
      const bTerm = isTerminalState(bt) ? 0 : 1;
      return aTerm - bTerm;
    });
    for (const [id] of terminalFirst.slice(0, this.tasks.size - MAX_TASKS)) {
      this.tasks.delete(id);
      this.cancelRequested.delete(id);
    }
  }
}

/** Prefix used to namespace subjects (exported for client parity). */
export const SUBJECT_ROOT = SUBJECT_PREFIX;
