/**
 * Wire contract for the A2A-over-NATS protocol binding.
 *
 * Every A2A object on the wire is ProtoJSON (`lf.a2a.v1` proto, spec 1.0.0):
 * camelCase field names, string enum names, ISO-8601 UTC timestamps,
 * base64 `raw` bytes. One message = one PubMsg envelope; the A2A payload
 * lives in `payload`, binding metadata (op, correlation, task routing)
 * travels in NATS message headers.
 */

/** URI that identifies this binding in an AgentCard `supportedInterfaces` entry. */
import type { Task } from "./types.js";

export const NATS_BINDING_URI = "https://github.com/a2a-overnats/bindings/nats/v1" as const;

/** Root subject namespace. Overridable for multi-tenant brokers. */
export const SUBJECT_PREFIX = "a2a.v1" as const;

/** A2A protocol version this binding implements. */
export const A2A_PROTOCOL_VERSION = "1.0" as const;

/** Binding implementation version. */
export const BINDING_VERSION = "0.1.0" as const;

/** NATS header: the A2A operation being invoked (`message/send`, `tasks/get`, ...). */
export const HDR_OP = "X-A2A-Op" as const;

/** NATS header: correlation id shared by a request and all of its stream events. */
export const HDR_CORRELATION_ID = "X-A2A-Correlation-Id" as const;

/** NATS header: task id, for subjects that may carry several task streams. */
export const HDR_TASK_ID = "X-A2A-Task-Id" as const;

/** NATS header: context id (A2A conversation grouping). */
export const HDR_CONTEXT_ID = "X-A2A-Context-Id" as const;

/** NATS header: publisher identity (agent id or `client:<id>`). */
export const HDR_PUBLISHER = "X-A2A-Publisher" as const;

/**
 * NATS header: pub-sub reference binding URI. Present on every message so a
 * packet capture or a generic bridge can identify traffic without parsing JSON.
 */
export const HDR_BINDING = "X-A2A-Binding" as const;

/** Envelope carried in the NATS message body for request/response operations.
 * The A2A payload (ProtoJSON) is kept verbatim so standard-binding clients can
 * round-trip objects through an HTTP shim unchanged.
 */
export interface PubMsg<T = unknown> {
  /** Envelope format tag. */
  kind: "pubmsg";
  /** Binding identifier, mirrors HDR_BINDING. */
  binding: string;
  /** Identifier of the sending party. */
  publisher: string;
  /** RFC 3339 UTC timestamp of publication. */
  ts: string;
  /** The A2A request/response payload (ProtoJSON-encoded). */
  payload: T;
}

/**
 * Envelope for stream events (`message/stream`, `tasks/subscribe`).
 * Carries a monotonic sequence number so consumers can detect drops/reorder.
 */
export interface PubEvent<T = unknown> {
  /** Envelope format tag. */
  kind: "pubevent";
  /** Binding identifier. */
  binding: string;
  /** Identifier of the sending party. */
  publisher: string;
  /** RFC 3339 UTC timestamp of publication. */
  ts: string;
  /** Correlation id: matches the originating request's HDR_CORRELATION_ID. */
  correlationId: string;
  /** Task id this event belongs to. */
  taskId: string;
  /** Monotonic per-stream sequence, starting at 1. */
  seq: number;
  /** Discriminator mirroring the StreamResponse oneof. */
  event:
    | "task"
    | "message"
    | "status_update"
    | "artifact_update";
  /** The A2A payload (ProtoJSON-encoded Task / Message / TaskStatusUpdateEvent / TaskArtifactUpdateEvent). */
  payload: T;
}

/** All subjects owned by the binding, parameterised by agent id. */
export function subjects(agentId: string) {
  const root = `${SUBJECT_PREFIX}.agent.${agentId}`;
  return {
    /** Request/reply entrypoint: message/send, message/stream, tasks/*, agent card. */
    rpc: root,
    /** Broadcast of every task lifecycle event this agent produces. */
    events: `${root}.events`,
    /** Cooperative cancel control subject for a specific task. */
    taskControl: (taskId: string) => `${SUBJECT_PREFIX}.agent.${agentId}.task.${taskId}.ctl`,
  } as const;
}

/** Discovery subjects (agent-wide, not per-agent). */
export const DISCOVERY = {
  /** Agents publish their AgentCard here on start + every heartbeat. */
  advertise: `${SUBJECT_PREFIX}.discovery.advertise`,
  /** Request/reply "give me your card" to a specific agent subject. */
  card: (agentId: string) => `${SUBJECT_PREFIX}.discovery.card.${agentId}`,
} as const;

/** Heartbeat interval for discovery adverts (ms). */
export const HEARTBEAT_INTERVAL_MS = 15_000;

/** TTL after which an agent without a heartbeat is considered offline. */
export const OFFLINE_AFTER_MS = 45_000;

/** Operation names (canonical A2A method names, slash form). */
export const OP = {
  sendMessage: "message/send",
  sendStreamingMessage: "message/stream",
  getTask: "tasks/get",
  listTasks: "tasks/list",
  cancelTask: "tasks/cancel",
  getAgentCard: "agent/getCard",
} as const;

export type OpName = (typeof OP)[keyof typeof OP];

/** Binding-level error taxonomy (maps A2A error semantics onto NATS replies). */
export type A2AErrorCode =
  | "TASK_NOT_FOUND"
  | "TASK_NOT_CANCELABLE"
  | "UNSUPPORTED_OPERATION"
  | "CONTENT_TYPE_NOT_SUPPORTED"
  | "INVALID_REQUEST"
  | "INTERNAL_ERROR";

export interface A2AError {
  code: A2AErrorCode;
  message: string;
  /** Task object when relevant (e.g. cancel on a terminal task). */
  task?: Task;
}

/** Terminal task states (proto TASK_STATE_*). */
export const TERMINAL_STATES = new Set([
  "TASK_STATE_COMPLETED",
  "TASK_STATE_FAILED",
  "TASK_STATE_CANCELED",
  "TASK_STATE_REJECTED",
]);

/** Interrupted (paused) task states that still allow further input. */
export const INTERRUPTED_STATES = new Set([
  "TASK_STATE_INPUT_REQUIRED",
  "TASK_STATE_AUTH_REQUIRED",
]);

/** True if the given TaskState (string enum name) is terminal. */
export function isTerminalState(state: string): boolean {
  return TERMINAL_STATES.has(state);
}

/** Validate the minimal invariants of an inbound PubMsg. */
export function isPubMsg(v: unknown): v is PubMsg {
  return (
    typeof v === "object" && v !== null &&
    (v as PubMsg).kind === "pubmsg" &&
    typeof (v as PubMsg).publisher === "string" &&
    typeof (v as PubMsg).payload === "object"
  );
}

/** Validate the minimal invariants of an inbound PubEvent. */
export function isPubEvent(v: unknown): v is PubEvent {
  const e = v as PubEvent;
  return (
    typeof e === "object" && e !== null &&
    e.kind === "pubevent" &&
    typeof e.correlationId === "string" &&
    typeof e.taskId === "string" &&
    typeof e.seq === "number" &&
    typeof e.payload === "object"
  );
}
