/**
 * A2A v1.0 data model — ProtoJSON-compatible TypeScript types.
 *
 * These mirror `specification/a2a.proto` of the A2A spec (package lf.a2a.v1)
 * with ProtoJSON naming: camelCase properties, string enum names on the wire,
 * ISO-8601 UTC timestamps, base64-encoded bytes.
 *
 * Derived from the normative proto (A2A 1.0.0, 2026-11-11). Do not rename
 * fields — wire compatibility with the official SDKs depends on them.
 */

export type TaskState =
  | "TASK_STATE_UNSPECIFIED"
  | "TASK_STATE_SUBMITTED"
  | "TASK_STATE_WORKING"
  | "TASK_STATE_COMPLETED"
  | "TASK_STATE_FAILED"
  | "TASK_STATE_CANCELED"
  | "TASK_STATE_INPUT_REQUIRED"
  | "TASK_STATE_REJECTED"
  | "TASK_STATE_AUTH_REQUIRED";

export type Role = "ROLE_UNSPECIFIED" | "ROLE_USER" | "ROLE_AGENT";

/** ProtoJSON Value: any JSON value. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface Part {
  /** Exactly one of: text | raw (base64) | url | data (JSON value). */
  text?: string;
  /** File bytes, base64-encoded on the wire (ProtoJSON bytes rule). */
  raw?: string;
  url?: string;
  data?: JsonValue;
  metadata?: { [key: string]: JsonValue };
  filename?: string;
  mediaType?: string;
}

export interface Message {
  /** Created by the message sender (UUID recommended). */
  messageId: string;
  contextId?: string;
  taskId?: string;
  role: Role;
  parts: Part[];
  metadata?: { [key: string]: JsonValue };
  /** URIs of extensions present in / contributing to this message. */
  extensions?: string[];
  /** Task ids this message references for additional context. */
  referenceTaskIds?: string[];
}

export interface TaskStatus {
  state: TaskState;
  message?: Message;
  /** ISO-8601 UTC, e.g. "2023-10-27T10:00:00Z". */
  timestamp?: string;
}

export interface Artifact {
  /** Unique within a task. */
  artifactId: string;
  name?: string;
  description?: string;
  parts: Part[];
  metadata?: { [key: string]: JsonValue };
  extensions?: string[];
}

export interface Task {
  id: string;
  contextId?: string;
  status: TaskStatus;
  artifacts?: Artifact[];
  history?: Message[];
  metadata?: { [key: string]: JsonValue };
}

export interface TaskStatusUpdateEvent {
  taskId: string;
  contextId: string;
  status: TaskStatus;
  metadata?: { [key: string]: JsonValue };
}

export interface TaskArtifactUpdateEvent {
  taskId: string;
  contextId: string;
  artifact: Artifact;
  /** Append content to a previously sent artifact with the same id. */
  append?: boolean;
  /** Final chunk of the artifact. */
  lastChunk?: boolean;
  metadata?: { [key: string]: JsonValue };
}

export interface AgentInterface {
  /**
   * Address where the interface is available. For this binding the form is
   * `nats://<broker-host>[:port]/<subjectRoot>` — e.g.
   * `nats://nats.my:4222/a2a.v1.agent.echo-agent`.
   */
  url: string;
  /** Binding URI. Core: JSONRPC, GRPC, HTTP+JSON. Custom bindings use URIs. */
  protocolBinding: string;
  tenant?: string;
  /** e.g. "1.0". */
  protocolVersion: string;
}

export interface AgentProvider {
  url: string;
  organization: string;
}

export interface AgentExtension {
  uri: string;
  description?: string;
  required?: boolean;
  params?: { [key: string]: JsonValue };
}

export interface AgentCapabilities {
  streaming?: boolean;
  pushNotifications?: boolean;
  extensions?: AgentExtension[];
  extendedAgentCard?: boolean;
}

export interface AgentSkill {
  id: string;
  name: string;
  description: string;
  tags: string[];
  examples?: string[];
  inputModes?: string[];
  outputModes?: string[];
}

export interface AgentCard {
  name: string;
  description: string;
  /** Ordered; first entry is preferred. */
  supportedInterfaces: AgentInterface[];
  provider?: AgentProvider;
  version: string;
  documentationUrl?: string;
  capabilities: AgentCapabilities;
  defaultInputModes: string[];
  defaultOutputModes: string[];
  skills: AgentSkill[];
  iconUrl?: string;
}

export interface SendMessageConfiguration {
  /** Media types the client accepts for response parts. */
  acceptedOutputModes?: string[];
  /** Push notification config (bind-send only; webhooks do not apply over NATS). */
  historyLength?: number;
  /**
   * true: reply as soon as the task is accepted (state submitted/working).
   * false (default): reply only at terminal/interrupted state.
   */
  returnImmediately?: boolean;
}

export interface SendMessageRequest {
  tenant?: string;
  message: Message;
  configuration?: SendMessageConfiguration;
  metadata?: { [key: string]: JsonValue };
}

export interface SendMessageResponse {
  task?: Task;
  message?: Message;
}

export interface GetTaskRequest {
  tenant?: string;
  id: string;
  historyLength?: number;
}

export interface ListTasksRequest {
  tenant?: string;
  contextId?: string;
  status?: TaskState;
  pageSize?: number;
  pageToken?: string;
  historyLength?: number;
  includeArtifacts?: boolean;
}

export interface ListTasksResponse {
  tasks: Task[];
  nextPageToken: string;
  pageSize: number;
  totalSize: number;
}

export interface CancelTaskRequest {
  tenant?: string;
  id: string;
  metadata?: { [key: string]: JsonValue };
}

export interface StreamResponse {
  task?: Task;
  message?: Message;
  statusUpdate?: TaskStatusUpdateEvent;
  artifactUpdate?: TaskArtifactUpdateEvent;
}
