import type {
  A2AError,
  PubEvent,
  PubMsg,
} from "./contract.js";
import { HDR_BINDING, NATS_BINDING_URI } from "./contract.js";
import type { Message, Part, Role } from "./types.js";
import { headers as natsHeaders } from "nats";
import type { Msg, MsgHdrs, NatsConnection, PublishOptions } from "nats";

/**
 * Helpers shared by client and agent implementations.
 */

export function nowIso(): string {
  return new Date().toISOString();
}

/** UUID v4 without external deps. */
export function uuid(): string {
  return crypto.randomUUID();
}

export function newCorrelationId(): string {
  return `c-${uuid()}`;
}

export function makePart(text: string): Part {
  return { text, mediaType: "text/plain" };
}

export function makeMessage(
  text: string,
  opts: { role?: Role; taskId?: string; contextId?: string; messageId?: string } = {},
): Message {
  const msg: Message = {
    messageId: opts.messageId ?? uuid(),
    role: opts.role ?? "ROLE_USER",
    parts: [makePart(text)],
  };
  if (opts.taskId !== undefined) msg.taskId = opts.taskId;
  if (opts.contextId !== undefined) msg.contextId = opts.contextId;
  return msg;
}

export function makePubMsg(publisher: string, payload: unknown): PubMsg {
  return {
    kind: "pubmsg",
    binding: NATS_BINDING_URI,
    publisher,
    ts: nowIso(),
    payload,
  };
}

export function makePubEvent(
  publisher: string,
  correlationId: string,
  taskId: string,
  seq: number,
  event: PubEvent["event"],
  payload: unknown,
): PubEvent {
  return {
    kind: "pubevent",
    binding: NATS_BINDING_URI,
    publisher,
    ts: nowIso(),
    correlationId,
    taskId,
    seq,
    event,
    payload,
  };
}

export function makeA2AError(
  code: A2AError["code"],
  message: string,
  task?: A2AError["task"],
): A2AError {
  const err: A2AError = { code, message };
  if (task !== undefined) err.task = task;
  return err;
}

export function makeHeaders(
  publisher: string,
  correlationId: string,
  extra: Record<string, string> = {},
): MsgHdrs {
  const h = natsHeaders();
  h.set(HDR_BINDING, NATS_BINDING_URI);
  h.set("X-A2A-Publisher", publisher);
  h.set("X-A2A-Correlation-Id", correlationId);
  for (const [k, v] of Object.entries(extra)) {
    const value = Array.isArray(v) ? v.join(",") : String(v);
    h.set(k, value);
  }
  return h;
}

/** JSON-encode and publish with binding headers. */
export async function publishJson(
  nc: NatsConnection,
  subject: string,
  data: unknown,
  headers?: MsgHdrs,
): Promise<void> {
  const payload = JSON.stringify(data);
  const opts: PublishOptions = headers ? { headers } : {};
  await nc.publish(subject, payload, opts);
}

/** JSON-encode and publish with binding headers and an explicit reply subject. */
export async function publishJsonWithReply(
  nc: NatsConnection,
  subject: string,
  data: unknown,
  headers?: MsgHdrs,
  reply?: string,
): Promise<void> {
  const payload = JSON.stringify(data);
  await nc.publish(subject, payload, { headers, reply });
}

/** Extract headers into a plain record (NATS header values may repeat). */
export function headersToRecord(msg: Msg): Record<string, string> {
  const out: Record<string, string> = {};
  if (msg.headers) {
    for (const [k, v] of msg.headers) out[k] = Array.isArray(v) ? v.join(", ") : v;
  }
  return out;
}

/** Get a header value case-insensitively (NATS header names are case-sensitive; be lenient). */
export function getHeader(msg: Msg, name: string): string | undefined {
  if (!msg.headers) return undefined;
  // exact
  const direct = msg.headers.get(name);
  if (direct !== undefined) return direct;
  const lower = name.toLowerCase();
  for (const [k, v] of msg.headers) {
    if (k.toLowerCase() === lower) return Array.isArray(v) ? v[0] : v;
  }
  return undefined;
}

/**
 * Subject name sanitizer — agent ids become NATS subject tokens, which must
 * not contain spaces, wildcards (`*`, `>`), or separators (`.`).
 */
export function sanitizeAgentId(agentId: string): string {
  const cleaned = agentId.trim().replace(/[^A-Za-z0-9_-]/g, "-");
  if (cleaned.length === 0) throw new Error("agent id must not be empty");
  return cleaned;
}

/** Build the AgentInterface url field for a NATS endpoint. */
export function natsInterfaceUrl(natsUrl: string, agentId: string): string {
  const base = natsUrl.replace(/\/+$/, "");
  return `${base}/a2a.v1.agent.${sanitizeAgentId(agentId)}`;
}
