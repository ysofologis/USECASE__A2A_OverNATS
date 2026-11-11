import type { A2AError } from "./contract.js";

/** Error thrown when an A2A operation fails over the NATS binding. */
export class A2AOperationError extends Error {
  readonly code: A2AError["code"];
  readonly task?: A2AError["task"];

  constructor(err: A2AError) {
    super(err.message);
    this.name = "A2AOperationError";
    this.code = err.code;
    this.task = err.task;
  }

  toJSON(): A2AError {
    const out: A2AError = { code: this.code, message: this.message };
    if (this.task !== undefined) out.task = this.task;
    return out;
  }
}

/** Thrown when the transport times out waiting for a reply. */
export class A2ATimeoutError extends Error {
  constructor(public readonly subject: string, public readonly timeoutMs: number) {
    super(`no reply within ${timeoutMs}ms on ${subject}`);
    this.name = "A2ATimeoutError";
  }
}

/** Thrown when an inbound envelope fails validation. */
export class A2AProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "A2AProtocolError";
  }
}
