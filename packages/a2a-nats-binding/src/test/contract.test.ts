import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  DISCOVERY,
  HDR_CORRELATION_ID,
  NATS_BINDING_URI,
  OP,
  isPubEvent,
  isPubMsg,
  isTerminalState,
  subjects,
} from "../contract.js";
import {
  makeHeaders,
  makePubEvent,
  makePubMsg,
  natsInterfaceUrl,
  sanitizeAgentId,
} from "../util.js";
import type { TaskStatusUpdateEvent } from "../types.js";

describe("contract", () => {
  it("builds per-agent subjects", () => {
    const s = subjects("echo-agent");
    assert.equal(s.rpc, "a2a.v1.agent.echo-agent");
    assert.equal(s.events, "a2a.v1.agent.echo-agent.events");
    assert.equal(s.taskControl("t1"), "a2a.v1.agent.echo-agent.task.t1.ctl");
  });

  it("classifies terminal states", () => {
    assert.equal(isTerminalState("TASK_STATE_COMPLETED"), true);
    assert.equal(isTerminalState("TASK_STATE_FAILED"), true);
    assert.equal(isTerminalState("TASK_STATE_WORKING"), false);
    assert.equal(isTerminalState("TASK_STATE_INPUT_REQUIRED"), false);
  });
});

describe("envelopes", () => {
  it("round-trips a PubMsg through JSON", () => {
    const env = makePubMsg("client-1", { hello: "world" });
    const json = JSON.parse(JSON.stringify(env));
    assert.equal(isPubMsg(json), true);
    assert.equal(isPubEvent(json), false);
    assert.equal(env.binding, NATS_BINDING_URI);
    assert.equal(typeof env.ts, "string");
  });

  it("round-trips a PubEvent through JSON", () => {
    const ev: TaskStatusUpdateEvent = {
      taskId: "t1",
      contextId: "c1",
      status: { state: "TASK_STATE_WORKING", timestamp: new Date().toISOString() },
    };
    const env = makePubEvent("agent-1", "corr-1", "t1", 3, "status_update", ev);
    const json = JSON.parse(JSON.stringify(env));
    assert.equal(isPubEvent(json), true);
    assert.equal(isPubMsg(json), false);
    assert.equal(json.seq, 3);
  });
});

describe("util", () => {
  it("sanitizes agent ids for subject tokens", () => {
    assert.equal(sanitizeAgentId("My Agent!"), "My-Agent-");
    assert.equal(sanitizeAgentId("a.b/c*d"), "a-b-c-d");
    assert.throws(() => sanitizeAgentId("   "));
  });

  it("builds nats interface urls", () => {
    assert.equal(
      natsInterfaceUrl("nats://nats.my:4222", "echo"),
      "nats://nats.my:4222/a2a.v1.agent.echo",
    );
  });

  it("builds headers with binding + correlation", () => {
    const h = makeHeaders("pub", "corr-9", { "X-Extra": "1" });
    assert.equal(h.get("X-A2A-Binding"), NATS_BINDING_URI);
    assert.equal(h.get(HDR_CORRELATION_ID), "corr-9");
    assert.equal(h.get("X-Extra"), "1");
  });
});

describe("wire-op map", () => {
  it("uses canonical A2A operation names", () => {
    assert.equal(OP.sendMessage, "message/send");
    assert.equal(OP.sendStreamingMessage, "message/stream");
    assert.equal(OP.getTask, "tasks/get");
    assert.equal(OP.cancelTask, "tasks/cancel");
    assert.equal(DISCOVERY.advertise, "a2a.v1.discovery.advertise");
  });
});
