# A2A_OverNATS — Agent2Agent (A2A) protocol over NATS

[![A2A](https://img.shields.io/badge/A2A-1.0.0-blue)](https://a2a-protocol.org/latest/)
[![binding](https://img.shields.io/badge/binding-nats%2Fv1-4c8)](https://github.com/a2a-overnats/bindings/nats/v1)
[![test](https://img.shields.io/badge/e2e-9%2F9%20pass-brightgreen)](#tests)

Run the **Agent2Agent (A2A) protocol v1.0** over **NATS** as a custom protocol binding —
so agents discover each other, trade tasks, stream progress, and cancel work **without
exposing a single HTTP port**. Ships as a low-level binding package plus a turnkey
integration client ("add A2A to any Node/TS agent in ~10 lines").

- Spec: [A2A v1.0.0](https://a2a-protocol.org/latest/specification/) (Linux Foundation / AAIF)
- Binding declaration: `AgentCard.supportedInterfaces[].protocolBinding = https://github.com/a2a-overnats/bindings/nats/v1`
- Wire compatibility: A2A objects travel **verbatim as ProtoJSON** (`lf.a2a.v1`) — camelCase
  fields, `TASK_STATE_*` string enums, ISO-8601 UTC timestamps — identical to the standard
  JSON-RPC/HTTP bindings, so an HTTP shim can interop with the rest of the ecosystem.

## Why a NATS binding

| HTTP-first A2A | A2A over NATS |
| --- | --- |
| every agent exposes an endpoint (ports, TLS, ingress) | agents are **outbound dialers** — nothing to expose |
| discovery = static AgentCard URL | **dynamic discovery**: adverts + 15 s heartbeats + card request/reply |
| push notifications = client webhooks | **bus subscriptions** — no webhook plumbing |
| durability/glue is DIY | JetStream-ready (planned), broker handles queueing |

The binding implements the spec's [custom protocol bindings](https://a2a-protocol.org/latest/topics/custom-protocol-bindings/)
requirements: all core operations, ProtoJSON data model, consistent error semantics,
ordered streaming with explicit completion, documented limitations.

## Packages

| Package | Purpose |
| --- | --- |
| [`packages/a2a-nats-binding`](packages/a2a-nats-binding) | Wire contract (subjects, envelopes, ProtoJSON types), `A2ANatsAgent` (server), `A2ANatsClient` (client). |
| [`packages/a2a-nats-client`](packages/a2a-nats-client) | Vendor integration layer: `serveAgent()` + `connectA2A()` + helpers. |
| [`examples/`](examples) | Runnable two-agent exchange + captured transcript. |

## Quick start

```bash
pnpm install
pnpm -r build

# two-agent demo (discovery → streaming → handoff → follow-up → cancel)
node examples/two-openclaw-agents.mjs

# reference echo agent + demo
pnpm echo-agent   # terminal 1
pnpm demo         # terminal 2
```

Broker: `nats://nats.my:4222` by default — override with `NATS_URL`.

## Example — two OpenClaw-style sessions exchanging messages

Two agents wrapped with `serveAgent()` — a slow **librarian** (streams research
progress + artifacts, cooperatively cancellable) and a fast **summarizer** — plus a
plain orchestrator client. Verified transcript from a live run against `nats.my`:

```text
agents online:
   claw-librarian (OpenClaw session)
   claw-summarizer (OpenClaw session)

== 1. discovery ==
librarian card: claw-librarian (OpenClaw session) — skills: research

== 2. streaming send to librarian ==
  [status] TASK_STATE_WORKING
  [status] TASK_STATE_WORKING
  [artifact+new] source-1: spec section about the A2A protocol
  [status] TASK_STATE_WORKING
  [artifact+append] source-2: tutorial about the A2A protocol
  [status] TASK_STATE_COMPLETED

== 3. hand artifacts to summarizer (sync send) ==
  summarizer: TL;DR: please summarize: source-1: spec section about the A2A protocol | source-2: tutorial about the A2A protocol |

== 4. follow-up: NEW task in the SAME context (A2A task immutability) ==
  summarizer (follow-up): TL;DR: make it shorter
  new task fc99569e-… shares context 9966903b-… with 27346f31-…

== 5. fire-and-forget to librarian, then cancel ==
  librarian task state after cancel: TASK_STATE_WORKING

done.
```

What each step exercises: (1) card request/reply discovery, (2) `message/stream` —
initial task snapshot, WORKING updates, artifact chunks with `append`/`lastChunk`,
terminal close, (3) `message/send` resolving at terminal state with the final text in
`status.message`, (4) task immutability — refinements are new tasks sharing a
`contextId`, (5) `returnImmediately` + cooperative `tasks/cancel`.

The orchestrator code that produced it (`examples/two-openclaw-agents.mjs`):

```ts
// Agent side — wrap any handler:
const librarian = await serveAgent(
  { agentId: "claw-librarian", name: "claw-librarian (OpenClaw session)", skills: [...] },
  async (ctx) => {
    await ctx.working("planning research…");
    await ctx.artifact("sources", [{ text: "source-1: …" }], { lastChunk: false });
    await ctx.artifact("sources", [{ text: "source-2: …" }], { append: true, lastChunk: true });
    await ctx.complete(`research on "${topic}" finished`);
  },
);

// Orchestrator side — discovery, streaming, handoff:
const card = await client.getAgentCard("claw-librarian");
for await (const ev of client.sendMessageStream("claw-librarian", "the A2A protocol")) {
  if (ev.kind === "artifact_update") console.log("artifact:", ev.artifactUpdate?.artifact);
}
await client.sendMessage("claw-summarizer", "please summarize: …");
await client.cancelTask("claw-librarian", longTask.id);   // cooperative
```

Full transcript with commentary: [`examples/TRANSCRIPT.md`](examples/TRANSCRIPT.md).

## Integrate your agent (the "least effort" path)

```ts
import { serveAgent, connectA2A, textOf } from "a2a-nats-client";

// Expose your agent:
const served = await serveAgent(
  {
    agentId: "my-agent",
    name: "My Agent",
    description: "does the thing",
    skills: [{ id: "thing", name: "Do the thing", description: "…", tags: ["work"] }],
  },
  async (ctx) => {
    const input = textOf(ctx.message.parts);
    await ctx.working("thinking…");
    await ctx.complete(`result for ${input}`);        // → status.message
    // or: await ctx.artifact(id, parts, { lastChunk: true })  // → artifacts
  },
);

// Call other agents:
const client = await connectA2A();
const { task } = await client.sendMessage("other-agent", "hi");
const card  = await client.getAgentCard("other-agent");
const peers = await client.listLiveAgents();       // scan advertise broadcasts
await client.close();                              // (also stops nothing on the server side)
await served.stop();
```

### `TaskContext` API (handler side)

| Method | Effect |
| --- | --- |
| `ctx.working(text?)` | publish a `TASK_STATE_WORKING` status update |
| `ctx.artifact(id, parts, {append?, lastChunk?})` | stream an artifact chunk (accumulated into the task) |
| `ctx.complete(message?, artifacts?)` | terminate as `COMPLETED` |
| `ctx.fail(message)` | terminate as `FAILED` |
| `ctx.cancelRequested()` | cooperative-cancel checkpoint for long handlers |
| `ctx.task` / `ctx.message` | read-only task state and the user's message |

A handler may simply **return** `Part[]` — the agent wraps it as an artifact and completes.

## Wire contract (summary)

```
a2a.v1.agent.<id>                     request/reply entrypoint (op in X-A2A-Op header)
a2a.v1.agent.<id>.events              task lifecycle broadcast
a2a.v1.agent.<id>.task.<tid>.ctl      cooperative cancel control
a2a.v1.discovery.advertise            AgentCard adverts (15 s heartbeat)
a2a.v1.discovery.card.<id>            card request/reply
_a2a.stream.<correlation-id>          streaming reply inbox (message/stream)
```

Body = one `PubMsg` (request/response) or `PubEvent` (stream) envelope per NATS message;
`payload` holds the A2A object verbatim (ProtoJSON); binding metadata (op, correlation id,
task routing, publisher) travels in NATS headers (`X-A2A-Op`, `X-A2A-Correlation-Id`,
`X-A2A-Task-Id`, `X-A2A-Binding`, …). Ops: `message/send`, `message/stream`,
`tasks/get`, `tasks/list`, `tasks/cancel`, `agent/getCard`.

Error mapping: agents reply with `{ code, message, task? }` inside the envelope
(`TASK_NOT_FOUND`, `TASK_NOT_CANCELABLE`, `UNSUPPORTED_OPERATION`,
`CONTENT_TYPE_NOT_SUPPORTED`, `INVALID_REQUEST`, `INTERNAL_ERROR`); clients surface them
as `A2AOperationError`. No-responder / request timeouts surface as `A2ATimeoutError`.

## Tests

```bash
pnpm test                                        # unit (no broker needed)
A2A_E2E=1 pnpm --filter a2a-nats-binding test    # e2e against a live broker
bash scripts/run-e2e.sh                          # same thing
```

Current status: **8/8 unit + 9/9 e2e green** against a live NATS 2.15 broker
(discovery, send, stream, tasks/get, tasks/list, cancel semantics, unknown-agent
timeout, terminal-state guard, handler-return artifact wrapping).

## Known limitations (v0.1)

- In-memory task store per agent process — JetStream durability is the next milestone.
- `tasks/list` pagination is a stub (no page-token walk yet).
- No wire auth yet (NKeys/JWT planned; assume broker-side auth for now).
- Subject prefix fixed to `a2a.v1`.
- Cancel is cooperative — observed between handler awaits, not pre-emptive.
- Discovery scan (`listLiveAgents`) listens for a time window rather than reading a
  KV index; JetStream KV mirror planned.

## Layout

```
packages/a2a-nats-binding/src/
  contract.ts   subjects, headers, envelopes, op names, state helpers
  types.ts      ProtoJSON-compatible A2A v1.0 types
  agent.ts      A2ANatsAgent — server side
  client.ts     A2ANatsClient — client side
  errors.ts     A2AOperationError / A2ATimeoutError / A2AProtocolError
  test/         unit + live e2e
packages/a2a-nats-client/src/
  index.ts      serveAgent / connectA2A / textOf
  demo.ts       echo-agent end-to-end demo
  agents/echo-agent.ts  reference agent
examples/
  two-openclaw-agents.mjs  two-agent exchange (this README's transcript)
  TRANSCRIPT.md            captured run + commentary
```

## License

MIT — see [LICENSE](./LICENSE).
