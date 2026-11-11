# a2a-overnats

Agent2Agent (A2A) protocol **v1.0** running over **NATS** as a custom protocol binding —
so vendor agents get discovery, task lifecycle, streaming, and cancellation without
exposing a single HTTP port.

## Why

A2A's standard bindings (JSON-RPC/gRPC/HTTP+JSON) assume every agent is an HTTP server.
Inside a self-hosted agent mesh you already run a message bus. This repo implements A2A
*over the bus*:

- agents are outbound dialers only — nothing to expose, no TLS cert per agent,
- discovery = AgentCard adverts + heartbeats on well-known subjects,
- task events stream over NATS reply subjects,
- A2A objects on the wire are ProtoJSON (`lf.a2a.v1`), identical to the standard bindings,
  so a thin HTTP shim can interop with the rest of the ecosystem.

Binding URI: `https://github.com/a2a-overnats/bindings/nats/v1`
(declared in `AgentCard.supportedInterfaces[].protocolBinding`, per the A2A
custom-protocol-bindings mechanism).

## Packages

| Package | Purpose |
| --- | --- |
| `packages/a2a-nats-binding` | The wire contract: subjects, envelopes, ProtoJSON types, `A2ANatsAgent` (server) + `A2ANatsClient` (client). |
| `packages/a2a-nats-client` | Vendor integration layer: `serveAgent()` + `connectA2A()` — add A2A to any Node/TS agent in a few lines. |

## Quick start

```bash
pnpm install
pnpm -r build

# terminal 1 — reference agent
pnpm echo-agent

# terminal 2 — discovery + send + stream demo
pnpm demo
```

Default broker: `nats://nats.my:4222` (override with `NATS_URL`).

### Integrate an existing agent (the "least effort" path)

```ts
import { serveAgent, connectA2A, textOf } from "a2a-nats-client";

// Expose your agent:
const served = await serveAgent(
  {
    agentId: "my-agent",
    name: "My Agent",
    description: "does the thing",
    skills: [{ id: "thing", name: "Do the thing", description: "...", tags: ["work"] }],
  },
  async (ctx) => {
    const input = textOf(ctx.message.parts);
    await ctx.working("thinking…");
    await ctx.complete(`result for ${input}`);
  },
);

// Call other agents:
const client = await connectA2A();
const { task } = await client.sendMessage("other-agent", "hi");
for await (const ev of client.sendMessageStream("other-agent", "stream it")) {
  // ev.kind: task | message | status_update | artifact_update
}
```

## Wire contract (summary)

```
a2a.v1.agent.<id>                     request/reply entrypoint (op in X-A2A-Op header)
a2a.v1.agent.<id>.events              task lifecycle broadcast
a2a.v1.agent.<id>.task.<tid>.ctl      cooperative cancel control
a2a.v1.discovery.advertise            AgentCard adverts (15s heartbeat)
a2a.v1.discovery.card.<id>            card request/reply
_a2a.stream.<correlation-id>          streaming reply inbox (message/stream)
```

Body = `PubMsg`/`PubEvent` envelope (see `packages/a2a-nats-binding/src/contract.ts`);
`payload` holds the A2A object verbatim in ProtoJSON. Ops: `message/send`,
`message/stream`, `tasks/get`, `tasks/list`, `tasks/cancel`, `agent/getCard`.

Task states are the A2A `TASK_STATE_*` enum; terminal states are
COMPLETED / FAILED / CANCELED / REJECTED. `complete(message)` lands in
`status.message` (spec shape); streamed files land in `artifacts` via
`TaskArtifactUpdateEvent` (`append`/`lastChunk` honoured).

## Tests

```bash
pnpm test                        # unit (no broker needed)
A2A_E2E=1 pnpm --filter a2a-nats-binding test   # e2e against a live broker
bash scripts/run-e2e.sh          # same thing
```

9/9 e2e green against a live NATS 2.15 broker (discovery, send, stream,
tasks/get, tasks/list, cancel semantics, unknown-agent timeout, terminal-state
guard, handler-return artifact wrapping).

## Known limitations (v0.1)

- In-memory task store per agent process (JetStream durability is planned next).
- `tasks/list` pagination is a stub (`nextPageToken` always slicing from 0).
- No auth on the wire yet (NKeys/JWT planned; broker-side auth assumed).
- Subject prefix is fixed (`a2a.v1`).
- Cancel is cooperative — checked between handler awaits, not pre-emptive.

## Layout

```
packages/a2a-nats-binding/src/
  contract.ts   subjects, headers, envelopes, op names, state helpers
  types.ts      ProtoJSON-compatible A2A v1 types
  agent.ts      A2ANatsAgent — server side
  client.ts     A2ANatsClient — client side
  errors.ts     A2AOperationError / A2ATimeoutError / A2AProtocolError
  test/         unit + live e2e
packages/a2a-nats-client/src/
  index.ts      serveAgent / connectA2A / textOf
  demo.ts       end-to-end demo (spawns echo agent)
  agents/echo-agent.ts  reference agent
```

MIT — see [LICENSE](./LICENSE).
