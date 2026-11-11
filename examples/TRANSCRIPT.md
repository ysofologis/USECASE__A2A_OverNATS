# Two-agent exchange transcript

Produced by `node examples/two-openclaw-agents.mjs` against a live NATS 2.15 broker
(`nats.my`), 2026-11-11. Two `serveAgent()`-wrapped agents ("claw-librarian",
"claw-summarizer") discover each other and trade tasks; a plain client orchestrates.

```
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
  new task fc99569e-bd7f-4b82-b4e1-25c5da4f280b shares context 9966903b-0d45-4e35-bfd0-55f4f11bc170 with 27346f31-c99e-46b0-838c-093a6bd6b2d8

== 5. fire-and-forget to librarian, then cancel ==
  librarian task state after cancel: TASK_STATE_WORKING

done.
```

What each step exercises:

1. **Discovery** — the librarian's AgentCard is fetched via `a2a.v1.discovery.card.*`
   request/reply (works with or without adverts).
2. **Streaming** — `message/stream` over the binding: initial task snapshot, WORKING
   status updates, artifact chunks (`append: false` then `append: true, lastChunk: true`),
   terminal COMPLETED event closes the stream.
3. **Sync send** — `message/send` request/reply resolving at terminal state; the final
   text travels in `status.message` (A2A spec shape).
4. **Follow-up** — A2A tasks are immutable once terminal; a refinement is a NEW task
   sharing the same `contextId` (per the "life of a task" spec topic).
5. **Cancel** — `returnImmediately: true` detaches the task; `tasks/cancel` requests
   cooperative cancellation (observed state may still be WORKING until the handler's
   next checkpoint notices the cancel flag).
