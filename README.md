# open-flume

Unified notification listener for Discord, Slack, and GitHub. Raw WebSocket + `fetch` + Zod. Zero SDK dependencies — no `discord.js`, no `@slack/bolt`, no `@slack/web-api`. ESM only (`require()` not supported). Runs on Node 22+, Bun, Deno, Cloudflare Workers, or any environment with global `fetch` and `WebSocket` (the GitHub source only needs `fetch`).

```
Discord  ─┐
Slack    ─┼──▶  Flume  ──open()──▶  FlumeEvent  (one merged stream)
GitHub   ─┘
```

Flume only **receives**. It opens the WebSocket / polls the API, parses the payload with Zod, serializes events through a per-source queue, and hands you a typed event. Sending replies is out of scope — bring your own HTTP call.

## Install

```bash
npm add @interactive-inc/flume
```

## Quick start

```ts
import { Flume } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"
import { FlumeGitHubSource } from "@interactive-inc/flume/github"

const flume = new Flume({
  sources: [
    new FlumeDiscordSource({ token: process.env.DISCORD_BOT_TOKEN! }),
    new FlumeSlackSource({
      appToken: process.env.SLACK_APP_TOKEN!,
      botToken: process.env.SLACK_BOT_TOKEN!,
    }),
    new FlumeGitHubSource({ token: process.env.GITHUB_TOKEN!, pollInterval: 60 }),
  ],
  onEvent: (item) => {
    // single firehose: events + every log, discriminated by `kind`
    if (item.kind === "event") console.log(item.event.source, item.event.type)
    if (item.kind === "log") console.log(`[${item.log.level}] ${item.log.action}`)
  },
  onError: (log) => Sentry.captureException(log.error ?? new Error(log.message)),
  reconnect: { maxAttempts: 10 },
})

const running = await flume.open()

if (running instanceof Error) throw running

// later
await running.close()
```

`new Flume({ sources, ...options })` — a single options object; `sources` is the only required field, everything else is optional. There is one unified firehose: `onEvent` (push) and `FlumeRunning.stream()` (pull) both deliver the same `FlumeStreamItem` — received events **and** every log (status transitions, errors, debug) merged into one stream. The consumer filters by `item.kind` (`"event"` / `"log"`) and `item.log.level`. This is built for piping the whole picture into an agent (Claude / Codex) so it notices disconnects on its own. `onError` is a convenience filter that additionally receives only the `level: "error"` logs (route it straight to Sentry).

```ts
// Minimum: just open the protocols and discard everything.
const flume = new Flume({ sources: [new FlumeDiscordSource({ token })] })

// Errors-only: forward failures to Sentry, ignore the rest.
const flume = new Flume({ sources: [new FlumeDiscordSource({ token })], onError })
```

## Lifecycle (type-state FSM)

`Flume` enforces lifecycle correctness through three classes — misuse becomes a compile error.

```
Flume  ──open()──▶  FlumeRunning  ──close()──▶  FlumeClosed
(idle)               (running)                  (terminal)
```

- `Flume.open()` returns `FlumeRunning | FlumeStartError`. Branch with `instanceof Error`. On partial failure (one source fails while another succeeds), the already-opened sources are rolled back and a `FlumeStartError` is returned with per-source detail in `.message`. Calling `open()` a second time on the same `Flume` instance returns `FlumeStartError` at runtime — the type system also rejects calling `open()` on the returned `FlumeRunning`/`FlumeClosed` handles.
- `FlumeRunning.close()` returns a `FlumeClosed` snapshot. `close()` is idempotent and concurrent-safe.
- `FlumeClosed` exposes only `statuses()` — a frozen snapshot of each source's final state. No `open`, no `close`, no leaking source references.
- An `AbortSignal` on `Flume` drives an automatic transition to `FlumeClosed`.
- `FlumeRunning.kind === "running"` and `FlumeClosed.kind === "closed"` provide a runtime discriminator when generic code holds the union.

```ts
const running = await flume.open()
if (running instanceof Error) {
  console.error(running.message)
  // "Flume.open: 1 source(s) failed: slack: connect refused"
  return
}

running.open() // type error — `open` is not on FlumeRunning

const closed = await running.close()
closed.close() // type error
closed.open() // type error
closed.statuses() // [{ source: "discord", status: "disconnected" }, ...]
```

## Dynamic groups

A `Flume` is single-use: its source set is fixed at construction and the FSM is terminal once closed. To add or drop sources at runtime, compose at a higher level with `FlumeConfluence` — it holds many `Flume` instances, merges all their firehoses into one `onEvent`, and lets you `add` / `remove` groups while the others keep running.

```ts
import { FlumeConfluence } from "@interactive-inc/flume"

const confluence = new FlumeConfluence({
  onEvent: (item) => {
    if (item.kind === "event") feedToAgent(item.event)
    if (item.kind === "log" && item.log.action === "status") noticeDisconnect(item.log)
  },
  reconnect: { maxAttempts: 10 },
})

await confluence.add("team-a", [new FlumeDiscordSource({ token: tokenA })])
await confluence.add("team-b", [new FlumeSlackSource({ appToken, botToken })]) // existing groups keep running
await confluence.remove("team-a") // stops only team-a
await confluence.closeAll()
```

`add(id, sources)` starts a fresh `Flume` for that group and returns `Error | null` (a duplicate id or a failed start is returned, never thrown). The `id` is just a management handle — the merged stream itself is untagged; identify the origin via the `source` field inside each item. Each group is an independent `Flume`, so a failure in one group never rolls back another. Reconstructing a group (rather than mutating one in place) loses only its reconnect counters, dedup caches, and Discord session resume — acceptable on a deliberate add/remove.

## Sub-entries

Each source has a dedicated entry — importing one does not pull the others into your bundle. The root entry never loads source-specific code.

```ts
import { Flume, FlumeSource } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"
import { FlumeGitHubSource } from "@interactive-inc/flume/github"
import { FlumeTimeSource } from "@interactive-inc/flume/time"
```

- `@interactive-inc/flume` — `Flume`, `FlumeConfluence`, `FlumeRunning`, `FlumeClosed`, `FlumeSource` (abstract base for third-party sources), `createFlumeDefaultDeps`, errors, types
- `@interactive-inc/flume/discord` — `FlumeDiscordSource`, `FlumeDiscordGatewayIntents`, `flumeExtractDiscordMeta`
- `@interactive-inc/flume/slack` — `FlumeSlackSource`, `flumeExtractSlackMeta`
- `@interactive-inc/flume/github` — `FlumeGitHubSource`, `flumeExtractGitHubMeta`
- `@interactive-inc/flume/time` — `FlumeTimeSource`, `parseCron`, `flumeCronNext`

## Custom sources

Extend `FlumeSource` to plug in any protocol. The base class owns `start`/`stop`/`status`, the per-source event queue, status emission (logged on every transition), and consumed/stopped guards. You implement `connect` (open the protocol, emit events, set status) and `disconnect` (tear it down).

```ts
import { FlumeSource } from "@interactive-inc/flume"
import type { FlumeSourceStartContext } from "@interactive-inc/flume"

class MyWebhookSource extends FlumeSource {
  readonly name = "my-webhook"

  private timer: ReturnType<typeof setInterval> | null = null

  constructor(private readonly options: { url: string; pollInterval?: number }) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")
    const interval = (this.options.pollInterval ?? 30) * 1000
    this.timer = ctx.deps.setInterval(() => this.poll(ctx), interval) as ReturnType<
      typeof setInterval
    >
    this.setStatus("connected")
    return null
  }

  protected disconnect(): void {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
  }

  private async poll(ctx: FlumeSourceStartContext): Promise<void> {
    const res = await ctx.deps.fetch(this.options.url)
    const payload = await res.json()
    this.emit({
      source: this.name as "discord", // declare your own discriminant via FlumeEvent extension
      type: "webhook",
      data: payload,
      meta: { event_type: "webhook" },
      receivedAt: ctx.deps.now(),
    })
  }
}
```

`this.emit({...})` queues events through the base's serial queue and routes them to `ctx.onEvent` with `attempt()` isolation. `this.setStatus(...)` deduplicates idempotent transitions and logs the change (which is how status surfaces to consumers). Subclasses never need to write try/catch.

## Time source

`FlumeTimeSource` emits a `tick` event on a cron schedule. It holds no external connection, so it reaches `connected` as soon as it starts and is never a reconnect target. Useful as a heartbeat — a fixed marker in the same stream as your real sources.

```ts
import { FlumeTimeSource } from "@interactive-inc/flume/time"

new Flume({
  sources: [new FlumeTimeSource({ cron: "0 * * * *" })],
  onEvent: (item) => console.log(item), // fires at minute 0 of every hour
})
```

The cron expression is a standard 5-field spec (`minute hour day-of-month month day-of-week`) evaluated against the wall clock (local time). Supported per field: `*`, `*/n`, `a`, `a-b`, `a-b/n`, and comma lists. Day-of-week accepts `0-7` (`7` and `0` both mean Sunday). When both day-of-month and day-of-week are restricted, a day matches if either matches (standard cron semantics).

`message()` customizes each tick. Omitted fields fall back to the defaults (`type: "tick"`, `data: { firedAt, cron }`, `meta: { cron }`):

```ts
new FlumeTimeSource({
  cron: "*/15 * * * *",
  message: (tick) => ({
    type: "heartbeat",
    data: { at: new Date(tick.firedAt).toISOString() },
    meta: { channel: "ops" },
  }),
})
```

`tick.firedAt` is the scheduled wall-clock time (epoch ms), not the exact `setTimeout` firing instant. A throwing `message()` is isolated and falls back to the defaults. Parse the cron up front with `parseCron(expr)` (returns `FlumeCron | FlumeParseError`) or compute the next fire with `flumeCronNext(cron, afterMs)`.

## Pull stream

`FlumeRunning.stream()` is the pull form of the same firehose as `onEvent` — an async iterator over `FlumeStreamItem`, handy for feeding an agent (Claude / Codex) with `for await`, where backpressure falls out naturally from how fast you pull.

```ts
type FlumeStreamItem = { kind: "event"; event: FlumeEvent } | { kind: "log"; log: FlumeLog }

const running = await flume.open()
if (running instanceof Error) throw running

for await (const item of running.stream()) {
  if (item.kind === "event") await handleWithAgent(item.event)
  if (item.kind === "log" && item.log.action === "status") noticeDisconnect(item.log)
}
// loop ends when the flume stops (running.close() or signal abort)
```

The iterator ends cleanly when the flume stops; `break`ing out unsubscribes automatically. When a slow consumer lets the buffer overflow, the oldest items are dropped by default:

```ts
running.stream({ buffer: 5000, onOverflow: "drop-newest" })
```

`stream()` (pull) and `onEvent` (push) carry the same items — use either or both. Multiple `stream()` consumers each get their own buffer.

## Event shape

Every source emits the same `FlumeEvent` — a discriminated union keyed on `source` so `data` narrows automatically:

```ts
type FlumeDiscordEvent = {
  source: "discord"
  type: string
  data: Record<string, unknown>
  meta: Record<string, string>
  receivedAt: number
}
type FlumeSlackEvent = {
  source: "slack"
  type: string
  data: Record<string, unknown>
  meta: Record<string, string>
  receivedAt: number
}
type FlumeGitHubEvent = {
  source: "github"
  type: "notification"
  data: FlumeGitHubNotification
  meta: Record<string, string>
  receivedAt: number
}
type FlumeTimeEvent = {
  source: "time"
  type: string // "tick" by default, or whatever your message() returns
  data: Record<string, unknown>
  meta: Record<string, string>
  receivedAt: number
}

type FlumeEvent = FlumeDiscordEvent | FlumeSlackEvent | FlumeGitHubEvent | FlumeTimeEvent
```

`meta` is flat string keys tailored per source:

- discord — `event_type`, `channel_id`, `guild_id`, `user_id`
- slack — `event_type`, `channel_id`, `user_id`, `thread_ts`, `slack_event_type`
- github — `event_type`, `reason`, `subject_type`, `repository`, `thread_id`
- time — `cron` by default, or whatever your `message()` returns

`data` is the raw parsed payload (Zod-validated at the protocol boundary).

## Observability

Flume never calls a third-party service. Every internal action is reported through the firehose (`onEvent` push / `stream()` pull) as `{ kind: "log", log }` items — there are no silent paths. Each `FlumeLog` is tagged with `source: "flume"`, `source: "discord"`, `source: "slack"`, `source: "github"`, `source: "time"`, or your custom source's `name`. `onError` is a convenience filter that additionally receives only the `level: "error"` subset (route it straight to Sentry).

```ts
type FlumeLog = {
  level: "debug" | "info" | "warn" | "error"
  source: string
  action: string
  message: string
  error?: Error
  detail?: Record<string, unknown>
  timestamp: number
}
```

What gets logged:

- HTTP boundary — every request URL, response status, and parsed body shape (`http.request` / `http.response` / `http.body`). Slack's `apps.connections.open` call and GitHub's poll request both emit these.
- WebSocket boundary — every inbound frame (`ws.recv`), every outbound frame (`ws.send` / `ws.sent`), with a 200-byte preview and total byte count. Discord op / t / s is decoded into structured `detail`.
- Protocol lifecycle — Discord HELLO / READY / RESUMED / RECONNECT / INVALID_SESSION / HEARTBEAT / HEARTBEAT_ACK, Slack hello / disconnect / envelope ack, GitHub bootstrap / fresh / idle.
- Parse failures — any Zod schema mismatch emits a `warn` with the field paths and messages. Dropped GitHub notifications carry a per-item `parse.skip` and a `parse.summary` count. Slack envelopes that don't match `FlumeSlackEnvelopeSchema` emit `envelope.parse-fail` with the incoming `type` and the issues. Discord frames with an unknown op emit `ws.unknown-op`.
- Reconnect — `reconnect.scheduled` (with delay in ms), `reconnect.exhausted` (with attempt count), `reconnect.reset` (on successful connect after retries), `reconnect.cancel` (on stop).
- Status transitions — `status` action with `previous → next`.
- Errors — `level: "error"` carries the `error` field; use `onError` for a pre-filtered error sink.

Route it anywhere — Sentry, Datadog, `console`, a file — your choice. Filter the log items out of the firehose:

```ts
onEvent: (item) => {
  if (item.kind !== "log") return
  const log = item.log
  if (log.level === "error" && log.error) {
    Sentry.captureException(log.error, { tags: { source: log.source, action: log.action } })
  }
  if (log.level === "debug" && !process.env.FLUME_DEBUG) return
  console.log(`[${log.level}] ${log.source}/${log.action}: ${log.message}`)
}
```

## Reconnect

`reconnect` on `Flume` is an options object — omit it to disable reconnects entirely.

```ts
new Flume(sources, {
  onEvent: (item) => { ... },
  reconnect: {
    maxAttempts: 10,
    baseDelay: 1000,   // first backoff
    maxDelay: 30000,   // backoff cap
  },
})
```

Exponential backoff with jitter. Discord resumes the session when possible — the session id and resume URL are carried across reconnects via `FlumeDiscordGatewaySession`. GitHub polling doesn't need reconnect (it's stateless polling); the option is wired through but ignored.

## Status

There is no dedicated status callback. Connection-state transitions ride the firehose as `status` log entries (`action: "status"`, message `previous → next`, with `detail.from` / `detail.to` / `detail.reason`). This is deliberate: a consumer feeding the firehose to an agent gets work (events) and drops (status logs) in one stream, no third callback.

```ts
onEvent: (item) => {
  if (item.kind === "log" && item.log.action === "status") {
    console.log(`${item.log.source}: ${item.log.message}`) // "github: connected → reconnecting (HTTP 500)"
  }
}
```

The live status of each source is also readable as a snapshot via `running.statuses()` and `closed.statuses()` (`{ source, status }[]`). GitHub populates the transition `reason` with the failure cause (e.g. `"HTTP 500"`, `"network error"`); Discord and Slack leave it null.

## Cancellation

Pass an `AbortSignal` to `Flume` — it propagates to every source via the auto-stop pathway.

```ts
const controller = new AbortController()

const flume = new Flume({
  sources: [new FlumeDiscordSource({ token })],
  onEvent: (item) => { ... },
  signal: controller.signal,
})

const running = await flume.open()

if (running instanceof Error) throw running

controller.abort() // FlumeRunning auto-transitions to FlumeClosed
```

If the signal is already aborted at `Flume.open()` time, `open` returns a `FlumeStartError` and no source is touched.

## Dependency injection

Every IO boundary (`fetch`, `WebSocket`, `now`, `random`, timers) lives in `FlumeRuntimeDeps`. `deps` is optional on `Flume` — when omitted, `createFlumeDefaultDeps()` wraps the global equivalents. Override only when you need mocks or runtime-specific shims. The same `deps` is handed to every source through its start context.

```ts
import { createFlumeDefaultDeps } from "@interactive-inc/flume"

new Flume({
  sources: [new FlumeDiscordSource({ token })],
  onEvent: (item) => { ... },
  deps: {
    ...createFlumeDefaultDeps(),
    fetch: mockFetch,
    now: () => 1_000,
  },
})
```

## Safety

- Ordering — each source has its own `FlumeSerialQueue` and per-source events are delivered FIFO. Cross-source ordering between events from different sources is undefined (no global serialization). `onEvent` invocations are awaited and run one at a time per source, so async callbacks don't race and `close()` drains in-flight events before transitioning state.
- Duplicate suppression — Slack envelopes are deduped by `envelope_id` (`FlumeSlackSeenCache`) to absorb ack retries. GitHub notifications are deduped by `id + updated_at` (`FlumeGitHubSeenCache`). Discord uses session resume so the Gateway does not re-emit dispatches.
- Partial-failure rollback — if any source fails during `Flume.open()`, the already-started sources are stopped and a `FlumeStartError` is returned with per-source detail.
- Idempotent close — `FlumeRunning.close()` is safe to call concurrently; the first call wins and subsequent callers receive the same `FlumeClosed` snapshot. The same guard exists at source level: a double-`close()` (e.g. via signal abort racing a manual close) does not re-invoke `disconnect()`.

## Errors

Flume does not throw on protocol/network failures. Every entry point returns `T | Error` — branch with `instanceof Error`. `Flume.open()` returns `FlumeRunning | FlumeStartError`; protocol-layer helpers (`FlumeDiscordGateway.connect()`, etc.) return `T | Error`:

- `FlumeStartError` — `Flume.open()` refused or failed (already started, signal aborted, partial-failure rollback)
- `FlumeConnectionError` — WebSocket closed before ready
- `FlumeHttpError` — HTTP call returned an error payload (e.g. Slack `ok: false`)
- `FlumeParseError` — Unparseable WebSocket frame

Exceptions thrown from `onEvent` are caught and logged (never rethrown into the protocol loop).

The library guarantees that no exception escapes any public surface — constructors, `open()`, `close()`, the `onEvent` invocation path, the `stream()` iterator, the abort-signal path, and the `onError` callback all route IO and user-supplied callbacks through internal `safe*` wrappers and the generic `attempt()` helper. A misbehaving `onEvent` / `onError` will be logged and isolated rather than crashing the protocol loop.

## Supported sources

- Discord — Gateway WebSocket v10 (JSON), bot token
- Slack — Socket Mode WebSocket, app token + bot token (both required)
- GitHub — REST polling `/notifications`, personal access token
- Time — cron-scheduled ticks, no external connection

GitHub also exposes `gh auth token` if you want to reuse the `gh` CLI's session:

```ts
import { execSync } from "node:child_process"
const token = execSync("gh auth token").toString().trim()
const github = new FlumeGitHubSource({ token })
```

## Module layout

- `Flume` / `FlumeRunning` / `FlumeClosed` — type-state FSM merging multiple sources into one stream
- `FlumeSource` — abstract base class for any protocol source; extend it to plug in your own
- `FlumeDiscordSource` / `FlumeSlackSource` / `FlumeGitHubSource` / `FlumeTimeSource` — built-in sources, each extending `FlumeSource`
- `FlumeRuntimeDeps` — IO boundary port (`fetch`, `WebSocket`, `now`, `random`, timers); `WebSocket` is nullable for fetch-only runtimes
- `flumeExtractDiscordMeta` / `flumeExtractSlackMeta` / `flumeExtractGitHubMeta` — pure functions that build `FlumeEvent.meta` from each protocol's payload shape
- Internal protocol modules (`FlumeDiscordGateway`, `FlumeSlackSocketMode`, `FlumeGitHubPoller`, `FlumeReconnector`, `FlumeLogger`, seen caches, schemas) are not part of the public surface — depend on the high-level Sources only

## Development

```bash
bun install
bunx vp pack       # build dist/
bunx tsc --noEmit  # typecheck
bunx vitest run    # tests
bunx vp lint       # lint
bunx vp fmt        # format
```

The library itself is runtime-agnostic. The dev toolchain (build / test / lint) uses Bun + `vite-plus` + `vitest`, but the published `dist/` is plain ESM with TypeScript declarations and runs on Node 22+, Bun, Deno, Cloudflare Workers, or modern browsers. Runtimes without a global `WebSocket` (e.g. fetch-only edge functions) can still use `FlumeGitHubSource`; `FlumeDiscordSource` / `FlumeSlackSource` will return `FlumeStartError` at `open()` time if `deps.WebSocket` is `null`.

## License

MIT © Interactive Inc.
