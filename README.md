# open-flume

Unified notification listener for Discord, Slack, and GitHub. Raw WebSocket + `fetch` + Zod. Zero SDK dependencies — no `discord.js`, no `@slack/bolt`, no `@slack/web-api`. ESM only (`require()` not supported). Runs on Node 22+, Bun, Deno, Cloudflare Workers, or any environment with global `fetch` and `WebSocket` (the GitHub source only needs `fetch`).

```
Discord  ─┐
Slack    ─┼──▶  Flume  ──start()──▶  FlumeEvent  (one merged stream)
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

const flume = new Flume(
  [
    new FlumeDiscordSource({ token: process.env.DISCORD_BOT_TOKEN! }),
    new FlumeSlackSource({
      appToken: process.env.SLACK_APP_TOKEN!,
      botToken: process.env.SLACK_BOT_TOKEN!,
    }),
    new FlumeGitHubSource({ token: process.env.GITHUB_TOKEN!, pollInterval: 60 }),
  ],
  {
    onEvent: (event) => {
      console.log(event.source, event.type, event.meta)
    },
    onLog: (log) => console.log(`[${log.level}] ${log.source}/${log.action}: ${log.message}`),
    onStatus: (e) => console.log(`${e.source} → ${e.status}${e.detail ? ` (${e.detail})` : ""}`),
    reconnect: { maxAttempts: 10 },
  },
)

const running = await flume.start()
if (running instanceof Error) throw running

// later
await running.stop()
```

`new Flume(sources, options?)` — `sources` is the only required argument. `options` is an object and every field is optional: omit `onEvent` to drop events silently (connection-observation mode), omit `onLog` to disable logging, etc. All three callbacks share the `on*` naming for symmetry: `onEvent` is the business stream, `onLog` is the operational stream, `onStatus` is the connection-state stream.

```ts
// Minimum: just open the protocols and discard everything.
const flume = new Flume([new FlumeDiscordSource({ token })])

// Connection-observation only: see when sources reconnect, ignore the payloads.
const flume = new Flume([new FlumeDiscordSource({ token })], { onLog })
```

## Lifecycle (type-state FSM)

`Flume` enforces lifecycle correctness through three classes — misuse becomes a compile error.

```
Flume  ──start()──▶  FlumeRunning  ──stop()──▶  FlumeStopped
(idle)               (running)                  (terminal)
```

- `Flume.start()` returns `FlumeRunning | FlumeStartError`. Branch with `instanceof Error`. On partial failure (one source fails while another succeeds), the already-started sources are rolled back and a `FlumeStartError` is returned with per-source detail in `.message`. Calling `start()` a second time on the same `Flume` instance returns `FlumeStartError` at runtime — the type system also rejects calling `start()` on the returned `FlumeRunning`/`FlumeStopped` handles.
- `FlumeRunning.stop()` returns a `FlumeStopped` snapshot. `stop()` is idempotent and concurrent-safe.
- `FlumeStopped` exposes only `statuses()` — a frozen snapshot of each source's final state. No `start`, no `stop`, no leaking source references.
- An `AbortSignal` on `Flume` drives an automatic transition to `FlumeStopped`.
- `FlumeRunning.kind === "running"` and `FlumeStopped.kind === "stopped"` provide a runtime discriminator when generic code holds the union.

```ts
const running = await flume.start()
if (running instanceof Error) {
  console.error(running.message)
  // "Flume.start: 1 source(s) failed: slack: connect refused"
  return
}

running.start() // type error — `start` is not on FlumeRunning

const stopped = await running.stop()
stopped.stop() // type error
stopped.start() // type error
stopped.statuses() // [{ source: "discord", status: "disconnected" }, ...]
```

## Sub-entries

Each source has a dedicated entry — importing one does not pull the others into your bundle. The root entry never loads source-specific code.

```ts
import { Flume, FlumeSource } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"
import { FlumeGitHubSource } from "@interactive-inc/flume/github"
```

- `@interactive-inc/flume` — `Flume`, `FlumeRunning`, `FlumeStopped`, `FlumeSource` (abstract base for third-party sources), `createFlumeDefaultDeps`, errors, types
- `@interactive-inc/flume/discord` — `FlumeDiscordSource`, `FlumeDiscordGatewayIntents`, `flumeExtractDiscordMeta`
- `@interactive-inc/flume/slack` — `FlumeSlackSource`, `flumeExtractSlackMeta`
- `@interactive-inc/flume/github` — `FlumeGitHubSource`, `flumeExtractGitHubMeta`

## Custom sources

Extend `FlumeSource` to plug in any protocol. The base class owns `start`/`stop`/`status`, the per-source event queue, status emission with `onStatus` bridging, and consumed/stopped guards. You implement `connect` (open the protocol, emit events, set status) and `disconnect` (tear it down).

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

`this.emit({...})` queues events through the base's serial queue and routes them to `ctx.onEvent` with `attempt()` isolation. `this.setStatus(...)` deduplicates idempotent transitions, logs the change, and forwards it to `ctx.onStatus`. Subclasses never need to write try/catch.

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

type FlumeEvent = FlumeDiscordEvent | FlumeSlackEvent | FlumeGitHubEvent
```

`meta` is flat string keys tailored per source:

- discord — `event_type`, `channel_id`, `guild_id`, `user_id`
- slack — `event_type`, `channel_id`, `user_id`, `thread_ts`, `slack_event_type`
- github — `event_type`, `reason`, `subject_type`, `repository`, `thread_id`

`data` is the raw parsed payload (Zod-validated at the protocol boundary).

## Observability

Flume never calls a third-party service. Every internal action is reported through the `onLog` callback you pass to `Flume` — there are no silent paths. Each `FlumeLog` is tagged with `source: "flume"`, `source: "discord"`, `source: "slack"`, `source: "github"`, or your custom source's `name`.

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
- Errors — `level: "error"` carries the `error` field so you can `captureException` in your `onLog` callback.

Route it anywhere — Sentry, Datadog, `console`, a file — your choice:

```ts
onLog: (log) => {
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
  onEvent: (event) => { ... },
  reconnect: {
    maxAttempts: 10,
    baseDelay: 1000,   // first backoff
    maxDelay: 30000,   // backoff cap
  },
})
```

Exponential backoff with jitter. Discord resumes the session when possible — the session id and resume URL are carried across reconnects via `FlumeDiscordGatewaySession`. GitHub polling doesn't need reconnect (it's stateless polling); the option is wired through but ignored.

## Status

```ts
type FlumeStatusEvent = {
  source: string
  status: "disconnected" | "connecting" | "connected" | "reconnecting"
  detail?: string
}

onStatus: (event: FlumeStatusEvent) => void
```

GitHub populates `detail` with the failure reason (e.g. `"HTTP 500"`, `"network error"`). Discord and Slack leave `detail` undefined.

## Cancellation

Pass an `AbortSignal` to `Flume` — it propagates to every source via the auto-stop pathway.

```ts
const controller = new AbortController()

const flume = new Flume([new FlumeDiscordSource({ token })], {
  onEvent: (event) => { ... },
  signal: controller.signal,
})

const running = await flume.start()
if (running instanceof Error) throw running

controller.abort() // FlumeRunning auto-transitions to FlumeStopped
```

If the signal is already aborted at `Flume.start()` time, `start` returns a `FlumeStartError` and no source is touched.

## Dependency injection

Every IO boundary (`fetch`, `WebSocket`, `now`, `random`, timers) lives in `FlumeRuntimeDeps`. `deps` is optional on `Flume` — when omitted, `createFlumeDefaultDeps()` wraps the global equivalents. Override only when you need mocks or runtime-specific shims. The same `deps` is handed to every source through its start context.

```ts
import { createFlumeDefaultDeps } from "@interactive-inc/flume"

new Flume([new FlumeDiscordSource({ token })], {
  onEvent: (event) => { ... },
  deps: {
    ...createFlumeDefaultDeps(),
    fetch: mockFetch,
    now: () => 1_000,
  },
})
```

## Safety

- Ordering — each source has its own `FlumeSerialQueue` and per-source events are delivered FIFO. Cross-source ordering between events from different sources is undefined (no global serialization). `onEvent` invocations are awaited and run one at a time per source, so async callbacks don't race and `stop()` drains in-flight events before transitioning state.
- Duplicate suppression — Slack envelopes are deduped by `envelope_id` (`FlumeSlackSeenCache`) to absorb ack retries. GitHub notifications are deduped by `id + updated_at` (`FlumeGitHubSeenCache`). Discord uses session resume so the Gateway does not re-emit dispatches.
- Partial-failure rollback — if any source fails during `Flume.start()`, the already-started sources are stopped and a `FlumeStartError` is returned with per-source detail.
- Idempotent stop — `FlumeRunning.stop()` is safe to call concurrently; the first call wins and subsequent callers receive the same `FlumeStopped` snapshot. The same guard exists at source level: a double-`stop()` (e.g. via signal abort racing manual stop) does not re-invoke `disconnect()`.

## Errors

Flume does not throw on protocol/network failures. Every entry point returns `T | Error` — branch with `instanceof Error`. `Flume.start()` returns `FlumeRunning | FlumeStartError`; protocol-layer helpers (`FlumeDiscordGateway.connect()`, etc.) return `T | Error`:

- `FlumeStartError` — `Flume.start()` refused or failed (already started, signal aborted, partial-failure rollback)
- `FlumeConnectionError` — WebSocket closed before ready
- `FlumeHttpError` — HTTP call returned an error payload (e.g. Slack `ok: false`)
- `FlumeParseError` — Unparseable WebSocket frame

Exceptions thrown from `onEvent` are caught and logged (never rethrown into the protocol loop).

The library guarantees that no exception escapes any public surface — constructors, `start()`, `stop()`, the `onEvent` invocation path, the abort-signal path, and the `onLog` / `onStatus` callbacks all route IO and user-supplied callbacks through internal `safe*` wrappers and the generic `attempt()` helper. A misbehaving `onEvent` / `onLog` / `onStatus` will be logged and isolated rather than crashing the protocol loop.

## Supported sources

- Discord — Gateway WebSocket v10 (JSON), bot token
- Slack — Socket Mode WebSocket, app token + bot token (both required)
- GitHub — REST polling `/notifications`, personal access token

GitHub also exposes `gh auth token` if you want to reuse the `gh` CLI's session:

```ts
import { execSync } from "node:child_process"
const token = execSync("gh auth token").toString().trim()
const github = new FlumeGitHubSource({ token })
```

## Module layout

- `Flume` / `FlumeRunning` / `FlumeStopped` — type-state FSM merging multiple sources into one stream
- `FlumeSource` — abstract base class for any protocol source; extend it to plug in your own
- `FlumeDiscordSource` / `FlumeSlackSource` / `FlumeGitHubSource` — built-in sources, each extending `FlumeSource`
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

The library itself is runtime-agnostic. The dev toolchain (build / test / lint) uses Bun + `vite-plus` + `vitest`, but the published `dist/` is plain ESM with TypeScript declarations and runs on Node 22+, Bun, Deno, Cloudflare Workers, or modern browsers. Runtimes without a global `WebSocket` (e.g. fetch-only edge functions) can still use `FlumeGitHubSource`; `FlumeDiscordSource` / `FlumeSlackSource` will return `FlumeStartError` at `start()` time if `deps.WebSocket` is `null`.

## License

MIT © Interactive Inc.
