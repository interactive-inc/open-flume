# open-flume

Unified notification listener for Discord, Slack, and GitHub. Raw WebSocket + `fetch` + Zod. Zero SDK dependencies — no `discord.js`, no `@slack/bolt`, no `@slack/web-api`. ESM only (`require()` not supported). Runs on Node 22+, Bun, Deno, Cloudflare Workers, or any environment with global `fetch` and `WebSocket` (the GitHub source only needs `fetch`).

```
Discord  ─┐
Slack    ─┼──▶  Flume  ──start(handler)──▶  FlumeEvent  (one merged stream)
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

const onLog = (log) => console.log(`[${log.level}] ${log.source}/${log.action}: ${log.message}`)

const flume = new Flume({
  sources: [
    new FlumeDiscordSource({ token: process.env.DISCORD_BOT_TOKEN!, onLog, reconnect: true }),
    new FlumeSlackSource({
      appToken: process.env.SLACK_APP_TOKEN!,
      botToken: process.env.SLACK_BOT_TOKEN!,
      onLog,
      reconnect: true,
    }),
    new FlumeGitHubSource({ token: process.env.GITHUB_TOKEN!, onLog, pollInterval: 60 }),
  ],
})

const running = await flume.start((event) => {
  console.log(event.source, event.type, event.meta)
})

if (running instanceof Error) throw running

// later
await running.stop()
```

## Lifecycle (type-state FSM)

`Flume` enforces lifecycle correctness through three classes — misuse becomes a compile error.

```
Flume  ──start()──▶  FlumeRunning  ──stop()──▶  FlumeStopped
(idle)               (running)                  (terminal)
```

- `Flume.start(handler)` returns `FlumeRunning | FlumeStartError`. Branch with `instanceof Error`. On partial failure (one source fails while another succeeds), the already-started sources are rolled back and a `FlumeStartError` is returned with per-source detail in `.message`. Calling `start()` a second time on the same `Flume` instance returns `FlumeStartError` at runtime — the type system also rejects calling `start()` on the returned `FlumeRunning`/`FlumeStopped` handles.
- `FlumeRunning.stop()` returns a `FlumeStopped` snapshot. `stop()` is idempotent and concurrent-safe.
- `FlumeStopped` exposes only `statuses()` — a frozen snapshot of each source's final state. No `start`, no `stop`, no leaking source references.
- An `AbortSignal` on `Flume` drives an automatic transition to `FlumeStopped`.
- `FlumeRunning.kind === "running"` and `FlumeStopped.kind === "stopped"` provide a runtime discriminator when generic code holds the union.

```ts
const running = await flume.start(handler)
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

## Direct source usage

Sources work standalone — `Flume` is only needed for multi-source orchestration.

```ts
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"

const source = new FlumeDiscordSource({
  token: process.env.DISCORD_BOT_TOKEN!,
  reconnect: true,
  onLog: (log) => console.log(log),
})

const error = await source.start((event) => {
  /* ... */
})
if (error instanceof Error) throw error
```

## Sub-entries

Each source has a dedicated entry — importing one does not pull the others into your bundle. The root entry never loads source-specific code.

| sub-entry                        | exports                                                                          |
| -------------------------------- | -------------------------------------------------------------------------------- |
| `@interactive-inc/flume`         | `Flume`, `FlumeRunning`, `FlumeStopped`, `createFlumeDefaultDeps`, errors, types |
| `@interactive-inc/flume/discord` | `FlumeDiscordSource`, `FlumeDiscordGatewayIntents`, `flumeExtractDiscordMeta`    |
| `@interactive-inc/flume/slack`   | `FlumeSlackSource`, `flumeExtractSlackMeta`                                      |
| `@interactive-inc/flume/github`  | `FlumeGitHubSource`, `flumeExtractGitHubMeta`                                    |

```ts
import { Flume } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"
import { FlumeGitHubSource } from "@interactive-inc/flume/github"
```

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

| source  | meta keys                                                              |
| ------- | ---------------------------------------------------------------------- |
| discord | `event_type`, `channel_id`, `guild_id`, `user_id`                      |
| slack   | `event_type`, `channel_id`, `user_id`, `thread_ts`, `slack_event_type` |
| github  | `event_type`, `reason`, `subject_type`, `repository`, `thread_id`      |

`data` is the raw parsed payload (Zod-validated at the protocol boundary).

## Observability

Flume never calls a third-party service. Every internal action is reported through the `onLog` callback you pass to `Flume` or each source — there are no silent paths.

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

- **HTTP boundary** — every request URL, response status, and parsed body shape (`http.request` / `http.response` / `http.body`). Slack's `apps.connections.open` call and GitHub's poll request both emit these.
- **WebSocket boundary** — every inbound frame (`ws.recv`), every outbound frame (`ws.send` / `ws.sent`), with a 200-byte preview and total byte count. Discord op / t / s is decoded into structured `detail`.
- **Protocol lifecycle** — Discord HELLO / READY / RESUMED / RECONNECT / INVALID_SESSION / HEARTBEAT / HEARTBEAT_ACK, Slack hello / disconnect / envelope ack, GitHub bootstrap / fresh / idle.
- **Parse failures** — any Zod schema mismatch emits a `warn` with the field paths and messages. Dropped GitHub notifications carry a per-item `parse.skip` and a `parse.summary` count. Slack envelopes that don't match `FlumeSlackEnvelopeSchema` emit `envelope.parse-fail` with the incoming `type` and the issues. Discord frames with an unknown op emit `ws.unknown-op`.
- **Reconnect** — `reconnect.scheduled` (with delay in ms), `reconnect.exhausted` (with attempt count), `reconnect.reset` (on successful connect after retries), `reconnect.cancel` (on stop).
- **Status transitions** — `status` action with `previous → next`.
- **Errors** — `level: "error"` carries the `error` field so you can `captureException` in your handler.

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

`reconnect` accepts `true`, an options object, or is omitted (no reconnect).

```ts
reconnect: {
  maxAttempts: 10,
  baseDelay: 1000,   // first backoff
  maxDelay: 30000,   // backoff cap
}
```

Exponential backoff with jitter. Discord resumes the session when possible — the session id and resume URL are carried across reconnects via `FlumeDiscordGatewaySession`.

## Status

```ts
onStatus: (status: "disconnected" | "connecting" | "connected" | "reconnecting", detail?: string) => void
```

GitHub populates `detail` with the failure reason (e.g. `"HTTP 500"`, `"network error"`). Discord and Slack leave `detail` undefined.

## Cancellation

Pass an `AbortSignal` to `Flume` (propagates to every source) or to an individual source.

````ts
const controller = new AbortController()

const flume = new Flume({
  sources: [new FlumeDiscordSource({ token, signal: controller.signal })],
  signal: controller.signal,
})

```ts
const running = await flume.start(handler)
if (running instanceof Error) throw running

controller.abort() // FlumeRunning auto-transitions to FlumeStopped
````

If the signal is already aborted at `Flume.start()` time, `start` returns a `FlumeStartError` and no source is touched.

## Dependency injection

Every IO boundary (`fetch`, `WebSocket`, `now`, `random`, timers) lives in `FlumeRuntimeDeps`. `deps` is optional on every source — when omitted, `createFlumeDefaultDeps()` wraps the global equivalents. Override only when you need mocks or runtime-specific shims.

```ts
import { createFlumeDefaultDeps } from "@interactive-inc/flume"

new FlumeDiscordSource({
  token,
  deps: {
    ...createFlumeDefaultDeps(),
    fetch: mockFetch,
    now: () => 1_000,
  },
})
```

## Safety

- **Ordering** — each source has its own `FlumeSerialQueue` and per-source events are delivered FIFO. Cross-source ordering between events from different sources is undefined (no global serialization). Handler invocations are awaited and run one at a time per source, so async handlers don't race and `stop()` drains in-flight events before transitioning state.
- **Duplicate suppression** — Slack envelopes are deduped by `envelope_id` (`FlumeSlackSeenCache`) to absorb ack retries. GitHub notifications are deduped by `id + updated_at` (`FlumeGitHubSeenCache`). Discord uses session resume so the Gateway does not re-emit dispatches.
- **Partial-failure rollback** — if any source fails during `Flume.start()`, the already-started sources are stopped and a `FlumeStartError` is returned with per-source detail.
- **Idempotent stop** — `FlumeRunning.stop()` is safe to call concurrently; the first call wins and subsequent callers receive the same `FlumeStopped` snapshot.

## Errors

Flume does not throw on protocol/network failures. Every entry point returns `T | Error` — branch with `instanceof Error`. `Flume.start()` returns `FlumeRunning | FlumeStartError`; `Source.start()` returns `Error | null`; protocol-layer helpers (`FlumeDiscordGateway.connect()`, `obtainSlackUrl()`, …) return `T | Error`:

- `FlumeStartError` — `Flume.start()` / `Source.start()` refused or failed (already started, signal aborted, partial-failure rollback)
- `FlumeConnectionError` — WebSocket closed before ready
- `FlumeHttpError` — HTTP call returned an error payload (e.g. Slack `ok: false`)
- `FlumeParseError` — Unparseable WebSocket frame

Internal handler exceptions are caught and logged (never rethrown into the protocol loop).

The library guarantees that no exception escapes any public surface — constructors, `start()`, `stop()`, the handler invocation path, the abort-signal path, and the `onLog` / `onStatus` callbacks all route IO and user-supplied callbacks through internal `safe*` wrappers (`safeNow`, `safeRandom`, `safeReadText`, `safeNewWebSocket`, `safeWsSend`, `safeWsClose`, `safeInvokeCallback`, `safeAddAbortListener`, `safeRemoveAbortListener`, `safeSourceStatus`). A misbehaving `onStatus` / `onLog` / `handler` will be logged and isolated rather than crashing the protocol loop.

## Supported sources

| source  | transport                     | auth                                  |
| ------- | ----------------------------- | ------------------------------------- |
| Discord | Gateway WebSocket v10 (JSON)  | bot token                             |
| Slack   | Socket Mode WebSocket         | app token + bot token (both required) |
| GitHub  | REST polling `/notifications` | personal access token                 |

GitHub also exposes `gh auth token` if you want to reuse the `gh` CLI's session:

```ts
import { execSync } from "node:child_process"
const token = execSync("gh auth token").toString().trim()
const github = new FlumeGitHubSource({ token })
```

## Module layout

- `Flume` / `FlumeRunning` / `FlumeStopped` — type-state FSM merging multiple sources into one stream
- `FlumeDiscordSource` / `FlumeSlackSource` / `FlumeGitHubSource` — high-level sources (each conforms to the structural `FlumeSource` type)
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
