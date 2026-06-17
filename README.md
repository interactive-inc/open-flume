# open-flume

Unified notification listener for Discord, Slack, and GitHub. Raw WebSocket + `fetch` + Zod. Zero SDK dependencies — no `discord.js`, no `@slack/bolt`, no `@slack/web-api`. Runs on Node 18+, Bun, Deno, Cloudflare Workers, or any environment with global `fetch` and `WebSocket`.

```
Discord  ─┐
Slack    ─┼──→  FlumeSource.start(handler)  ──→  FlumeEvent
GitHub   ─┘
```

Flume only **receives**. It opens the WebSocket / polls the API, parses the payload with Zod, and hands you a typed event. Sending replies is out of scope — bring your own HTTP call.

## Install

```bash
npm add open-flume
```

## Quick start

```ts
import { Flume, createFlumeDefaultDeps } from "open-flume"

const flume = new Flume({
  deps: createFlumeDefaultDeps(),
  onLog: (log) => console.log(`[${log.level}] ${log.source}/${log.action}: ${log.message}`),
  onStatus: (status, detail) => console.log(`status: ${status} ${detail ?? ""}`),
  reconnect: { maxAttempts: 10, baseDelay: 1000, maxDelay: 30000 },
})

const discord = flume.discord({ token: process.env.DISCORD_BOT_TOKEN! })
const slack = flume.slack({ appToken: process.env.SLACK_APP_TOKEN! })
const github = flume.github({ token: process.env.GITHUB_TOKEN!, pollInterval: 60 })

await discord.start((event) => {
  console.log(event.source, event.type, event.meta)
})

await slack.start((event) => { /* ... */ })
await github.start((event) => { /* ... */ })
```

## Direct source usage

Skip the `Flume` container and instantiate a source directly:

```ts
import { FlumeDiscordSource, createFlumeDefaultDeps } from "open-flume"

const source = new FlumeDiscordSource({
  token: process.env.DISCORD_BOT_TOKEN!,
  deps: createFlumeDefaultDeps(),
  reconnect: true,
  onLog: (log) => console.log(log),
})

await source.start((event) => { /* ... */ })
```

## Sub-entries

Each source is also importable on its own. Use this to keep Slack's Socket Mode code out of a Discord-only bundle, or vice versa.

| sub-entry             | exports                                                           |
|-----------------------|-------------------------------------------------------------------|
| `open-flume/discord`  | `FlumeDiscordSource`                                              |
| `open-flume/slack`    | `FlumeSlackSource`                                                |
| `open-flume/github`   | `FlumeGitHubSource`                                               |

```ts
import { FlumeSlackSource } from "open-flume/slack"
import { FlumeDiscordSource } from "open-flume/discord"
import { FlumeGitHubSource } from "open-flume/github"
```

The root entry exports the `Flume` container, every source class, every protocol class (`FlumeDiscordGateway`, `FlumeSlackSocketMode`, `FlumeGitHubPoller`), `FlumeReconnector`, `FlumeLogger`, `createFlumeDefaultDeps`, every Zod schema, every error class, and all public types.

## Event shape

Every source emits the same `FlumeEvent`:

```ts
type FlumeSourceName = "discord" | "slack" | "github"

type FlumeEvent = {
  source: FlumeSourceName
  type: string
  data: unknown
  meta: Record<string, string>
  receivedAt: number
}
```

`meta` is flat string keys tailored per source:

| source  | meta keys                                                              |
|---------|------------------------------------------------------------------------|
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

Pass an `AbortSignal` to any source. Aborting prevents start and triggers `stop()`:

```ts
const controller = new AbortController()
const flume = new Flume({ signal: controller.signal, deps: createFlumeDefaultDeps() })
// ...
controller.abort() // all sources stop
```

## Dependency injection

Every IO boundary (`fetch`, `WebSocket`, `now`, `random`, timers) lives in `FlumeRuntimeDeps`. The default factory wraps the global equivalents; tests pass mocks:

```ts
import { createFlumeDefaultDeps } from "open-flume"

const deps = {
  ...createFlumeDefaultDeps(),
  fetch: mockFetch,
  WebSocket: MockWebSocket,
  now: () => 1_000,
  random: () => 0.5,
}
```

`Flume` accepts `Partial<FlumeRuntimeDeps>` and merges over the defaults — override only what you need.

## Errors

Flume does not throw on protocol/network failures. Connection methods return `T | Error` and you check `instanceof`:

- `FlumeConnectionError` — WebSocket closed before ready
- `FlumeHttpError` — HTTP call returned an error payload (e.g. Slack `ok: false`)
- `FlumeParseError` — Unparseable WebSocket frame

Internal handler exceptions are caught and logged (never rethrown into the protocol loop).

## Supported sources

| source  | transport                         | auth                                          |
|---------|----------------------------------|-----------------------------------------------|
| Discord | Gateway WebSocket v10 (JSON)     | bot token                                     |
| Slack   | Socket Mode WebSocket            | app token (`botToken` optional, for future)  |
| GitHub  | REST polling `/notifications`    | personal access token                         |

GitHub also exposes `gh auth token` if you want to reuse the `gh` CLI's session:

```ts
import { execSync } from "node:child_process"
const token = execSync("gh auth token").toString().trim()
const github = flume.github({ token })
```

## Module layout

- `Flume` — DI container; `.discord()` / `.slack()` / `.github()` build sources with shared deps
- `FlumeDiscordSource` / `FlumeSlackSource` / `FlumeGitHubSource` — high-level sources
- `FlumeDiscordGateway` / `FlumeSlackSocketMode` / `FlumeGitHubPoller` — protocol layer
- `FlumeReconnector` — exponential backoff with jitter
- `FlumeLogger` — structured log emitter (feeds `onLog`)
- `FlumeRuntimeDeps` — IO boundary port
- Zod schemas for every external boundary: `FlumeGatewayMessageSchema`, `FlumeSlackEnvelopeSchema`, `FlumeSlackConnectionResponseSchema`, `FlumeGitHubNotificationSchema`

## Development

```bash
bun install
bunx vp pack       # build dist/
bunx tsc --noEmit  # typecheck
bunx vitest run    # tests
bunx vp lint       # lint
bunx vp fmt        # format
```

The library itself is runtime-agnostic. The dev toolchain (build / test / lint) uses Bun + `vite-plus` + `vitest`, but the published `dist/` is plain ESM with TypeScript declarations and runs on Node 18+, Bun, Deno, Cloudflare Workers, or modern browsers.

## License

MIT © Interactive Inc.
