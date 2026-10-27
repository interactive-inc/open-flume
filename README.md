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
await running.drain() // outside callbacks: finish accepted callback work and diagnostics
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
- `FlumeRunning.close()` waits for sources to stop and returns a `FlumeClosed` snapshot. It is idempotent and concurrent-safe, and can be awaited inside `onEvent` or `onError`.
- `FlumeRunning.drain()` waits for accepted callbacks and their failure diagnostics. Call it **outside callbacks**, normally after `close()`, when shutdown must also finish callback work. Calling it inside a callback would wait for that callback itself.
- `open()` and `close()` do not wait for observation callbacks. A slow callback cannot hold startup or source shutdown open. Callback delivery remains serial; accepted callbacks may finish after `close()` returns.
- `FlumeClosed` exposes `statuses()` and `errors()` — frozen snapshots of final source states and disconnect failures. No `open`, no `close`, no leaking source references.
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
await running.drain() // finish callback delivery before exiting the host process
```

Migration from 0.10.1: shutdown code that relied on `close()` waiting for `onEvent` / `onError` should now use `await running.close(); await running.drain()` outside those callbacks. A callback may use `await running.close()` on its own. Pull streams finish after the accepted callbacks and their failure diagnostics have drained. A callback that never settles can therefore keep `drain()` and stream completion pending, while source shutdown still completes.

## Dynamic groups

A `Flume` is single-use: its source set is fixed at construction and the FSM is terminal once closed. To add or drop sources at runtime, compose at a higher level with `FlumeConfluence` — it holds many `Flume` instances, merges all their firehoses into one `onEvent`, and lets you `add` / `remove` groups while the others keep running.

```ts
import { FlumeConfluence } from "@interactive-inc/flume"

const confluence = new FlumeConfluence({
  onEvent: (item) => {
    if (item.kind === "event") feedToAgent(item.groupId, item.event)
    if (item.kind === "log" && item.log.action === "status") noticeDisconnect(item.log)
  },
  reconnect: { maxAttempts: 10 },
})

await confluence.add("team-a", [new FlumeDiscordSource({ token: tokenA })])
await confluence.add("team-b", [new FlumeSlackSource({ appToken })]) // existing groups keep running
await confluence.remove("team-a") // stops only team-a
await confluence.closeAll()
```

`add(id, sources)` starts a fresh `Flume` for that group and returns `Error | null` (a duplicate id or a failed start is returned, never thrown). Every merged item is stamped with that id as `groupId`, while `item.event.source` / `item.log.source` identifies the protocol source inside the group. Each group is an independent `Flume`, so a failure in one group never rolls back another. `replace(id, sources)` opens a replacement first and closes the previous group only after the new one is ready; a failed replacement leaves the previous group running.

Source instances are single-use. Reusing an existing instance returns a `FlumeStartError` and preserves its current owner's connection. `remove()` / `closeAll()` wait for source shutdown; callbacks already accepted by those groups may finish afterward.

## Sub-entries

Each source has a dedicated entry — importing one does not pull the others into your bundle. The root entry never loads source-specific code.

```ts
import { Flume, FlumeSource } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"
import { FlumeGitHubSource } from "@interactive-inc/flume/github"
import { FlumeTimeSource } from "@interactive-inc/flume/time"
```

- `@interactive-inc/flume` — `Flume`, `FlumeConfluence`, `FlumeRunning`, `FlumeClosed`, `FlumeSource` (abstract base for third-party sources), `createFlumeDefaultDeps`, errors, types, and the common `attempt` / `safe*` utilities used by custom sources
- `@interactive-inc/flume/discord` — `FlumeDiscordSource`, `FlumeDiscordGatewayIntents`, `flumeExtractDiscordMeta`
- `@interactive-inc/flume/slack` — `FlumeSlackSource`, `flumeExtractSlackMeta`
- `@interactive-inc/flume/github` — `FlumeGitHubSource`, `flumeExtractGitHubMeta`
- `@interactive-inc/flume/time` — `FlumeTimeSource`, `parseCron`, `flumeCronNext`, `flumeCollectCatchupMatches`

## Custom sources

Extend `FlumeSource` to plug in any protocol. The base class owns `start`/`stop`/`status`, the per-source event queue, status emission (logged on every transition), and consumed/stopped guards. You implement `connect` (open the protocol, emit events, set status) and `disconnect` (tear it down).

```ts
import {
  attempt,
  FlumeSource,
  safeErrorMessage,
  safeInvokeCallback,
  safeJsonParse,
  safeNow,
  safeReadText,
} from "@interactive-inc/flume"
import type { FlumeSourceStartContext, FlumeTimerHandle } from "@interactive-inc/flume"

class MyWebhookSource extends FlumeSource {
  readonly name = "my-webhook"

  private timer: FlumeTimerHandle | null = null

  constructor(private readonly options: { url: string; pollInterval?: number }) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")
    const interval = (this.options.pollInterval ?? 30) * 1000
    const timer = attempt(() => ctx.deps.setInterval(() => this.runPoll(ctx), interval))
    if (timer instanceof Error) return timer
    this.timer = timer
    this.setStatus("connected")
    return null
  }

  protected disconnect(): void {
    const timer = this.timer
    this.timer = null
    const ctx = this.context
    if (timer === null || ctx === null) return

    const result = attempt(() => ctx.deps.clearInterval(timer))
    if (result instanceof Error) {
      ctx.log.error({
        action: "timer.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
  }

  private runPoll(ctx: FlumeSourceStartContext): void {
    safeInvokeCallback({
      fn: async () => {
        const result = await this.poll(ctx)
        if (result instanceof Error) {
          ctx.log.error({
            action: "poll.error",
            message: safeErrorMessage({ error: result }),
            error: result,
          })
        }
      },
      onError: (error) => {
        ctx.log.error({
          action: "poll.unhandled",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }

  private async poll(ctx: FlumeSourceStartContext): Promise<Error | null> {
    const response = await attempt(() => ctx.deps.fetch(this.options.url))
    if (response instanceof Error) return response
    const raw = await safeReadText({ response, context: this.name })
    if (raw instanceof Error) return raw
    const payload = safeJsonParse(raw)
    if (payload instanceof Error) return payload

    this.emit({
      source: "custom",
      sourceName: this.name,
      type: "webhook",
      data: { payload },
      meta: { event_type: "webhook" },
      receivedAt: safeNow({ deps: ctx.deps }),
    })
    return null
  }
}
```

`this.emit({...})` は同期の受け手へその場で転送し、非同期 callback は Source ごとに順序を保って処理する。Flume の有限配送バッファの手前に無制限なキューを作らない。Source を直接 `start(ctx)` する場合は `ctx.eventBuffer` (既定 1000、実行中を含む) が上限となり、満杯時は新着を破棄して受信を継続する。`source.deliveryStatus()` は `pendingEvents`・`droppedEvents`・`callbackFailures` を不変スナップショットで返す。

`source.stop()` は接続の停止まで待ち、通知 callback の完了は待たない。callback 内から停止を await できる。受理済み通知とその失敗診断まで待つ場合は、callback の外で `await source.stop(); await source.drain()` とする。単体起動でも signal は停止まで購読され、起動失敗時は自動で後始末する。

`this.setStatus(...)` は同じ状態と理由の連続通知を省略する。`connect()` の後に動く timer などの IO は `attempt()` または `safeInvokeCallback()` で囲む。どちらも root entry から import できる。

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

`statePersister` は checkpoint を直列保存する。保存が遅い間は実行中の一件と最新の待機値一件だけを保持し、中間の保存要求は最新値へ集約する。tick の配送とは独立している。`stateTimeoutMs` は各 `load` / `save` の期限 (既定 30 秒)。persister の任意引数 `{ signal }` を IO に渡し、取消しに対応すること。

通常の保存失敗は記録して次の保存を続ける。保存が期限切れになると `stateStatus().phase` が `halted` となり、tick は継続するが追加保存は停止する。取消し非対応の古い書き込みが後から完了して新しい checkpoint を上書きするのを防ぐため、同じ Source では自動再開しない。復旧時は保存先に残る処理の終了を確認すること。

`source.stateStatus()` の `lifecycle` は `open` → `closing` → `closed`、保存処理の `phase` は `idle` → `saving` → `idle` / `halted` を表す。終了処理中も受理済みの保存を待ち、`halted` から追加保存は再開しない。

`source.stateStatus()` で `lastRequestedAt`・`lastSavedAt`・`pendingWrites` (最大二件)・`coalescedWrites`・`failedWrites`・`error` を確認できる。`pendingWrites` は終了後も未完了のホスト IO や未保存の待機値を含む。通常の保存失敗が解消されないまま停止した場合も `error` と二つの時刻から未保存状態を判別できる。停止は有限件数の保存を各期限まで待ち、最後の未解消エラーを `FlumeStateError` として `source.stop()` と `FlumeClosed.errors()` に返す。ホストが返した元の例外は `cause` に保持する。persister 内から Flume の終了を await しない。

`load` の失敗・期限切れ・不正な保存値は診断を残して catchup を省略する。停止時は load の signal を中断し、遅延結果を無視する。`catchupPolicy` (`off` / `lastOnly` / `missed`) で次回起動時の再配送を選ぶ。同じ保存キーを複数の Source で共有する場合は、ホスト側でも書き込み順を直列化するか、版の比較を原子的に行う。

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
// after close / signal abort, the loop ends once accepted callback diagnostics are delivered
```

The iterator ends cleanly after source shutdown and accepted callback work; `break`ing out unsubscribes automatically. When a slow consumer lets the buffer overflow, the oldest items are dropped by default:

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

Flume never calls a third-party service. Every internal action is reported through the firehose (`onEvent` push / `stream()` pull) as `{ kind: "log", log }` items — there are no silent internal paths. Each `FlumeLog` is tagged with `source: "flume"`, `source: "discord"`, `source: "slack"`, `source: "github"`, `source: "time"`, or your custom source's `name`. `onError` is a convenience filter that additionally receives only the `level: "error"` subset (route it straight to Sentry).

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
- Observation sink failures — a failed `onEvent` emits `onEvent.error` to `onError` and the pull stream; a failed `onError` emits `onError.error` to `onEvent` and the pull stream. `detail` identifies the callback plus the failed item's kind, source, and event type or log action. `FlumeConfluence` also stamps `groupId` onto logs sent to its `onError`. The peer notification is attempted once, without recursive reporting.

For maximum diagnostics, configure both callbacks or keep a pull stream consumer. If both push callbacks fail, both failure logs remain available to the pull stream; startup logs are replayed to its first subscriber. `FlumeConfluence` has no pull iterator, so configure its `onError` as the independent diagnostic sink.

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
new Flume({
  sources,
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

- Ordering — each source has its own `FlumeSerialQueue` and per-source events are published FIFO. The public `onEvent` and `onError` callbacks share one global queue, so callback invocations never race across sources. `close()` stops the sources; a subsequent `drain()` waits for callbacks and their failure diagnostics. Pull subscribers receive items when they are published and have independent bounded buffers. push の通常配送には `callbackBuffer` (既定 1000) の上限があり、満杯時は新着を破棄する。診断用の配送は別枠で受け付け、`callback.overflow` を通知する。
- Duplicate suppression — Slack envelopes are deduped by `envelope_id` (`FlumeSlackSeenCache`) to absorb ack retries. GitHub notifications are deduped by `id + updated_at` (`FlumeGitHubSeenCache`). Discord uses session resume so the Gateway does not re-emit dispatches.
- Partial-failure rollback — if any source fails during `Flume.open()`, sources acquired by that open are stopped, including partially connected failures. A source that refused reuse remains with its existing owner. A `FlumeStartError` is returned with per-source detail.
- Idempotent close — `FlumeRunning.close()` is safe to call concurrently; the first call wins and subsequent callers receive the same `FlumeClosed` snapshot. The same guard exists at source level: a double-`close()` (e.g. via signal abort racing a manual close) does not re-invoke `disconnect()`.

## Errors

Flume does not throw on protocol/network failures. Every entry point returns `T | Error` — branch with `instanceof Error`. `Flume.open()` returns `FlumeRunning | FlumeStartError`; protocol-layer helpers (`FlumeDiscordGateway.connect()`, etc.) return `T | Error`:

- `FlumeStartError` — `Flume.open()` refused or failed (already started, signal aborted, partial-failure rollback)
- `FlumeConnectionError` — WebSocket closed before ready
- `FlumeHttpError` — HTTP call returned an error payload (e.g. Slack `ok: false`)
- `FlumeParseError` — Unparseable WebSocket frame
- `FlumeOperationError` — 操作の期限切れ・中断・timer など実行基盤の失敗。通信や保存の種類に依存しない。
- `FlumeStateError` — 永続状態の読取・検証・保存の失敗。保存の期限切れもこの型で返し、`errorCode` は `FLUME_TIMEOUT`、`cause` は期限切れを表す `FlumeOperationError` となる。

Exceptions thrown or rejected from `onEvent` / `onError` are isolated and emitted as `onEvent.error` / `onError.error` diagnostics; they are never rethrown into the protocol loop.

With type-valid constructor input, the library guarantees that protocol IO and user callback failures do not escape `open()`, `close()`, the `onEvent` invocation path, the `stream()` iterator, the abort-signal path, or the `onError` callback. These boundaries route work through internal `safe*` wrappers and the generic `attempt()` helper.

### 診断コードと調査情報

`FLUME_ERROR_CODES` は公開コードと確認事項の一覧。コードの意味は変更・再利用しない。標準 warn / error ログと公開エラーは `errorCode` を持つ。成功した debug / info ログには失敗コードを自動付与しない。`action` は発生操作、`source` は接続元、`detail` はその時点の状況を示す。

```ts
import { FLUME_ERROR_CODES } from "@interactive-inc/flume"

const flume = new Flume({
  sources,
  callbackBuffer: 1000,
  onError: (log) => {
    console.error(
      {
        errorCode: log.errorCode,
        guidance: log.errorCode ? FLUME_ERROR_CODES[log.errorCode] : undefined,
        source: log.source,
        action: log.action,
        detail: log.detail,
      },
      log.error,
    )
  },
})
```

- `FLUME_TIMEOUT` と `FLUME_ABORTED` は別の状態。GitHub の期限超過では operation・phase・page を記録する。応答を受け取れた場合は httpStatus と、サーバーが提供する requestId も保持する。`requestTimeoutMs` (既定 30 秒) は fetch、本文読取、全ページの取得を含む。
- `FlumeHttpError.status` とサービス固有の `code`、`FlumeConnectionError.code` (WebSocket 切断コード) は `errorCode` と独立して保持する。元の例外は `cause` に保持し、複数 Source の起動失敗は `AggregateError.errors` まで辿れる。
- `closed.errors()` には source・errorCode・元の error が残る。停止失敗をログの検索だけに依存させない。
- 予期しない WebSocket 切断は接続後も `ws.closed` として onError に通知する。切断コード・reason・接続段階・元の CloseEvent を保持する。明示的な close や接続更新の指示とは区別する。
- `FLUME_BUFFER_OVERFLOW` は通知を破棄した診断。受信を継続し、`flume.deliveryStatus()` / `running.deliveryStatus()` で callback の待機・失敗・破棄数と pull バッファの累積破棄数を確認できる。pull の値は起動時バッファと各購読バッファでの破棄の合計であり、ユニークなイベント数ではない。
- `close()` は Source の停止、`drain()` は受理した callback とその診断の配送完了を待つ。callback 内から `drain()` を待つと自身を待つため、callback の外で使う。完了しないユーザー callback は `drain()` も完了させないが、Source の停止は妨げない。
- 相手側が理由を返さない切断では真因を断定しない。観測した切断コード、失敗した段階、直前の状態遷移から調査する。本文や token をログへ追加して原因推定しない。

GitHub のページ送りは同一 API の通知エンドポイントだけを許可し、HTTP redirect は拒否する。全ページの JSON と schema が正常なときだけ既読・ETag・cursor を確定する。Slack の URL 取得にも handshakeTimeoutMs (既定 30 秒) を適用し、その後の WebSocket handshake には別に同じ期限を適用する。

`FlumeConfluence.remove()` は接続途中のグループも中断し、停止まで待つ。同じグループの並行 replace は拒否する。Source の connect は中断時に解放されるが、カスタム Source は IO に `ctx.signal` を渡し、await 後に `this.isStopped` を確認して停止済みのリソースを作り直さないこと。

### 設定の所有者

Flume、Confluence、標準 Source は生成時に設定を取り込み、稼働中・再接続時も同じ値を使う。渡したオブジェクトを書き換えても、認証情報、cron、catch-up、再接続方針、通知先は変更されない。設定変更には新しいインスタンスを作り、Confluence.replace で接続を交換する。

値としての設定と、signal・deps・statePersister などの IO ポートは区別する。ポートは選択したインスタンスの参照を維持し、ホストが所有するオブジェクトをライブラリから凍結しない。Source 単体利用では start 時に context を取り込む。

設定の getter が失敗した場合、コンストラクタから例外を漏らさず、open / start / add の戻り値に FLUME_RUNTIME_FAILED と cause を保持する。Time の message は同期関数とし、引数の tick は不変。呼出しや戻り値の検証に失敗した場合は message.error を記録し、既定の tick と checkpoint 保存を続ける。

### 起動期限と中断の所有者

`new Flume({ sources, signal, openTimeoutMs: 10_000 })` は起動全体に期限を設ける。openTimeoutMs は有限の正数で、未指定なら全体期限なし。タイマーの上限を超える指定は 2,147,483,647 ms に制限する。期限切れでは接続中の処理を中断し、取得済み Source の停止を待って FLUME_TIMEOUT を返す。カスタム Source の disconnect が完了しなければ、後始末の待機は続く。

ホストの signal は Flume 生成時に一度選び、停止まで同じ購読を保持する。起動中に渡した options.signal を変更しても中断先は変わらない。明示的な中断は FLUME_ABORTED、起動期限の予約失敗は FLUME_TIMER_FAILED、signal 操作の失敗は FLUME_RUNTIME_FAILED として区別する。取消し済みタイマーは runtime の clearTimeout が失敗しても接続を停止しない。元の例外は診断の error に残る。

Confluence の replaceTimeoutMs は同じ起動期限を使う。交換が失敗したら既存の接続を維持する。交換成功時の取消し失敗は、新しい接続の observe(id).diagnostics() でも取得できる。running.statuses() の配列と各要素は実行時にも凍結される。

### 過負荷時の観測

`callbackBuffer` は実行中を含む callback の通常配送枠で、イベントと全レベルのログが共有する。満杯時は新着の配送を破棄し、受信と再接続を継続する。Slack の ACK はライブラリの受信に対する応答であり、利用側 callback の処理完了や永続化を保証しない。

- `droppedEventCallbacks`: バッファ上限で破棄したイベントの onEvent 配送数。
- `droppedLogCallbacks`: バッファ上限で破棄したログの onEvent / onError 配送数。同じログでも配送先が二つなら二件として数える。
- `droppedCallbacks`: 上記二つの合計。
- `droppedDiagnosticCallbacks`: ログ配送のうち診断専用枠でも受理できなかった数。専用枠も最大二件に制限する。
- `callbackFailures`: 受理した callback の例外や callback 取得失敗の累積数。バッファ超過とは別に数える。

`flume.diagnostics()` / `running.diagnostics()` は最新百件の warn / error を、callback や pull の混雑に依存せず返す。`total` は累積診断数、`evicted` は履歴上限で破棄した数。配列と各ログ・detail の直下は不変のスナップショットで、元の error / cause の参照を保持する。起動失敗後や close 後も取得できる。全履歴を保存する場合は、利用側のログ保存先へ継続的に転送する。

```ts
const state = running.deliveryStatus()
console.log(state.droppedEventCallbacks, state.droppedLogCallbacks)
const history = running.diagnostics()
console.log(history.total, history.evicted, history.entries)

// Confluence では稼働中のグループから観測専用ハンドルを取得する。
const view = confluence.observe("group")
await confluence.remove("group")
console.log(view?.deliveryStatus(), view?.diagnostics())
```

Confluence の取得済み観測ハンドルは、その時点の接続世代を保持する。replace 後の新しい接続を観測する場合は再取得する。callback の停止待ちと同様、全記録の永久保存や相手サービス内部の真因特定をライブラリ単独で保証するものではない。

### 障害と負荷の検証

`bun run test` は単体試験に加え、ローカルの実 HTTP / WebSocket サーバーで再接続、Slack ACK、本文停止、handshake 停止、abort、タイマー解放を検査する。外部サービスや実トークンは使用しない。

テストランナーとサーバーは Node.js で動かす。Bun の検証は別プロセスでライブラリを実行し、Bun 標準の fetch / WebSocket を Node.js の試験サーバーに接続する。ライブラリの受信側と試験サーバーの runtime 互換性を独立して検証するため、試験全体を `bun --bun` で起動しない。

`bun run test:stress` は Discord と Slack をそれぞれ二百回接続し、各接続で五百件を受信する。十分な callbackBuffer を与えて順序・欠落・停止後の予約を検査する。小さなバッファでの破棄と回復は別の試験で件数まで検査する。実サービスでの長期運用や、runtime 内部の全リソースの証明に置き換わる試験ではない。

## Supported sources

- Discord — Gateway WebSocket v10 (JSON), bot token
- Slack — Socket Mode WebSocket, app token
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
vp pack            # build dist/
vp run typecheck   # typecheck
vp test            # tests
vp lint            # lint
vp fmt             # format
```

The library itself is runtime-agnostic. The dev toolchain (build / test / lint) uses Bun + `vite-plus` + `vitest`, but the published `dist/` is plain ESM with TypeScript declarations and runs on Node 22+, Bun, Deno, Cloudflare Workers, or modern browsers. Runtimes without a global `WebSocket` (e.g. fetch-only edge functions) can still use `FlumeGitHubSource`; `FlumeDiscordSource` / `FlumeSlackSource` will return `FlumeStartError` at `open()` time if `deps.WebSocket` is `null`.

## License

MIT © Interactive Inc.
