# Changelog

## 0.9.3

### Fixed

- Slack Socket Mode no longer force-closes quiet-but-healthy sockets by default. The frame-silence watchdog added in 0.9.2 remains available through `idleTimeoutMs`, but it is now opt-in because Slack does not guarantee regular application-level frames during idle periods across all runtimes and network paths.

## 0.9.2

### Fixed

- **`FlumeSlackSocketMode` now force-closes the socket after 90s (default, configurable via `idleTimeoutMs`) of frame silence.** Slack Socket Mode previously relied entirely on kernel TCP keepalive (defaults to 2h on macOS/Linux) to detect a silent NAT / corporate proxy / load-balancer idle drop. The result was that a host could go up to 2 hours without realising Slack notifications had stopped flowing. The watchdog ticks every 15s, compares `now() - lastFrameAt` against the limit, and triggers the existing close path so the source's reconnect picks it up. Discord already had this via heartbeat ack; Slack now matches.
- `FlumeSlackSocketMode.props.deps` requires `setInterval` and `clearInterval` (the watchdog uses them). Hosts that constructed a raw `Deps` literal need to add those two fields; hosts using `createFlumeDefaultDeps()` need no change.

## 0.9.1

### Fixed

- `FlumeConfluence.add()` no longer races on a duplicate `id`: the `has(id)` guard ran before `await flume.open()` while `set(id)` ran after, so two concurrent `add()` calls with the same id both passed the guard and the second overwrote the first — orphaning a `FlumeRunning` that never got closed. It now re-checks after `open()` and closes the loser.

## 0.9.0

### Breaking

- **Constructor takes a single options object: `new Flume({ sources, ...options })`.** `sources` is the only required field. The previous two-argument `new Flume(sources, options?)` form is removed.
- **Lifecycle verbs renamed to `open()` / `close()`.** `Flume.start()` → `Flume.open()`, `FlumeRunning.stop()` → `FlumeRunning.close()`. The river-gate metaphor matches the streaming model (open the flume to let it flow, close it to stop).
- **`FlumeStopped` → `FlumeClosed`** (`kind: "stopped"` → `"closed"`); **`FlumeStopError` → `FlumeCloseError`**. `FlumeRunning` keeps its name (`kind: "running"`).
- **Observation collapsed into one firehose.** The separate `onEvent` (typed event), `onLog`, and `onStatus` callbacks are removed. A single `onEvent: (item: FlumeStreamItem) => void` now delivers received events **and** every log merged into one discriminated union (`{ kind: "event"; event } | { kind: "log"; log }`); the consumer filters by `item.kind` / `item.log.level`. Connection-state transitions ride the firehose as `status` log entries instead of a dedicated callback. `onError` is kept as a convenience filter for `level: "error"` logs (e.g. Sentry). Built for piping the whole picture into an agent (Claude / Codex) so it can notice disconnects itself.
- **Log action renames:** `flume.start.*` → `flume.open.*`, `flume.stop.*` → `flume.close.*`.

### Added

- **`FlumeTimeSource`** (subpath `@interactive-inc/flume/time`) — emits a `tick` on a 5-field cron schedule (wall-clock, self-contained cron parser, no external deps). `message()` customizes each tick's `type` / `data` / `meta`. Also exports `parseCron` and `flumeCronNext`. Holds no connection, so it reaches `connected` immediately and is never a reconnect target.
- **`FlumeRunning.stream(options?)`** — pull-style async iterator over the same firehose (`FlumeStreamItem`), for `for await` consumption. Bounded buffer with `{ buffer, onOverflow: "drop-oldest" | "drop-newest" }` (default 1000 / drop-oldest); ends cleanly on `close()` / signal abort.
- **`FlumeConfluence`** — a higher-level supervisor that holds many `Flume` instances, merges all their firehoses into one `onEvent`, and lets you `add(id, sources)` / `remove(id)` / `closeAll()` groups at runtime while the others keep running. Each group is an independent `Flume`, so one group's failure never rolls back another.
- **`FlumeOptions`** type is now exported.

### Fixed

- `FlumeReconnector` no longer advances the attempt counter when `setTimeout` itself throws — a rejected timer scheduling used to skip a backoff step, growing the next delay incorrectly.
- GitHub poller: a rate limit on the **initial** poll no longer immediately re-arms the polling interval, which previously cancelled out the rate-limit pause. Added coverage for the `429` / `403 + X-RateLimit-Remaining: 0` paths (previously untested).
- `FlumeGitHubSeenCache` / `FlumeSlackSeenCache` `trim()` now evicts the oldest keys in place (`O(evicted)`) instead of rebuilding the whole `Map` (`O(n)`).
- Removed a redundant double `.catch()` on the GitHub poll interval (dead code — `poll()` never rejects).

## 0.8.0

### Breaking

- **`FlumeSourceStartContext.signal` is now an optional field (`signal?: AbortSignal`)** instead of required `signal: AbortSignal | undefined`. Tests / wrappers that previously had to pass `signal: undefined` explicitly can drop the field. Source implementations already used `ctx.signal` defensively, so runtime behavior is unchanged.

### Added

- `FlumeStopped.errors(): ReadonlyArray<{source, error}>` lists per-source disconnect failures so hosts can branch on stop outcome without grepping `onLog` for `flume.stop.failed`. Empty when every source stopped cleanly.
- `FlumeRunning.signal` getter exposes the host-supplied `AbortSignal` so callers without the original controller can check `running.signal?.aborted`.

### Fixed

- Tests no longer depend on `vi.waitFor` (not supported by `bun:test`). Replaced with an in-repo `waitFor` helper at `lib/test-utils/wait-for.ts`. The 11 previously-failing `vi.waitFor` tests now run.
- `dist/` chunk filenames lost the build hash (`flume-source-DDbm_Eik.js` → `flume-source.js`). Stable filenames mean bundle diffs across rebuilds only churn on real content changes.

## 0.7.0

### Breaking

- **`FlumeSourceStartContext` gains a required `signal: AbortSignal | undefined` field.** Custom `FlumeSource` subclasses that construct a context literal in tests or wrappers must add `signal`. Built-in sources are unaffected.

### Added

- `FlumeSourceStartContext.signal` carries the host's `Flume({ signal })` down to `connect(ctx)`. Source implementations can now plumb the host signal into their own `fetch(url, { signal })` / `setTimeout` cancellation / WebSocket close paths so a host SIGTERM propagates natively, instead of relying solely on Flume's outer `runStop` to stop the source.

### Fixed

- `createFlumeDefaultDeps().WebSocket` is now resolved lazily on each call (was cached at module load). Tests that patch `globalThis.WebSocket` in `beforeEach`, jsdom / happy-dom environments where `WebSocket` is installed after the flume import, and any other late-initialised WebSocket constructor now reach Flume correctly. `fetch` / timers were already lazy; `WebSocket` lagged behind for no good reason.

## 0.3.0

### Breaking

- **`Flume.start()` returns `Promise<FlumeRunning | FlumeStartError>`.** Branch with `instanceof Error`. The FSM transition flows through the return value directly — `flume.runningState()` is removed.
- **`Source.start()` returns `Promise<Error | null>`** instead of `void | Error`. Branch with `instanceof Error`.
- **`FlumeSlackSourceOptions.botToken` is now required.** Flume's Socket Mode transport only needs `appToken` to open the socket, but every realistic consumer needs the bot token to call `auth.test` for self-detection and to post replies. The type now forces it to be present rather than leaving it optional and failing at runtime.
- **`scheduleFlumeReconnect` removed from the public API.** It was an internal utility re-exported by accident. The reconnect behavior is unchanged — `FlumeReconnector` and `resolveFlumeReconnectConfig` remain public for callers building their own source.

### Added

- `FlumeStartError` — typed error returned by `Flume.start()` / `Source.start()` for refusal and rollback cases (already started, signal aborted, partial-failure rollback). Exported from the root entry.

## 0.2.0

### Breaking

- Redesigned `Flume` as a type-state FSM: `Flume` (pre-start) → `FlumeRunning` (post-start) → `FlumeStopped` (post-stop). The previous single-class `Flume` with `start`/`stop` on the same instance is replaced.
- Source classes moved behind per-type subpath entries: `@interactive-inc/flume/discord`, `@interactive-inc/flume/slack`, `@interactive-inc/flume/github`. The root entry no longer exports `FlumeDiscordSource` / `FlumeSlackSource` / `FlumeGitHubSource`; import from the subpath instead. Types (`FlumeEvent`, `FlumeSource`, etc.) remain on the root.
- `FlumeSourceOptions.deps` is now optional. When omitted, the source calls `createFlumeDefaultDeps()` internally. The previous form requiring `deps` is still supported.
- `FlumeSource` interface added: `{ name, start, stop, status }` with `start` returning `Promise<void | Error>`. All three sources implement it.
- `FlumeSourceStatus` and `FlumeRunning.statuses()` / `FlumeStopped.statuses()` added to inspect per-source state without holding source references.
- `FlumeSlackSeenCache` added inside `FlumeSlackSource` to deduplicate re-delivered Socket Mode envelopes by `envelope_id`.

## 0.1.0

- Initial release. Discord Gateway (raw WebSocket v10), Slack Socket Mode (raw WebSocket), GitHub notifications (REST polling). All transport is raw `fetch` + `WebSocket` + Zod — no `discord.js`, `@slack/bolt`, or `@slack/web-api`. Sentry dependency removed in favor of an `onLog` callback the host routes to its preferred sink.
