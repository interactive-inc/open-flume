# Changelog

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
