# Changelog

## 未リリース

- Slack Source は受信に使う appToken だけで構築できる。未使用だった botToken の必須指定を撤廃し、互換用の指定が残っていても読み取らず保持しない。

- Flume、Confluence、全標準 Source の設定を生成時に固定する。既存 options の書換えによる通知先・認証情報・cron・catch-up・再接続方針の変更は反映しない。設定を変える場合は新しい Source / Flume を作成して replace する。ホスト signal も open 時ではなく生成時に選択する。
- 設定取得の失敗は、コンストラクタから throw せず FLUME_RUNTIME_FAILED と元の cause を返す。Time の message が返した値の検証失敗でも tick と保存を継続し、非同期 message の誤用・遅延 rejection も診断する。

- ホストの中断購読を起動から停止まで保持し、起動中に options.signal が変更されても所有者を切り替えない。購読解除の失敗は元の例外と公開コードを残し、中断と接続失敗が競合しても原因を捨てない。statuses は実行時にも不変のスナップショットを返す。
- openTimeoutMs で Flume 全体の起動期限を指定できる。Confluence の交換期限も同じ所有者にまとめ、期限切れ・中断・タイマー故障を区別する。タイマー取消し失敗は診断に保持し、取消し済みの遅延発火を無効にする。

- 状態保存のフラグを不変の union 型に置き換え、保存状態と終了状態の遷移を明示する。stateStatus に lifecycle を追加し、同時停止で完了 Promise を共有する。期限切れ後に保存が成功した場合は、同じ checkpoint を未保存件数から除く。
- FlumeOperationError と FlumeStateError を公開する。保存の検証失敗を接続エラーとして扱わず、元の IO エラーは cause に保持する。保存の停止結果を比較する利用側は、元の例外を cause から参照する。既存の errorCode と通信境界のエラー型は維持する。

- Source の同期転送を即時処理し、公開バッファの手前にある無制限キューを解消する。単体利用の非同期 callback も有限にし、破棄数を公開する。stop と drain を分け、callback 内からの停止待ちを可能にする。単体起動の失敗後始末と接続後の signal 購読を追加する。
- Time の保存待ちは実行中と最新値の二件までに制限し、各 IO に期限と AbortSignal を追加する。期限切れ後は受信を継続しつつ追加保存を止め、書込順序の逆転を防ぐ。stateStatus と停止結果で未保存状態・原因を公開する。cron と timer の失敗原因も保持する。

- 過負荷時は破棄数を公開して受信を継続する。イベントとログの破棄数を分け、診断専用配送も有限にする。最新百件の診断履歴と破棄数を callback の混雑と独立して取得できる。Confluence に接続世代ごとの観測ハンドルを追加する。
- 接続後の予期しない切断、認証拒否、HTTP エラー、解析失敗をコードで区別する。HTTP 状態と requestId、元の切断イベントや例外を保持し、成功ログに失敗コードを付けない。
- heartbeat の追加要求、無効な間隔、再接続タイマーの reset・二重発火、取消し失敗通知中の再入、起動時 pull バッファの再入順序を修正する。
- 実 HTTP / WebSocket を使う障害・負荷試験を追加する。Node.js 22 / 24 / 26 を CI で検証する。追加した WebSocket サーバーはテスト用依存のみで、実行時依存は増やさない。
- Source の停止を共有する完了 Promise と不変状態にまとめ、接続途中の中断・失敗・再入を処理する。Confluence のグループ状態と通知配送の責任を分離する。
- 古いソケットからのフレーム、取消し失敗後のタイマー、close を伴わない socket error、接続先 URL の取得停止を処理する。
- GitHub は全ページの取得・検証に成功した場合だけ checkpoint を確定する。通信期限とページ送り先の制約を追加する。
- 公開 errorCode、原因を保持する cause、通信段階の context、停止エラーのコードを追加する。コードの登録漏れをテストする。
- callbackBuffer は既定 1000。通常配送の上限を超えた新着は破棄し診断する。deliveryStatus() で配送の待機・失敗・破棄を確認できる。大量処理を前提とする利用側は buffer と処理速度を確認すること。
- 依存パッケージと lock を更新し、全公開エントリの型を CI で検証する。

## 0.11.0

### Changed

- `open()` and `close()` complete independently of observation callbacks. `close()` waits for source shutdown and may be awaited from `onEvent` / `onError`; call the new `running.drain()` outside callbacks after closing to wait for accepted callbacks and failure diagnostics. This changes the callback-draining behavior introduced in 0.10.1. Pull streams retain late callback diagnostics before finishing.
- Time-source state writes run serially, and stopping waits for queued writes to finish. Write failures remain isolated and logged.

### Fixed

- Prevent callback-triggered shutdown from waiting on itself, and prevent slow observation callbacks from blocking `open()`.
- Release time-source startup when stopping during state loading, and prevent a late load result from starting an orphan scheduler.
- Preserve existing connections when a source is reused by another Flume or a failed confluence replacement; still clean up newly acquired, partially connected sources.
- Prevent out-of-order completion of time-source state writes from replacing the latest `lastFiredAt` with an older value.

## 0.10.1

### Added

- Publicly export the common `attempt`, `safe*`, and `isRecord` utilities from the root entry so third-party `FlumeSource` implementations can use the same throw-isolation boundaries as built-in sources.
- Emit `onEvent.error` and `onError.error` diagnostics when an observation callback throws or rejects. The failure is published to the pull stream and forwarded once to the peer callback without recursive reporting.

### Changed

- Serialize both `onEvent` and `onError` through the callback queue so `close()` drains callback work and callback-failure diagnostics deterministically.
- Correct the README contracts for `FlumeConfluence.groupId`, global callback ordering, custom-source IO isolation, and callback-failure observability.

### Removed

- Remove unused internal `FlumeSerialQueue` capacity, cancellation, and size APIs, plus the inaccessible `FlumeStream.dropped` getter.

## 0.10.0

### Added

- `FlumeConfluence` stamps every merged item with its originating `groupId` and supports failure-safe `replace(id, sources)` with an open timeout.
- `FlumeTimeSource` supports host-provided state persistence and `off`, `lastOnly`, or bounded `missed` catch-up policies.
- Export `flumeCollectCatchupMatches` for hosts that need the same catch-up calculation without constructing a source.

## 0.9.4

### Fixed

- `FlumeSlackSource` now exposes and forwards `idleTimeoutMs` to the Slack Socket Mode transport. The watchdog remains opt-in, but hosts can now enable silent-connection detection without constructing `FlumeSlackSocketMode` directly.

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
