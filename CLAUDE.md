Discord / Slack / GitHub の通知を統一的に受信するライブラリ。全ての内部動作がログで観測可能。

## セットアップ

```sh
bun install
```

## ビルド

```sh
vp build
```

## テスト

```sh
vp test
```

## フォーマット

```sh
vp fmt
```

## Lint

```sh
vp lint
```

## アーキテクチャ

### DI

全モジュールは外部依存を `FlumeRuntimeDeps` 経由で受け取る。各内部モジュールは `Pick<FlumeRuntimeDeps, ...>` で必要な依存だけを宣言する。テスト時はモックを渡す。

### エラー

throw しない。`T | Error` を返し `instanceof` で判別する。カスタムエラーは `lib/errors/` に集約: `FlumeStartError`, `FlumeConnectionError`, `FlumeHttpError`, `FlumeParseError`

公開境界 (constructor / start / stop / handler 呼び出し / signal abort / log handler) からは例外を一切漏らさない。IO 境界とユーザーコールバックは `lib/utils/safe-*` 群を必ず経由する。

- 時刻: `safeNow({ deps })`
- 乱数: `safeRandom({ deps })`
- HTTP body 読取: `safeReadText({ response, log, context })`
- URL hostname (ログ用): `safeUrlHostname({ url })`
- WebSocket: `safeNewWebSocket`, `safeWsSend`, `safeWsClose`
- ユーザーコールバック (`onStatus` 等): `safeInvokeCallback({ fn, log, action })`
- AbortSignal: `safeAddAbortListener`, `safeRemoveAbortListener`
- FlumeSource.status() ループ: `safeSourceStatus`

新しい IO / ユーザーコールバックを追加するときは、対応する safe 系を必ず通すこと。生の `new WebSocket()`, `response.text()`, `signal.addEventListener` を直接呼び出すコードはレビュー時に却下する。

### 観測性

- `FlumeLogger` が全クラスに注入され `onLog` コールバックで全操作を通知
- `FlumeLogger.error()` は `level: "error"` + `error` フィールドでハンドラに流れる。ユーザーが `onLog` 内で Sentry / Datadog / console など任意の送信先に振り分ける
- 外部サービスへの依存なし。`FlumeRuntimeDeps` は IO 境界（fetch / WebSocket / timer / clock / random）のみ

### 型定義

`lib/types.ts` に全公開型を集約。外部境界（Discord Gateway / Slack / GitHub API）のレスポンスは全て Zod で検証 (各 Source ディレクトリの `*-schema.ts`)。

### Flume クラス

`Flume` は複数の Source を1つのストリームに統合するマージャー。ライフサイクルを型レベルで分離した FSM (`.claude/skills/software-design/references/fsm.md` のクラス分割パターン):

- `Flume` (idle) → `start()` → `FlumeRunning | FlumeStartError`
- `FlumeRunning` → `stop()` → `FlumeStopped`
- `FlumeStopped` は観測のみ (statuses)

これにより「stop 済みインスタンスへの再 start」「running 中の再 start」が型エラーになる。signal abort で `FlumeRunning` は自動的に `FlumeStopped` へ遷移する。

```ts
import { Flume, createFlumeDefaultDeps } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"

const deps = createFlumeDefaultDeps()
const flume = new Flume({
  sources: [
    new FlumeDiscordSource({ token, deps, onLog }),
    new FlumeSlackSource({ appToken, botToken, deps, onLog }),
  ],
  signal: controller.signal,
})

const running = await flume.start((event) => console.log(event))
if (running instanceof Error) throw running

await running.stop()
```

主エントリ (`@interactive-inc/flume`) は `Flume` / `FlumeRunning` / `FlumeStopped` / 型 / 共通ユーティリティのみ。Source 実装は `./discord` `./slack` `./github` の subpath エントリ (`lib/discord.ts` / `lib/slack.ts` / `lib/github.ts`) を介してのみロードされる。

### モジュール構成

- `lib/index.ts` - 公開エントリ (Flume / 型 / 共通)
- `lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` - Source 別 subpath エントリ
- `lib/flume.ts` / `lib/flume-running.ts` / `lib/flume-stopped.ts` - FSM 3 クラス
- `lib/types.ts` - 全公開型
- `lib/deps.ts` - `createFlumeDefaultDeps()` ファクトリ
- `lib/logger.ts` - 構造化ログ (`onLog` コールバックドリブン)
- `lib/reconnector.ts` / `lib/reconnect-config.ts` / `lib/schedule-reconnect.ts` - 指数バックオフ再接続と Source 共通スケジューラ
- `lib/errors/` - カスタムエラークラス
- `lib/utils/` - ユーティリティ (`isRecord`, `safeJsonParse`, `safeFetch`, `safeNow`, `safeRandom`, `safeReadText`, `safeUrlHostname`, `safeNewWebSocket`, `safeWsSend`, `safeWsClose`, `safeInvokeCallback`, `safeAddAbortListener`, `safeRemoveAbortListener`, `safeSourceStatus`, `FlumeSerialQueue`)
- `lib/discord/` - Discord Gateway プロトコル一式と高レベル Source
- `lib/slack/` - Slack Socket Mode プロトコル一式と高レベル Source
- `lib/github/` - GitHub 通知ポーリングと高レベル Source
