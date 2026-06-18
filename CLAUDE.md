# open-flume

Discord / Slack / GitHub の通知を統一的に受信するライブラリ。全ての内部動作がログで観測可能。

## セットアップ

```sh
bun install
```

## ビルド

```sh
bunx vp build
```

## 型チェック

```sh
bunx tsc --noEmit
```

## テスト

```sh
bunx vitest
```

## フォーマット

```sh
bunx vp fmt
```

## Lint

```sh
bunx vp lint
```

## アーキテクチャ

### DI

全モジュールは外部依存を `FlumeRuntimeDeps` 経由で受け取る。各内部モジュールは `Pick<FlumeRuntimeDeps, ...>` で必要な依存だけを宣言する。テスト時はモックを渡す。

### エラー

throw しない。`T | Error` を返し `instanceof` で判別する。カスタムエラーは `lib/errors/` に集約: `FlumeConnectionError`, `FlumeHttpError`, `FlumeParseError`

### 観測性

- `FlumeLogger` が全クラスに注入され `onLog` コールバックで全操作を通知
- `FlumeLogger.error()` は `level: "error"` + `error` フィールドでハンドラに流れる。ユーザーが `onLog` 内で Sentry / Datadog / console など任意の送信先に振り分ける
- 外部サービスへの依存なし。`FlumeRuntimeDeps` は IO 境界（fetch / WebSocket / timer / clock / random）のみ

### 型定義

`lib/types.ts` に全公開型を集約。`lib/schema.ts` に全 Zod スキーマを集約。外部境界（Discord Gateway / Slack / GitHub API）のレスポンスは全て Zod で検証。

### Flume クラス

`Flume` は複数の Source を1つのストリームに統合するマージャー。ライフサイクルを型レベルで分離した FSM (`.claude/skills/software-design/references/fsm.md` のクラス分割パターン):

- `Flume` (idle) → `start()` → `FlumeRunning | Error`
- `FlumeRunning` → `stop()` → `FlumeStopped`
- `FlumeStopped` は観測のみ (statuses / sources)

これにより「stop 済みインスタンスへの再 start」「running 中の再 start」が型エラーになる。signal abort で `FlumeRunning` は自動的に `FlumeStopped` へ遷移する。

```ts
import { Flume } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"

const deps = createFlumeDefaultDeps()
const flume = new Flume({
  sources: [
    new FlumeDiscordSource({ token, deps, onLog }),
    new FlumeSlackSource({ appToken, deps, onLog }),
  ],
  signal: controller.signal,
})

const running = await flume.start((event) => console.log(event))
if (running instanceof Error) throw running

await running.stop()
```

主エントリ (`@interactive-inc/flume`) は `Flume` / `FlumeRunning` / `FlumeStopped` / 型 / 共通ユーティリティのみ。Source 実装は `./discord` `./slack` `./github` の subpath を介してのみロードされる。

### モジュール構成

- `lib/index.ts` - 公開エントリ (Flume / 型 / 共通)
- `lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` - Source 別 subpath エントリ
- `lib/flume.ts` - `Flume` (idle 状態、start で FlumeRunning へ遷移)
- `lib/flume-running.ts` - `FlumeRunning` (稼働中、stop で FlumeStopped へ遷移)
- `lib/flume-stopped.ts` - `FlumeStopped` (停止済み終端)
- `lib/types.ts` - 全公開型
- `lib/deps.ts` - `createFlumeDefaultDeps()` ファクトリ
- `lib/logger.ts` - 構造化ログ (`onLog` コールバックドリブン)
- `lib/reconnector.ts` - 指数バックオフ再接続
- `lib/reconnect-config.ts` - 再接続設定の解決
- `lib/schedule-reconnect.ts` - Source 共通の再接続スケジューラ関数
- `lib/utils/` - ユーティリティ（isRecord, safeJsonParse, safeFetch, serial-queue）
- `lib/utils/serial-queue.ts` - `FlumeSerialQueue` (handler 直列実行用キュー、バックプレッシャ)
- `lib/utils/safe-fetch.ts` - `safeFetch` (try/catch + ログ済み fetch ラッパ)
- `lib/errors/` - カスタムエラークラス
- `lib/discord/discord-gateway-message-schema.ts` - Gateway メッセージ Zod スキーマ
- `lib/discord/discord-heartbeat.ts` - ハートビートタイマー + zombie 検出
- `lib/discord/discord-gateway-session.ts` - セッション状態の値オブジェクト
- `lib/discord/parse-discord-gateway-message.ts` - WS メッセージの Zod パース
- `lib/discord/discord-gateway-intents.ts` - Gateway Intent 定数
- `lib/discord/discord-gateway.ts` - Discord Gateway プロトコル
- `lib/discord/extract-discord-meta.ts` - イベントから meta を抽出する純関数
- `lib/discord/discord-source.ts` - Discord 高レベル Source
- `lib/slack/slack-envelope-schema.ts` - Envelope Zod スキーマ
- `lib/slack/slack-connection-response-schema.ts` - connections.open レスポンス Zod スキーマ
- `lib/slack/slack-seen-cache.ts` - envelope_id 重複排除キャッシュ
- `lib/slack/obtain-slack-url.ts` - Slack WebSocket URL 取得
- `lib/slack/slack-socket-mode.ts` - Slack Socket Mode プロトコル
- `lib/slack/extract-slack-meta.ts` - envelope から meta を抽出する純関数
- `lib/slack/slack-source.ts` - Slack 高レベル Source
- `lib/github/github-notification-schema.ts` - 通知 Zod スキーマ
- `lib/github/github-seen-cache.ts` - 通知の重複排除キャッシュ
- `lib/github/github-poller.ts` - GitHub API ポーリング
- `lib/github/extract-github-meta.ts` - notification から meta を抽出する純関数
- `lib/github/github-source.ts` - GitHub 高レベル Source

### コーディング規約

`.claude/rules/ts.md` に従う。

- `as` 禁止、`isRecord()` / Zod で安全に型を絞る
- `switch` 禁止、if/else if チェイン
- `const` のみ
- 分割代入禁止
- 複数引数はオブジェクト引数
- `@/` 絶対パスインポート
- 全エクスポートに `Flume` プレフィックス
- テストはソースファイルの横に `*.test.ts` で配置
- IIFE やトリッキーなコード禁止。可読性を最優先
- ユーティリティ関数は `lib/utils/` に配置
