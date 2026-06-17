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

`Flume` クラスは共有設定（onLog / deps / reconnect / signal）を持つ DI コンテナ。`flume.discord()` / `flume.slack()` / `flume.github()` で各 Source を生成する。各 Source は `Flume` を使わず直接 `new FlumeDiscordSource(options)` でも使える。

### モジュール構成

- `lib/index.ts` - 公開エントリ
- `lib/flume.ts` - `Flume` DI コンテナ
- `lib/types.ts` - 全公開型
- `lib/schema.ts` - 全 Zod スキーマ
- `lib/deps.ts` - `createFlumeDefaultDeps()` ファクトリ
- `lib/logger.ts` - 構造化ログ (`onLog` コールバックドリブン)
- `lib/reconnector.ts` - 指数バックオフ再接続
- `lib/reconnect-config.ts` - 再接続設定の解決
- `lib/utils/` - ユーティリティ関数（isRecord, safeJsonParse）
- `lib/errors/` - カスタムエラークラス
- `lib/discord/discord-heartbeat.ts` - ハートビートタイマー + zombie 検出
- `lib/discord/discord-gateway-session.ts` - セッション状態の値オブジェクト
- `lib/discord/parse-discord-gateway-message.ts` - WS メッセージの Zod パース
- `lib/discord/discord-gateway-intents.ts` - Gateway Intent 定数
- `lib/discord/discord-gateway.ts` - Discord Gateway プロトコル
- `lib/discord/discord-source.ts` - Discord 高レベル Source
- `lib/slack/obtain-slack-url.ts` - Slack WebSocket URL 取得
- `lib/slack/slack-socket-mode.ts` - Slack Socket Mode プロトコル
- `lib/slack/slack-source.ts` - Slack 高レベル Source
- `lib/github/github-seen-cache.ts` - 通知の重複排除キャッシュ
- `lib/github/github-poller.ts` - GitHub API ポーリング
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
