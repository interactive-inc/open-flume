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

公開境界 (constructor / start / stop / onEvent 呼び出し / signal abort / log handler) からは例外を一切漏らさない。汎用 HOF `attempt(fn)` で try/catch をラップし `T | Error` に正規化する。同期/非同期はオーバーロードで自動判定。`safeInvokeCallback({ fn, onError })` はユーザーコールバック (`onEvent` / `onStatus` / `onLog`) を例外隔離して呼ぶ薄いラッパー (`attempt` + `onError` 通知)。

- 時刻: `safeNow({ deps })` (NaN / Infinity / throw を吸収して `Date.now()` フォールバック)
- 乱数: `safeRandom({ deps })` (範囲外 / throw を吸収して `Math.random()` フォールバック)
- HTTP body 読取: `safeReadText({ response })`
- JSON: `safeJsonParse({ raw })` / `safeStringify({ value })`
- エラー正規化: `safeNormalizeError({ value })` / `safeErrorMessage({ error })`

新しい IO や user callback を追加するときは `attempt` か `safeInvokeCallback` を必ず経由する。生の try/catch を書きたい場合は `lib/utils/` に1ファイル1関数で隔離してテストを書く。

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

コンストラクタは `new Flume(sources, options?)` の 2 引数。`sources` だけ必須で `options` は全フィールド optional。cross-cutting (`onEvent` / `onLog` / `onStatus` / `signal` / `deps` / `reconnect`) は全て `options` で受け取り、各 Source へ `FlumeSourceStartContext` として注入する。`onEvent` 省略時は events が黙って捨てられる (接続観測専用モード)。Source コンストラクタは protocol 固有の config のみ。

```ts
import { Flume } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"

const flume = new Flume(
  [new FlumeDiscordSource({ token }), new FlumeSlackSource({ appToken, botToken })],
  {
    onEvent: (event) => console.log(event),
    onLog,
    onStatus: (e) => console.log(`${e.source} → ${e.status}`),
    signal: controller.signal,
    reconnect: { maxAttempts: 10 },
  },
)

const running = await flume.start()
if (running instanceof Error) throw running

await running.stop()
```

### FlumeSource (抽象基底)

全 Source は `FlumeSource` を extend する。基底クラスが queue / status emitter / onEvent 安全呼び出し / consumed・stopped guard を担当し、subclass は `connect(ctx)` と `disconnect()` の2つだけ実装する。

```ts
export class MySource extends FlumeSource {
  readonly name = "my-source"

  constructor(private readonly options: { url: string }) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")
    // protocol 接続 → 受信時に this.emit({...})、状態遷移で this.setStatus(...)
    return null
  }

  protected disconnect(): void { ... }
}
```

`this.emit({...})` は base の serial queue 経由で `ctx.onEvent` に流す (`attempt` で例外隔離済み)。`this.setStatus(...)` は冪等遷移を握り潰しつつ log + `ctx.onStatus` に流す。subclass は try/catch を書く必要がない。

主エントリ (`@interactive-inc/flume`) は `Flume` / `FlumeRunning` / `FlumeStopped` / `FlumeSource` / 型 / 共通ユーティリティのみ。Source 実装は `./discord` `./slack` `./github` の subpath エントリ (`lib/discord.ts` / `lib/slack.ts` / `lib/github.ts`) を介してのみロードされる。

### モジュール構成

- `lib/index.ts` - 公開エントリ (Flume / FlumeSource / 型 / 共通)
- `lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` - Source 別 subpath エントリ
- `lib/flume.ts` / `lib/flume-running.ts` / `lib/flume-stopped.ts` - FSM 3 クラス
- `lib/flume-source.ts` - 全 Source の抽象基底クラス
- `lib/types.ts` - 全公開型 (`FlumeSourceStartContext` 含む)
- `lib/deps.ts` - `createFlumeDefaultDeps()` ファクトリ
- `lib/logger.ts` - 構造化ログ (`onLog` コールバックドリブン、`child(source)` で source 別 logger を派生)
- `lib/reconnector.ts` / `lib/reconnect-config.ts` / `lib/schedule-reconnect.ts` - 指数バックオフ再接続と Source 共通スケジューラ
- `lib/source-helpers/flume-status-emitter.ts` - Source 内部の status 集約 + 冪等通知
- `lib/errors/` - カスタムエラークラス
- `lib/utils/` - ユーティリティ (`attempt`, `isRecord`, `safeErrorMessage`, `safeInvokeCallback`, `safeJsonParse`, `safeNormalizeError`, `safeNow`, `safeRandom`, `safeReadText`, `safeStringify`, `FlumeSerialQueue`)
- `lib/discord/` - Discord Gateway プロトコル一式と高レベル Source
- `lib/slack/` - Slack Socket Mode プロトコル一式と高レベル Source
- `lib/github/` - GitHub 通知ポーリングと高レベル Source
