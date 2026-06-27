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

公開境界 (constructor / start / stop / onEvent 呼び出し / stream() / signal abort / log handler) からは例外を一切漏らさない。汎用 HOF `attempt(fn)` で try/catch をラップし `T | Error` に正規化する。同期/非同期はオーバーロードで自動判定。`safeInvokeCallback({ fn, onError })` はユーザーコールバック (`onEvent` / `onError`) を例外隔離して呼ぶ薄いラッパー (`attempt` + `onError` 通知)。firehose の `onEvent` 転送は再帰回避のため Flume 内で直接握り潰す。

- 時刻: `safeNow({ deps })` (NaN / Infinity / throw を吸収して `Date.now()` フォールバック)
- 乱数: `safeRandom({ deps })` (範囲外 / throw を吸収して `Math.random()` フォールバック)
- HTTP body 読取: `safeReadText({ response })`
- JSON: `safeJsonParse({ raw })` / `safeStringify({ value })`
- エラー正規化: `safeNormalizeError({ value })` / `safeErrorMessage({ error })`

新しい IO や user callback を追加するときは `attempt` か `safeInvokeCallback` を必ず経由する。生の try/catch を書きたい場合は `lib/utils/` に1ファイル1関数で隔離してテストを書く。

### 観測性

- `FlumeLogger` が全クラスに注入され、全操作が firehose に `{ kind: "log", log }` として流れる
- `FlumeLogger.error()` は `level: "error"` + `error` フィールドで流れる。`onError` で error だけ受けるか、firehose を `item.log.level` で filter して Sentry / Datadog / console へ振り分ける
- 外部サービスへの依存なし。`FlumeRuntimeDeps` は IO 境界（fetch / WebSocket / timer / clock / random）のみ

### 型定義

`lib/types.ts` に全公開型を集約。外部境界（Discord Gateway / Slack / GitHub API）のレスポンスは全て Zod で検証 (各 Source ディレクトリの `*-schema.ts`)。

### Flume クラス

`Flume` は複数の Source を1つのストリームに統合するマージャー。ライフサイクルを型レベルで分離した FSM (`.claude/skills/software-design/references/fsm.md` のクラス分割パターン):

- `Flume` (idle) → `open()` → `FlumeRunning | FlumeStartError`
- `FlumeRunning` → `close()` → `FlumeClosed`
- `FlumeClosed` は観測のみ (statuses)

これにより「close 済みインスタンスへの再 open」「running 中の再 open」が型エラーになる。signal abort で `FlumeRunning` は自動的に `FlumeClosed` へ遷移する。

コンストラクタは `new Flume({ sources, ...options })` の単一オブジェクト。`sources` だけ必須で他は全て optional。cross-cutting (`onEvent` / `onError` / `signal` / `deps` / `reconnect`) も同じオブジェクトで受け取り、各 Source へ `FlumeSourceStartContext` として注入する。観測は 1 本の firehose に統合: `onEvent(item)` (push) と `FlumeRunning.stream()` (pull) が同じ `FlumeStreamItem` (`{ kind: "event" } | { kind: "log" }` の union) を流し、events も全ログ (status 遷移・error・debug) も含む。使う側が `item.kind` / `item.log.level` で filter する。`onError` は error レベル log だけの便利フィルタ (Sentry 等)。公開 status callback は持たない (接続断は status log として firehose に出る)。Source コンストラクタは protocol 固有の config のみ。

```ts
import { Flume } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"

const flume = new Flume({
  sources: [new FlumeDiscordSource({ token }), new FlumeSlackSource({ appToken, botToken })],
  onEvent: (item) => {
    if (item.kind === "event") console.log(item.event)
    if (item.kind === "log" && item.log.action === "status") console.log(item.log.message)
  },
  onError: (log) => Sentry.captureException(log.error ?? new Error(log.message)),
  signal: controller.signal,
  reconnect: { maxAttempts: 10 },
})

const running = await flume.open()
if (running instanceof Error) throw running

await running.close()
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

`this.emit({...})` は base の serial queue 経由で `ctx.onEvent` に流す (`attempt` で例外隔離済み)。`this.setStatus(...)` は冪等遷移を握り潰しつつ status を log に出す (`ctx.onStatus` は内部ブリッジで optional)。subclass は try/catch を書く必要がない。

主エントリ (`@interactive-inc/flume`) は `Flume` / `FlumeRunning` / `FlumeClosed` / `FlumeSource` / 型 / 共通ユーティリティのみ。Source 実装は `./discord` `./slack` `./github` `./time` の subpath エントリ (`lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` / `lib/time.ts`) を介してのみロードされる。

### モジュール構成

- `lib/index.ts` - 公開エントリ (Flume / FlumeSource / 型 / 共通)
- `lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` / `lib/time.ts` - Source 別 subpath エントリ
- `lib/flume.ts` / `lib/flume-running.ts` / `lib/flume-closed.ts` - FSM 3 クラス
- `lib/flume-confluence.ts` - 複数 Flume を動的に増減する上位レイヤー (add/remove で firehose を1本に合流)
- `lib/flume-stream-hub.ts` / `lib/flume-stream.ts` - firehose の push→pull fan-out (`FlumeStreamItem` の async iterator)
- `lib/flume-source.ts` - 全 Source の抽象基底クラス
- `lib/types.ts` - 全公開型 (`FlumeSourceStartContext` 含む)
- `lib/deps.ts` - `createFlumeDefaultDeps()` ファクトリ
- `lib/logger.ts` - 構造化ログ (handler 経由で firehose に流す、`child(source)` で source 別 logger を派生)
- `lib/reconnector.ts` / `lib/reconnect-config.ts` / `lib/schedule-reconnect.ts` - 指数バックオフ再接続と Source 共通スケジューラ
- `lib/source-helpers/flume-status-emitter.ts` - Source 内部の status 集約 + 冪等通知
- `lib/errors/` - カスタムエラークラス
- `lib/utils/` - ユーティリティ (`attempt`, `isRecord`, `safeErrorMessage`, `safeInvokeCallback`, `safeJsonParse`, `safeNormalizeError`, `safeNow`, `safeRandom`, `safeReadText`, `safeStringify`, `FlumeSerialQueue`)
- `lib/discord/` - Discord Gateway プロトコル一式と高レベル Source
- `lib/slack/` - Slack Socket Mode プロトコル一式と高レベル Source
- `lib/github/` - GitHub 通知ポーリングと高レベル Source
- `lib/time/` - cron スケジューラ (自前 cron パーサ + next 計算) と高レベル Source
