Discord / Slack / GitHub の通知を統一的に受信するライブラリ。全ての内部動作がログで観測可能。

## セットアップ

```sh
bun install
```

## ビルド

```sh
vp pack
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

throw しない。`T | Error` を返し `instanceof` で判別する。カスタムエラーは `lib/errors/` に集約: `FlumeStartError`, `FlumeConnectionError`, `FlumeHttpError`, `FlumeParseError`, `FlumeOperationError`, `FlumeStateError`

公開境界 (constructor / start / stop / onEvent 呼び出し / stream() / signal abort / log handler) からは例外を一切漏らさない。汎用 HOF `attempt(fn)` で try/catch をラップし `T | Error` に正規化する。同期/非同期はオーバーロードで自動判定。`safeInvokeCallback({ fn, onError })` はユーザーコールバック (`onEvent` / `onError`) を例外隔離して呼ぶ薄いラッパー (`attempt` + `onError` 通知)。`FlumeDelivery` がコールバックを直列化し、失敗を `onEvent.error` / `onError.error` として pull と peer callback に一度だけ配送する。

- 時刻: `safeNow({ deps })` (NaN / Infinity / throw を吸収して `Date.now()` フォールバック)
- 乱数: `safeRandom({ deps })` (範囲外 / throw を吸収して `Math.random()` フォールバック)
- HTTP body 読取: `safeReadText({ response, context })`
- JSON: `safeJsonParse(raw)` / `safeStringify(value)`
- エラー正規化: `safeNormalizeError({ value })` / `safeErrorMessage({ error })`

新しい IO や user callback を追加するときは `attempt` か `safeInvokeCallback` を必ず経由する。生の try/catch を書きたい場合は `lib/utils/` に1ファイル1関数で隔離してテストを書く。

### 観測性

- `FlumeLogger` が全クラスに注入され、全操作が firehose に `{ kind: "log", log }` として流れる
- `FlumeLogger.error()` は `level: "error"` + `error` フィールドで流れる。`onError` で error だけ受けるか、firehose を `item.log.level` で filter して Sentry / Datadog / console へ振り分ける
- 外部サービスへの依存なし。`FlumeRuntimeDeps` は IO 境界（fetch / WebSocket / timer / clock / random）のみ

### 型定義

`lib/types.ts` に全公開型を集約。外部境界（Discord Gateway / Slack / GitHub API）のレスポンスは全て Zod で検証 (各 Source ディレクトリの `*-schema.ts`)。

### Flume クラス

`Flume` は複数の Source を1つのストリームに統合するマージャー。公開ハンドルは起動前・稼働中・停止済みに分ける。Source は不変の状態値で idle → active → stopping → stopped を表す:

- `Flume` (idle) → `open()` → `FlumeRunning | FlumeStartError`
- `FlumeRunning` → `close()` → `FlumeClosed`
- `FlumeClosed` は観測のみ (statuses)

`open()` と `close()` は `onEvent` / `onError` の完了を待たない。`close()` は Source の停止まで待ち、コールバック内からも await できる。終了時のコールバック・失敗診断の配送まで待つ場合は、コールバック外で `await running.close(); await running.drain()` とする。pull stream は配送済みコールバックの診断まで流して終了する。

`FlumeRunning` / `FlumeClosed` に `open()` はない。ただし呼び出し元が保持する古い `Flume` 参照は型だけでは無効化できないため、実行時にも再利用を拒否する。signal abort は `close()` と同じ停止経路を使う。

コンストラクタは `new Flume({ sources, ...options })` の単一オブジェクト。`sources` だけ必須で他は全て optional。cross-cutting (`onEvent` / `onError` / `signal` / `deps` / `reconnect`) も同じオブジェクトで受け取り、各 Source へ `FlumeSourceStartContext` として注入する。`reconnect` は `boolean | FlumeReconnectOptions` — 未指定 / `false` は再接続無効 (接続断で source は disconnected のまま)、`true` で既定値、オブジェクトはフィールドごとに検証され不正値 (NaN / 0 以下 / 非有限) は既定値へフォールバックする。常駐リスナー用途では明示的な有効化を推奨。観測は 1 本の firehose に統合: `onEvent(item)` (push) と `FlumeRunning.stream()` (pull) が同じ `FlumeStreamItem` (`{ kind: "event" } | { kind: "log" }` の union) を流し、events も全ログ (status 遷移・error・debug) も含む。使う側が `item.kind` / `item.log.level` で filter する。`onError` は error レベル log だけの便利フィルタ (Sentry 等)。公開 status callback は持たない (接続断は status log として firehose に出る)。現在の接続状態は `running.statuses()` を正本とし、非同期に配送される status log は履歴として扱う。Source は現在値を更新してから状態ログを発行する。Source コンストラクタは protocol 固有の config のみ。Source へ渡す signal は Flume が所有し、host の abort・起動失敗・close で中断する。

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

全 Source は `FlumeSource` を extend する。基底クラスが queue / status emitter / onEvent 安全呼び出し / ライフサイクルの状態遷移を担当し、subclass は `connect(ctx)` と `disconnect()` の2つだけ実装する。

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

`this.emit({...})` は同期の受け手へその場で転送し、非同期 callback だけを有限の serial queue で処理する (`attempt` で例外隔離済み)。Source 単体の eventBuffer は既定 1000。満杯時は破棄数を公開して受信を継続する。Source.stop は通知完了を待たず、callback 外の drain で待つ。`this.setStatus(...)` は冪等遷移を握り潰しつつ status を log に出す (`ctx.onStatus` は内部ブリッジで optional)。subclass は try/catch を書く必要がない。

主エントリ (`@interactive-inc/flume`) は `Flume` / `FlumeRunning` / `FlumeClosed` / `FlumeSource` / 型 / 共通ユーティリティのみ。Source 実装は `./discord` `./slack` `./github` `./time` の subpath エントリ (`lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` / `lib/time.ts`) を介してのみロードされる。

### モジュール構成

- `lib/index.ts` - 公開エントリ (Flume / FlumeSource / 型 / 共通)
- `lib/discord.ts` / `lib/slack.ts` / `lib/github.ts` / `lib/time.ts` - Source 別 subpath エントリ
- `lib/flume.ts` / `lib/flume-running.ts` / `lib/flume-closed.ts` - FSM 3 クラス
- `lib/flume-delivery.ts` - 配送・診断・有限 callback buffer と drain の所有者
- `lib/utils/timeout.ts` - タイマーの所有者。取消し失敗時も古い発火を無効にする
- `lib/errors/error-codes.ts` - 公開診断コードと対処方法
- `lib/github/github-notification-reader.ts` - 全ページの取得・検証。成功時だけ Poller が checkpoint を確定する
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

## 診断の契約

公開エラーと標準 warn / error ログは `errorCode` を持つ。`lib/errors/error-codes.ts` を正本とし、コードの意味を変更・再利用しない。action を追加したら `resolve-error-code.ts` に分類を追加する。動的 action は errorCode を明示する。登録漏れはテストで検出する。成功ログに失敗コードを付けない。HTTP status、requestId、サービス固有の code、cause を捨ててメッセージだけに置き換えない。

callbackBuffer は既定 1000。満杯時は新着 callback を破棄して受信を継続し、`callback.overflow` を一度通知する。診断用の配送は最大二件の別枠で受け付ける。`deliveryStatus()` は pending・failure・drop の累積値を不変スナップショットで返し、イベントとログの破棄数を分ける。pull の drop は各バッファでの破棄の合計であり、ユニークなイベント数ではない。`diagnostics()` は最新百件の警告・エラーと履歴上限による破棄数を callback の混雑と独立して返す。Confluence は `observe(id)` で接続世代ごとの観測ハンドルを取得できる。

`bun run test:stress` は実 HTTP / WebSocket と実 timer で二百回の接続、各五百件の配送を検査する。注入 runtime の取消し失敗・遅延発火・callback 再入は単体試験で検査する。

Time の状態保存は実行中一件と最新の待機値一件に制限する。load / save は stateTimeoutMs (既定三十秒) で期限を設ける。保存が期限切れになったら追加保存を停止し、古い書込との順序逆転を避ける。tick は継続する。stateStatus で未保存時刻・集約数・失敗・原因を公開し、未解消の保存エラーは停止結果にも含める。

Time の writer は idle / saving / halted、lifecycle は open / closing / closed の不変状態で表す。状態ごとの必要データを union 型で定義し、状態フラグを追加しない。withDeadline は対象に依存しない FlumeOperationError を返し、保存側は FlumeStateError に原因を保持する。通信の公開境界で包み直す場合も errorCode と context を保持する。
