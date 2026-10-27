# open-flume

Discord、Slack、GitHub の通知と cron の tick を、イベントと診断の一つのストリームへ統合する TypeScript ライブラリ。通知の受信に専念し、返信や業務処理は利用側で実装する。実行時依存は Zod のみ。サービスの SDK は使わない。

ESM と TypeScript 宣言を配布する。Node.js 22 以上と Bun で検証する。通信・時刻・タイマーは `FlumeRuntimeDeps` から注入できる。GitHub と Time は WebSocket のない環境でも利用できる。

## 導入

```sh
npm add @interactive-inc/flume
```

```ts
import { Flume, FLUME_ERROR_CODES } from "@interactive-inc/flume"
import { FlumeDiscordSource } from "@interactive-inc/flume/discord"
import { FlumeSlackSource } from "@interactive-inc/flume/slack"
import { FlumeGitHubSource } from "@interactive-inc/flume/github"

const controller = new AbortController()
const flume = new Flume({
  sources: [
    new FlumeDiscordSource({ token: process.env.DISCORD_BOT_TOKEN! }),
    new FlumeSlackSource({ appToken: process.env.SLACK_APP_TOKEN! }),
    new FlumeGitHubSource({ token: process.env.GITHUB_TOKEN!, pollInterval: 60 }),
  ],
  reconnect: { maxAttempts: 10 },
  openTimeoutMs: 30_000,
  signal: controller.signal,
  onEvent: (item) => {
    if (item.kind === "event") console.log(item.event.source, item.event.type)
    if (item.kind === "log") console.log(item.log.action, item.log.message)
  },
  onError: (log) => {
    console.error(
      {
        source: log.source,
        action: log.action,
        errorCode: log.errorCode,
        guidance: log.errorCode ? FLUME_ERROR_CODES[log.errorCode] : undefined,
        detail: log.detail,
      },
      log.error,
    )
  },
})

const running = await flume.open()
if (running instanceof Error) {
  console.error(running.errorCode, running.cause, flume.diagnostics())
} else {
  // 利用終了時。callback の外から呼ぶ。
  const closed = await running.close()
  await running.drain()
  console.log(closed.errors(), running.deliveryStatus())
}
```

主エントリは Source 固有の実装を読み込まない。必要な Source を `/discord`、`/slack`、`/github`、`/time` から import する。`sources` 以外の Flume 設定は省略できる。

## 起動と終了

```text
Flume ── open() ──> FlumeRunning ── close() ──> FlumeClosed
```

- `open()` は `FlumeRunning | FlumeStartError` を返す。部分的な起動失敗では、その操作が取得した Source を停止する。元の起動エラーと後始末のエラーは `cause` の `AggregateError.errors` に保持する。
- `close()` は Source の停止を待ち、`FlumeClosed` を返す。並行した終了要求は同じ完了を共有する。`closed.statuses()` と `closed.errors()` は不変の結果で、停止失敗には source・errorCode・元の error がある。
- `open()` と `close()` は通知 callback の完了を待たない。callback 内からも `await running.close()` できる。
- `drain()` は受理した callback と、その失敗診断の配送を待つ。callback 内では自分自身を待つことになるため、必ず外から使う。完了しない callback は drain と pull stream の終端を保留させるが、Source の停止は妨げない。
- Source と Flume は再利用できない。`FlumeRunning` と `FlumeClosed` は `open()` を持たず、元の Flume 参照からの二重起動も実行時に拒否する。他の所有者が使う Source は停止しない。
- `running.statuses()` が現在の接続状態の正本。非同期に届く `status` ログは遷移履歴として扱う。配列と要素は凍結される。

`signal` の中断は明示的な close と同じ終了経路を使う。すでに中断された signal では Source を起動しない。ホスト signal は生成時に選び、open から停止まで一つの購読を保持する。

`openTimeoutMs` は起動全体の期限。未指定では全体期限なし。有限の正数を指定し、上限は 2,147,483,647 ms。期限切れでは接続を中断して後始末を待ち、`FLUME_TIMEOUT` を返す。カスタム Source の disconnect が完了しなければ、その待機は続く。

## 設定と通知の所有者

Flume、Confluence、標準 Source はコンストラクタで設定を一度取り込む。元の options を変更しても、認証情報、cron、catchup、再接続方針、通知先は変わらない。変更には新しいインスタンスを作り、必要なら Confluence.replace で交換する。Source 単体の context は start 時に取り込む。

`deps`、`signal`、`statePersister` はホストが所有する IO ポート。同じ参照を保持し、ホストのオブジェクトを凍結しない。設定の getter が失敗した場合は、起動結果に `FLUME_RUNTIME_FAILED` と元の cause を返す。

イベントとログは発行時に不変のスナップショットへ変換する。通常の object と array は入れ子までコピーし、循環・共有参照を保持する。元の入力は凍結しない。push、pull、診断履歴で同じ通知を共有しても、他の購読者が内容を書き換えることはできない。

Error とその cause、Date、Map、クラスインスタンスなどは opaque な外部値として参照を保持する。これらの内部状態の不変性は保証しない。通知データには通常の object、array、プリミティブを使い、外部値は利用側で変更しないこと。

データの getter 失敗は `source.event.snapshot.error` / `delivery.snapshot.error`、ログの取得失敗は `log.snapshot.error` として原因を残す。後続の受信は続ける。イベントの拒否数は `source.deliveryStatus().rejectedEvents`、配送境界での拒否数は `running.deliveryStatus().rejectedItems` で確認でき、過負荷の破棄とは区別する。

## 配送と過負荷

`onEvent` は `{ kind: "event", event } | { kind: "log", log }` を受け取る。`onError` は error レベルのログだけを追加で受け取る。同じ Flume の callback は一つのキューで直列に処理する。

```ts
for await (const item of running.stream({ buffer: 5000, onOverflow: "drop-newest" })) {
  if (item.kind === "event") console.log(item.event)
}
```

pull の購読者はそれぞれ独立した有限バッファを持つ。既定は 1000 件で、超過時は古い通知を破棄する。`drop-newest` では新着を破棄する。最初の購読者には、起動時から蓄積した最新 1000 件までを再生する。その後の購読者には購読開始以降の通知だけを流す。`break` は購読を解除する。

`stream()` の設定 getter が失敗した場合は `stream.options.error` を記録して既定のバッファ設定を使う。

push の `callbackBuffer` は実行中を含む通常配送枠で、既定 1000 件。イベントとログが共有する。満杯時は新着の配送を破棄し、受信を継続する。最初の超過で `callback.overflow` を通知し、診断 callback には最大二件の別枠を設ける。

`deliveryStatus()` は次を不変のスナップショットで返す。

- `pendingCallbacks`: 実行中と待機中の callback 数。
- `droppedEventCallbacks`: イベントの onEvent 配送を破棄した数。
- `droppedLogCallbacks`: ログの onEvent / onError 配送を破棄した数。同じログでも配送先が二つなら二件。
- `droppedCallbacks`: 上記二つの合計。
- `droppedDiagnosticCallbacks`: ログ配送のうち、診断専用枠でも受理できなかった数。
- `callbackFailures`: callback の失敗数。
- `droppedStreamItems`: 起動時バッファと各 pull バッファの破棄数の合計。ユニークなイベント数ではない。
- `rejectedItems`: データ取得の失敗で配送できなかった通知数。

Source 単体の非同期 callback も `eventBuffer` (既定 1000) で制限する。同期の Flume 転送の手前に別の無制限キューは作らない。破棄数は `source.deliveryStatus()` で確認できる。

配送は永続キューではない。Slack の ACK は受信への応答であり、callback の完了や業務処理の保存を保証しない。再送や再接続も含め、利用側の処理はイベント識別子で冪等にすること。

## 診断とエラー

`FLUME_ERROR_CODES` が公開コードと調査案内の正本。コードの意味は変更・再利用しない。標準の warn / error と公開エラーは errorCode を持つ。成功ログに失敗コードを付けない。

- `action`: 発生した操作。`source`: 発信元。`detail` / `context`: 失敗した段階や通信状態。
- `error` / `cause`: 元の例外。複数の失敗は AggregateError まで辿れる。
- `FlumeHttpError.status` とサービス固有の `code` は公開 errorCode と独立する。HTTP の requestId が得られた場合も保持する。
- WebSocket の予期しない切断は `ws.closed` に切断コード・reason・接続段階・CloseEvent を残す。
- `FLUME_TIMEOUT`、`FLUME_ABORTED`、`FLUME_TIMER_FAILED`、`FLUME_AUTH_FAILED` は別の原因として扱う。

`flume.diagnostics()` / `running.diagnostics()` は最新百件の warn / error を返す。callback と pull の混雑に依存しない。`total` は累積数、`evicted` は履歴上限で破棄した数。起動失敗後や終了後も読める。全履歴が必要なら利用側で継続保存する。

callback の例外や rejection は `onEvent.error` / `onError.error` として pull ともう一方の callback へ一度通知する。失敗した callback 自身へ戻して再帰させない。peer の失敗も履歴へ残す。

代表的な診断は `status`、`ws.recv`、`ws.send`、`http.request`、`http.response`、`reconnect.scheduled`、`reconnect.exhausted`。構文・schema の失敗は `ws.parse.error`、`envelope.parse.error`、`http.body.schema` などで示す。フレーム本文、認証 token、接続用 ticket のプレビューを診断へ追加しない。保持する元の例外には runtime や外部コード由来の情報が含まれ得るため、外部送信時の秘匿処理は利用側で行う。

相手が理由を返さない切断など、外部サービス内部の真因は断定できない。観測した情報を保持し、推測を原因として表示しない。利用側 callback や独自 IO が生成したエラーに機密情報を含めないこと。

公開エラーは `FlumeStartError`、`FlumeConnectionError`、`FlumeHttpError`、`FlumeParseError`、`FlumeOperationError`、`FlumeStateError`。通信・callback の失敗は結果値か診断に変換する。iterator の `throw(error)` は JavaScript の iterator 契約に従い、指定した error で reject する。

## 動的な接続グループ

```ts
import { FlumeConfluence } from "@interactive-inc/flume"

const groups = new FlumeConfluence({
  reconnect: true,
  onEvent: (item) => console.log(item.groupId, item.kind),
})
const added = await groups.add("group", [new FlumeSlackSource({ appToken })])
if (added instanceof Error) console.error(added)

const oldView = groups.observe("group")
const replaced = await groups.replace("group", [new FlumeSlackSource({ appToken: nextAppToken })])
if (replaced instanceof Error) console.error(replaced)
else console.log(replaced.previous.errors())

const removed = await groups.remove("group")
console.log(removed?.closed?.errors(), removed?.openingError)
console.log(oldView?.diagnostics())
const remaining = await groups.closeAll()
```

- `add` は `Error | null`。グループ ID の重複や起動失敗を返す。
- `replace` は新接続の起動に成功してから交換する。失敗なら旧接続を維持する。成功結果の `previous` は旧接続の FlumeClosed で、その停止エラーも確認できる。旧接続の停止に失敗しても交換自体は成立している。
- `replaceTimeoutMs` は新接続の起動期限で、既定十秒。Flume.open と同じ期限処理を使う。同じグループの並行交換は拒否する。
- `remove` は起動中の接続も中断し、停止まで待つ。結果の `closed` は稼働していた接続の終了結果、`openingError` は中断した起動の結果。存在しない ID は undefined。
- `closeAll` は終端操作。残る各グループの remove 結果を不変配列で返す。
- `observe(id)` はその接続世代の配送状態と診断を読むハンドル。remove / replace 後も取得済みハンドルは有効。新世代には再取得する。

各グループは独立した Flume。callback には groupId を付ける。停止結果の取得は callback の完了に依存しない。失敗した add / replace の原因は戻り値に保持する。

## Source ごとの契約

### Discord

Gateway v10 の JSON を使う。bot token が必要。既定 intents は Guilds、GuildMessages、DirectMessages。メッセージ本文が必要なら MessageContent を接続先の設定で許可し、`FlumeDiscordGatewayIntents` を明示指定する。

`handshakeTimeoutMs` は WebSocket の生成から READY / RESUMED までの期限で、既定三十秒。heartbeat の ACK 不着、閉じるイベントが来ないソケット、古い接続の遅延イベントを処理する。再接続では session を引き継ぎ、可能なら RESUME を使う。resume URL は wss と Discord のドメインを検証し、不正なら session を破棄して既定 Gateway に戻る。

### Slack

Socket Mode の app token を使う。受信に botToken は不要。`handshakeTimeoutMs` は URL 取得と、その後の hello 待機にそれぞれ適用し、既定三十秒。`idleTimeoutMs` でフレーム無受信の監視も設定できる。

受信 envelope を ACK し、Events API の event_id または envelope_id で重複を抑制する。キャッシュは有限であり、業務処理の exactly-once は保証しない。認証・権限の終端エラーは無限に再接続しない。429 の Retry-After を再試行の下限に使う。

### GitHub

REST の `/notifications` をポーリングする。`pollInterval` の単位は秒。`requestTimeoutMs` は fetch、本文、全ページを含む一回の取得の期限で、既定三十秒。

全ページの JSON と schema が正常なときだけ既読判定、ETag、cursor を確定する。一件でも不正なら取得全体を失敗させて再試行する。初回取得は既存通知の基準を作り、その後の新規・更新通知を発行する。重複は id と updated_at で抑制する。

認証ヘッダの送信先は同一 API の通知エンドポイントに限定し、HTTP redirect と不正なページ送り先を拒否する。サーバーのポーリング間隔・レート制限に従う。再接続設定には依存しない。

### Time

```ts
import { FlumeTimeSource } from "@interactive-inc/flume/time"

const clock = new FlumeTimeSource({
  cron: "*/15 * * * *",
  message: (tick) => ({ type: "heartbeat", data: { firedAt: tick.firedAt } }),
})
```

cron は local time の五フィールド (分・時・日・月・曜日)。ワイルドカード、範囲、ステップ、カンマ区切りを受理し、曜日の 0 と 7 は日曜。日と曜日を両方制限した場合は OR で一致させる。`parseCron`、`flumeCronNext`、`flumeCollectCatchupMatches` も公開する。

tick の `firedAt` は予定時刻。`message` は同期関数で、未指定の type / data / meta は既定値を使う。callback や戻り値の検証が失敗しても `message.error` を記録し、既定の tick と checkpoint 保存を続ける。

`statePersister` はホストの load / save ポート。`stateTimeoutMs` (既定三十秒) を各操作に適用し、引数の signal で中断を伝える。保存は直列で、実行中一件と最新の待機値一件だけを保持する。

通常の保存失敗は記録して次の保存を続ける。期限切れなどで古い書き込みが未確定なら writer を halted にし、tick は継続して追加保存を止める。終了後に古い書き込みが新しい値を上書きするのを避けるため、同じ Source では自動再開しない。

`stateStatus()` は lifecycle (open / closing / closed)、phase (idle / saving / halted)、pendingWrites、coalescedWrites、failedWrites、lastRequestedAt、lastSavedAt、error を返す。未解消の保存エラーは停止結果にも残す。persister 内から停止を await しない。

load 失敗は診断を残して catchup を省略する。`catchupPolicy` は off、lastOnly、missed。missed は既定二十四時間の範囲で最新一万件までを再発行し、打ち切りを診断する。同じ保存キーを複数の Source で共有する場合は、ホスト側で書き込み順や版比較を保護する。

## 再接続と独自 Source

`reconnect` は未指定 / false で無効、true で既定値。オブジェクトでは maxAttempts、baseDelay、maxDelay を指定する。既定は試行上限なし、初期待機一秒、待機上限三十秒。指数バックオフとジッタを使い、停止後の古い予約は発火しても接続を再生成しない。

独自 Source は `FlumeSource` を継承し、`connect(ctx)` と `disconnect()` を実装する。`this.emit(event)` で通知し、`this.setStatus(status, reason)` で状態を更新する。受信イベントは source が custom、sourceName が独自の名前となる。

```ts
import { FlumeSource, type FlumeSourceStartContext } from "@interactive-inc/flume"

class FeedSource extends FlumeSource {
  readonly name = "feed"
  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    // ctx.deps で IO を開始し、ctx.signal を渡す。
    // await 後は this.isStopped を確認してからリソースを取得する。
    this.setStatus("connected")
    return null
  }
  protected disconnect(): void {
    // 所有する IO とタイマーを解放する。失敗は Error を返してもよい。
  }
}
```

基底クラスが一度限りの起動・停止、有限配送、callback 隔離を所有する。独自の IO は `attempt` / `safeInvokeCallback` を通す。stop は callback を待たず、単体利用でも callback 外から drain を呼ぶ。connect と後片付けが両方失敗したら、両方の原因を start の結果に残す。

## 移行時の変更

- close の後に callback の完了も必要なら、callback 外で `await running.drain()` を呼ぶ。
- 設定の後書き換えは反映しない。新しい Source / Flume を生成する。
- 受け取ったイベント・ログは readonly。変更した値が必要なら利用側でコピーする。
- Confluence.replace の成功値は null から `{ previous: FlumeClosed }` に変更。remove と closeAll も終了結果を返す。
- Slack の botToken は互換性のため optional で受理するが、読取・保持・送信しない。

## 開発と検証

```sh
bun install
bun run fmt
bun run typecheck
bun run lint
bun run test
bun run build
bun run test:stress
bun audit
```

単体試験で中断、再入、遅延発火、タイマー取消し失敗、過負荷、本文停止、ページ送りの部分失敗、保存期限を検査する。ローカルの実 HTTP / WebSocket でも接続、再接続、ACK、順序、終了を検査する。外部サービスや実トークンは使わない。

test:stress は Discord と Slack をそれぞれ二百回接続し、各接続で五百件を受信する。Node.js の試験サーバーと、別プロセスの Bun 標準 fetch / WebSocket も検証する。試験全体は `bun --bun` で起動しない。実サービスでの長期運用や相手側の障害条件をすべて証明する試験ではない。

## ライセンス

MIT © Interactive Inc.
