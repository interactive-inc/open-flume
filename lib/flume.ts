import type {
  FlumeErrorHandler,
  FlumeEvent,
  FlumeEventHandler,
  FlumeLog,
  FlumeLogHandler,
  FlumeReconnectConfig,
  FlumeReconnectOptions,
  FlumeRuntimeDeps,
  FlumeSourceStartContext,
  FlumeStreamHandler,
  FlumeStreamItem,
} from "@/types"
import type { FlumeSource } from "@/flume-source"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeSourceReuseError } from "@/errors/source-reuse-error"
import { FlumeStreamHub } from "@/flume-stream-hub"
import { FlumeLogger } from "@/logger"
import { FlumeRunning } from "@/flume-running"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"
import { FlumeSerialQueue } from "@/utils/serial-queue"

export type FlumeOptions = {
  /** 統合する Source 群 (必須) */
  sources: ReadonlyArray<FlumeSource>
  /**
   * 統合 firehose (push)。events と全レベルのログを `FlumeStreamItem` の union で受ける。
   * 使う側が `item.kind` ("event" | "log") と `item.log.level` で filter する。
   * pull 版は `FlumeRunning.stream()`
   */
  onEvent?: FlumeStreamHandler
  /** error レベルのログだけ (Sentry など error 専用の送信先用途)。firehose の error 部分の便利フィルタ */
  onError?: FlumeErrorHandler
  signal?: AbortSignal
  deps?: FlumeRuntimeDeps
  /**
   * 再接続方針。未指定 / false は無効 (接続断で source は disconnected のまま)。
   * true は既定値 (maxAttempts: Infinity / baseDelay: 1s / maxDelay: 30s)。
   * 常駐リスナー用途では明示的に有効化を推奨
   */
  reconnect?: boolean | FlumeReconnectOptions
}

type Failure = {
  name: string
  error: Error
}

type FlumeCallbackName = "onEvent" | "onError"

/**
 * 起動前の Flume。`open()` で `FlumeRunning` へ遷移する。
 * コンストラクタは単一オブジェクト `{ sources, ...options }` を受け取る (`sources` のみ必須)。
 * events も全ログも 1 本の firehose (`onEvent` push / `stream()` pull) に流れ、購読側が filter する。
 * 起動失敗時はこの open が取得した source を `stop()` してロールバックする。
 * 半接続状態で失敗した source も含むが、再利用を拒否した source は他の所有者のため停止しない。
 * `source.start()` / `source.stop()` の sync throw も `Promise.resolve().then` 経由で
 * Promise rejection に正規化して `allSettled` で捕捉する (`open()` は決して reject しない)
 */
export class Flume {
  private consumed = false

  private isAcceptingItems = true

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly sources: ReadonlyArray<FlumeSource>

  private readonly sourceEventHandler: FlumeEventHandler

  private readonly hub: FlumeStreamHub

  private readonly callbackQueue = new FlumeSerialQueue()

  constructor(private readonly options: FlumeOptions) {
    // 呼び出し側の配列 mutate で start/stop/status の対象集合がズレないよう防御コピー
    this.sources = [...options.sources]
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.hub = new FlumeStreamHub({
      onDrop: () => this.notifyStreamOverflow(),
    })
    this.log = new FlumeLogger({
      source: "flume",
      handler: this.buildLogHandler(),
      deps: this.deps,
    })
    this.sourceEventHandler = (event: FlumeEvent) => {
      // Source の queue は配送までを待つ。ユーザー callback を待つと callback 内の close と循環する。
      void this.emitItem({ kind: "event", event })
    }
  }

  /**
   * stream の buffer 溢れ通知 (stream ごとに初回 1 回)。
   * firehose (hub) には流さない — 溢れている stream 自身に還流して実イベントを
   * さらに追い出す自己破壊になるため、push の `onEvent` にだけ warn log として届ける
   */
  private notifyStreamOverflow(): void {
    const log: FlumeLog = {
      level: "warn",
      source: "flume",
      action: "stream.overflow",
      message:
        "stream buffer overflowed, dropping items (notified once per stream; see FlumeStreamOptions.buffer)",
      timestamp: safeNow({ deps: this.deps }),
    }

    void this.enqueueCallback({ kind: "log", log })
  }

  /** source が受信したログを firehose へ流す handler。error は onError にも分岐する */
  private buildLogHandler(): FlumeLogHandler {
    return (log: FlumeLog) => {
      if (!this.isAcceptingItems) return
      void this.emitItem({ kind: "log", log })

      if (log.level === "error") void this.invokeOnError(log)
    }
  }

  /**
   * firehose の単一 sink: pull の hub と push の onEvent の両方へ item を配る。
   * close 後の遅延 emit (stop 中の straggler) は push 側にも流さない (pull 側と対称にする)。
   * onEvent への転送は this.log を経由しない (経由すると log item 経路で再帰する)。
   * callback failure は reportCallbackFailure が pull hub と peer callback へ直接診断する。
   */
  private emitItem(item: FlumeStreamItem): Promise<void> {
    if (!this.isAcceptingItems || this.hub.isClosed) return Promise.resolve()

    this.hub.publish(item)
    return this.enqueueCallback(item)
  }

  private enqueueCallback(item: FlumeStreamItem, notifyPeerOnFailure = true): Promise<void> {
    const onEventResult = attempt(() => this.options.onEvent)
    if (onEventResult instanceof Error) {
      this.reportCallbackFailure("onEvent", onEventResult, notifyPeerOnFailure, item)
      return Promise.resolve()
    }
    if (!onEventResult) return Promise.resolve()

    return this.callbackQueue.add(async () => {
      const result = await attempt(() => Promise.resolve(onEventResult(item)))
      if (result instanceof Error) {
        this.reportCallbackFailure("onEvent", result, notifyPeerOnFailure, item)
      }
    })
  }

  /**
   * error log 専用 sink も callbackQueue に載せ、drain() が in-flight callback と
   * その失敗診断まで drain できるようにする
   */
  private invokeOnError(log: FlumeLog, notifyPeerOnFailure = true): Promise<void> {
    const onErrorResult = attempt(() => this.options.onError)
    if (onErrorResult instanceof Error) {
      this.reportCallbackFailure("onError", onErrorResult, notifyPeerOnFailure, {
        kind: "log",
        log,
      })
      return Promise.resolve()
    }
    if (!onErrorResult) return Promise.resolve()

    return this.callbackQueue.add(async () => {
      const result = await attempt(() => Promise.resolve(onErrorResult(log)))
      if (result instanceof Error) {
        this.reportCallbackFailure("onError", result, notifyPeerOnFailure, { kind: "log", log })
      }
    })
  }

  /**
   * 観測 sink 自身の失敗は同じ sink へ戻すと再帰するため、まず pull stream へ直接 publish し、
   * もう一方の callback にだけ転送する。peer も失敗した場合は hub-only の診断を残して終端する
   */
  private reportCallbackFailure(
    callback: FlumeCallbackName,
    error: Error,
    notifyPeer: boolean,
    failedItem?: FlumeStreamItem,
  ): void {
    const itemDetail =
      failedItem?.kind === "event"
        ? {
            itemKind: failedItem.kind,
            itemSource: failedItem.event.source,
            itemType: failedItem.event.type,
          }
        : failedItem?.kind === "log"
          ? {
              itemKind: failedItem.kind,
              itemSource: failedItem.log.source,
              itemAction: failedItem.log.action,
              itemDetail: failedItem.log.detail,
            }
          : {}
    const log: FlumeLog = {
      level: "error",
      source: "flume",
      action: `${callback}.error`,
      message: `${callback} callback failed: ${safeErrorMessage({ error })}`,
      error,
      detail: { callback, ...itemDetail },
      timestamp: safeNow({ deps: this.deps }),
    }

    this.hub.publish({ kind: "log", log })
    if (!notifyPeer) return

    if (callback === "onEvent") {
      void this.invokeOnError(log, false)
      return
    }
    void this.enqueueCallback({ kind: "log", log }, false)
  }

  async open(): Promise<FlumeRunning | FlumeStartError> {
    const guard = this.guardOpen()
    if (guard) return guard

    this.consumed = true
    this.log.info({
      action: "flume.open",
      message: `opening ${this.sources.length} source(s)`,
      detail: { count: this.sources.length },
    })

    const reconnect = this.resolveReconnect()

    const settled = await Promise.allSettled(
      this.sources.map((source) => this.safeStart(source, reconnect)),
    )

    const failures: Failure[] = []
    const ownedSources = new Set<FlumeSource>()

    for (const [index, result] of settled.entries()) {
      const source = this.sources[index]
      if (source === undefined) continue
      const name = this.sourceName(source)

      const outcome = result.status === "rejected" ? result.reason : result.value
      if (!(outcome instanceof FlumeSourceReuseError)) ownedSources.add(source)

      if (result.status === "rejected") {
        failures.push({ name, error: safeNormalizeError({ value: result.reason }) })
        continue
      }

      if (result.value instanceof Error) {
        failures.push({ name, error: result.value })
      }
    }

    if (failures.length > 0) {
      for (const failure of failures) {
        this.log.error({
          action: "flume.source.failed",
          message: `${failure.name}: ${safeErrorMessage({ error: failure.error })}`,
          error: failure.error,
          detail: { source: failure.name },
        })
      }

      await this.rollback([...ownedSources])

      const detail = failures
        .map((f) => `${f.name}: ${safeErrorMessage({ error: f.error })}`)
        .join("; ")
      const error = new FlumeStartError(
        `Flume.open: ${failures.length} source(s) failed: ${detail}`,
      )
      this.log.error({ action: "flume.open.failed", message: safeErrorMessage({ error }), error })
      this.finishFailedOpen()
      return error
    }

    if (this.isSignalAborted()) {
      await this.rollback([...ownedSources])
      const error = new FlumeStartError("Flume.open: aborted during open")
      this.log.warn({ action: "flume.open.aborted", message: safeErrorMessage({ error }), error })
      this.finishFailedOpen()
      return error
    }

    this.log.info({ action: "flume.open.complete", message: "all sources opened" })

    const running = new FlumeRunning({
      sources: this.sources,
      signal: this.options.signal,
      log: this.log,
      hub: this.hub,
      callbackQueue: this.callbackQueue,
      seal: () => {
        this.isAcceptingItems = false
      },
    })

    if (!this.isSignalAborted()) return running

    await running.close()
    const error = new FlumeStartError("Flume.open: aborted while entering running state")
    this.log.warn({ action: "flume.open.aborted", message: safeErrorMessage({ error }), error })
    return error
  }

  /** 起動失敗の戻り値は callback を待たず、配送済み診断の完了後に hub を閉じる。 */
  private finishFailedOpen(): void {
    this.isAcceptingItems = false
    void this.callbackQueue.drain().then(() => this.hub.close())
  }

  private guardOpen(): FlumeStartError | null {
    if (this.consumed) {
      const error = new FlumeStartError("Flume.open: already opened")
      this.log.warn({ action: "flume.open.refused", message: safeErrorMessage({ error }), error })
      return error
    }

    if (this.isSignalAborted()) {
      const error = new FlumeStartError("Flume.open: signal already aborted")
      this.log.warn({ action: "flume.open.refused", message: safeErrorMessage({ error }), error })
      return error
    }

    return null
  }

  /** reconnect オプションの解決。throwing getter を持つ hostile 入力でも open() を reject させない */
  private resolveReconnect(): FlumeReconnectConfig | null {
    const result = attempt(() => resolveFlumeReconnectConfig(this.options.reconnect))
    if (result instanceof Error) {
      this.log.warn({
        action: "reconnect.config.invalid",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      return null
    }
    return result
  }

  private isSignalAborted(): boolean {
    const signal = this.options.signal
    if (!signal) return false
    const result = attempt(() => signal.aborted === true)
    return result instanceof Error ? true : result
  }

  private sourceName(source: FlumeSource): string {
    const result = attempt(() => source.name)
    if (result instanceof Error) return "?"
    if (typeof result !== "string") return "?"
    return result
  }

  private safeStart(
    source: FlumeSource,
    reconnect: FlumeReconnectConfig | null,
  ): Promise<Error | null> {
    const name = this.sourceName(source)
    const ctx: FlumeSourceStartContext = {
      onEvent: this.sourceEventHandler,
      log: this.log.child(name),
      deps: this.deps,
      reconnect,
      signal: this.options.signal,
    }
    return Promise.resolve().then(() => source.start(ctx))
  }

  private async rollback(sources: ReadonlyArray<FlumeSource>): Promise<void> {
    const settled = await Promise.allSettled(
      sources.map((source) => Promise.resolve().then(() => source.stop())),
    )

    for (const [index, result] of settled.entries()) {
      const source = sources[index]
      const name = source ? this.sourceName(source) : "?"

      if (result.status === "rejected") {
        const error = safeNormalizeError({ value: result.reason })
        this.logRollbackFailure(name, error)
        continue
      }

      if (result.value instanceof Error) {
        this.logRollbackFailure(name, result.value)
      }
    }
  }

  private logRollbackFailure(name: string, error: Error): void {
    this.log.error({
      action: "flume.rollback.failed",
      message: `${name}: ${safeErrorMessage({ error })}`,
      error,
      detail: { source: name },
    })
  }
}
