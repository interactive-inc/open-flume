import { captureConfiguration } from "@/utils/capture-configuration"
import type {
  FlumeDeliveryStatus,
  FlumeDiagnosticSnapshot,
  FlumeErrorHandler,
  FlumeReconnectConfig,
  FlumeReconnectOptions,
  FlumeRuntimeDeps,
  FlumeSourceStartContext,
  FlumeStreamHandler,
} from "@/types"
import type { FlumeSource } from "@/flume-source"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeSourceReuseError } from "@/errors/source-reuse-error"
import { FlumeDelivery } from "@/flume-delivery"
import { FlumeLogger } from "@/logger"
import { FlumeRunning } from "@/flume-running"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { attempt } from "@/utils/attempt"
import { FlumeTimeout } from "@/utils/timeout"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

export type FlumeOptions = Readonly<{
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
  /** push callback の待機上限 (既定 1000)。超過時は新着を落とし callback.overflow を通知する */
  callbackBuffer?: number
  signal?: AbortSignal
  /** 起動全体の期限 (ms)。未指定は無期限。期限切れでは取得済み Source を停止する。 */
  openTimeoutMs?: number
  deps?: FlumeRuntimeDeps
  /**
   * 再接続方針。未指定 / false は無効 (接続断で source は disconnected のまま)。
   * true は既定値 (maxAttempts: Infinity / baseDelay: 1s / maxDelay: 30s)。
   * 常駐リスナー用途では明示的に有効化を推奨
   */
  reconnect?: boolean | FlumeReconnectOptions
}>

type Configuration = Readonly<
  Omit<FlumeOptions, "reconnect"> & { reconnect: FlumeReconnectConfig | null }
>

type Failure = {
  name: string
  error: Error
}

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

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly sources: ReadonlyArray<FlumeSource>

  private readonly delivery: FlumeDelivery

  private readonly options: Configuration | FlumeStartError

  constructor(options: FlumeOptions) {
    this.options = captureConfiguration("Flume", () => {
      return {
        sources: Object.freeze([...options.sources]),
        onEvent: options.onEvent,
        onError: options.onError,
        callbackBuffer: options.callbackBuffer,
        signal: options.signal,
        openTimeoutMs: options.openTimeoutMs,
        deps: options.deps,
        reconnect: resolveFlumeReconnectConfig(options.reconnect),
      }
    })
    const captured: Configuration =
      this.options instanceof Error ? { sources: [], reconnect: null } : this.options
    this.sources = captured.sources
    this.deps = captured.deps ?? createFlumeDefaultDeps()
    this.delivery = new FlumeDelivery(captured, this.deps)
    this.log = new FlumeLogger({
      source: "flume",
      handler: (log) => this.delivery.publish({ kind: "log", log }),
      deps: this.deps,
    })
  }

  /** 起動失敗時や callback 停滞時も警告・エラーを取得できる。 */
  diagnostics(): FlumeDiagnosticSnapshot {
    return this.delivery.diagnostics()
  }

  deliveryStatus(): FlumeDeliveryStatus {
    return this.delivery.status()
  }

  async open(): Promise<FlumeRunning | FlumeStartError> {
    if (this.options instanceof Error) {
      this.log.error({
        action: "flume.open.refused",
        message: this.options.message,
        error: this.options,
      })
      this.finishFailedOpen()
      return this.options
    }
    const { signal, openTimeoutMs: requestedTimeout } = this.options
    const guard = this.guardOpen(signal)
    if (guard) return guard
    if (
      requestedTimeout !== undefined &&
      (typeof requestedTimeout !== "number" ||
        !Number.isFinite(requestedTimeout) ||
        requestedTimeout <= 0)
    ) {
      const error = new FlumeStartError(
        "Flume.open: openTimeoutMs must be a finite positive number",
      )
      this.log.warn({ action: "flume.open.refused", message: error.message, error })
      return error
    }
    const timeoutMs =
      requestedTimeout === undefined ? undefined : Math.min(requestedTimeout, 2_147_483_647)

    this.consumed = true
    this.log.info({
      action: "flume.open",
      message: `opening ${this.sources.length} source(s)`,
      detail: { count: this.sources.length },
    })

    const reconnect = this.options.reconnect

    const controller = new AbortController()
    let subscribed = true
    const onAbort = () => {
      if (subscribed) controller.abort()
    }
    const releaseSignal = () => {
      if (!subscribed) return
      subscribed = false
      const detached = attempt(() => signal?.removeEventListener("abort", onAbort))
      if (detached instanceof Error)
        this.log.warn({
          action: "signal.removeListener.failed",
          message: safeErrorMessage({ error: detached }),
          error: detached,
        })
    }
    const attached = attempt(() => signal?.addEventListener("abort", onAbort, { once: true }))
    if (attached instanceof Error) {
      releaseSignal()
      const error = new FlumeStartError("Flume.open: cannot observe abort signal", {
        cause: attached,
        errorCode: "FLUME_RUNTIME_FAILED",
      })
      this.log.error({ action: "signal.addListener.failed", message: error.message, error })
      this.finishFailedOpen()
      return error
    }
    const timer = new FlumeTimeout({ deps: this.deps, log: this.log, action: "flume.open.timer" })
    let openError = this.signalError(signal)
    if (!openError && timeoutMs !== undefined) {
      const scheduled = timer.schedule(() => {
        openError = new FlumeStartError(`Flume.open: timed out after ${timeoutMs}ms`, {
          errorCode: "FLUME_TIMEOUT",
        })
        controller.abort()
      }, timeoutMs)
      if (scheduled instanceof Error)
        openError = new FlumeStartError("Flume.open: failed to schedule startup timeout", {
          cause: scheduled,
          errorCode: "FLUME_TIMER_FAILED",
        })
    }
    if (openError) {
      timer.cancel()
      releaseSignal()
      this.reportInterruptedOpen(openError)
      this.finishFailedOpen()
      return openError
    }
    const settled = await Promise.allSettled(
      this.sources.map(async (source) => {
        const result = await attempt(() => this.safeStart(source, reconnect, controller.signal))
        if (result instanceof Error) controller.abort()
        return result
      }),
    )

    timer.cancel()

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

    const signalError = openError ?? this.signalError(signal)
    if (signalError) {
      releaseSignal()
      await this.rollback([...ownedSources])
      const error =
        failures.length === 0
          ? signalError
          : new FlumeStartError(signalError.message, {
              errorCode: signalError.errorCode,
              cause: new AggregateError(
                [
                  ...(signalError.cause === undefined ? [] : [signalError.cause]),
                  ...failures.map((failure) => failure.error),
                ],
                "source startup interrupted",
              ),
            })
      this.reportInterruptedOpen(error)
      this.finishFailedOpen()
      return error
    }

    if (failures.length > 0) {
      releaseSignal()
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
        {
          cause: new AggregateError(
            failures.map((failure) => failure.error),
            "source startup failed",
          ),
        },
      )
      this.log.error({ action: "flume.open.failed", message: safeErrorMessage({ error }), error })
      this.finishFailedOpen()
      return error
    }

    this.log.info({ action: "flume.open.complete", message: "all sources opened" })

    const running = new FlumeRunning({
      sources: this.sources,
      signal,
      releaseSignal,
      log: this.log,
      delivery: this.delivery,
      controller,
    })

    const finalSignalError = this.signalError(signal)
    if (!finalSignalError) return running

    const error = finalSignalError
    this.reportInterruptedOpen(error)
    await running.close()
    return error
  }

  private reportInterruptedOpen(error: FlumeStartError): void {
    if (error.errorCode === "FLUME_ABORTED")
      this.log.warn({ action: "flume.open.aborted", message: error.message, error })
    else this.log.error({ action: "flume.open.failed", message: error.message, error })
  }

  /** 起動失敗の戻り値は callback を待たず、配送済み診断の完了後に hub を閉じる。 */
  private finishFailedOpen(): void {
    this.delivery.close()
  }

  private guardOpen(signal: AbortSignal | undefined): FlumeStartError | null {
    if (this.consumed) {
      const error = new FlumeStartError("Flume.open: already opened")
      this.log.warn({ action: "flume.open.refused", message: safeErrorMessage({ error }), error })
      return error
    }

    const error = this.signalError(signal)
    if (error) {
      this.log.warn({ action: "flume.open.refused", message: safeErrorMessage({ error }), error })
      return error
    }

    return null
  }

  private signalError(signal: AbortSignal | undefined): FlumeStartError | null {
    const aborted = attempt(() => signal?.aborted === true)
    if (aborted instanceof Error)
      return new FlumeStartError("Flume.open: cannot read abort state", {
        cause: aborted,
        errorCode: "FLUME_RUNTIME_FAILED",
      })
    return aborted
      ? new FlumeStartError("Flume.open: aborted by host signal", { errorCode: "FLUME_ABORTED" })
      : null
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
    signal: AbortSignal,
  ): Promise<Error | null> {
    const name = this.sourceName(source)
    const ctx: FlumeSourceStartContext = {
      onEvent: (event) => this.delivery.publish({ kind: "event", event }),
      log: this.log.child(name),
      deps: this.deps,
      reconnect,
      signal,
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
