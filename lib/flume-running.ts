import type { FlumeSourceStatus, FlumeStreamItem, FlumeStreamOptions } from "@/types"
import type { FlumeStreamHub } from "@/flume-stream-hub"
import type { FlumeSource } from "@/flume-source"
import { FlumeLogger } from "@/logger"
import { FlumeClosed, type FlumeCloseError } from "@/flume-closed"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { FlumeSerialQueue } from "@/utils/serial-queue"

type Props = {
  sources: ReadonlyArray<FlumeSource>
  signal?: AbortSignal
  log: FlumeLogger
  hub: FlumeStreamHub
  callbackQueue: FlumeSerialQueue
}

/**
 * 稼働中の Flume。close() で FlumeClosed へ遷移する。signal が abort されると自動 close。
 * 全ての source 呼び出し・signal 操作・status 読み取りを `attempt` 経由で扱い、
 * `runClose` の最外殻 try/catch で想定外の throw も `FlumeClosed` の resolve に変換する
 */
export class FlumeRunning {
  readonly kind = "running" as const

  private closePromise: Promise<FlumeClosed> | null = null

  private readonly onAbort: () => void

  constructor(private readonly props: Props) {
    this.onAbort = () => {
      this.props.log.info({ action: "flume.abort", message: "signal aborted, closing" })
      safeInvokeCallback({
        fn: () => this.close(),
        onError: (error) => {
          this.props.log.error({
            action: "flume.abort.close.failed",
            message: safeErrorMessage({ error }),
            error,
          })
        },
      })
    }

    const signal = props.signal
    if (signal) {
      const result = attempt(() => signal.addEventListener("abort", this.onAbort, { once: true }))
      if (result instanceof Error) {
        const error = safeNormalizeError({ value: result })
        props.log.error({
          action: "signal.addListener.failed",
          message: safeErrorMessage({ error }),
          error,
        })
      }
    }
  }

  close(): Promise<FlumeClosed> {
    if (this.closePromise) return this.closePromise

    this.closePromise = this.runClose()
    return this.closePromise
  }

  statuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.snapshotStatuses()
  }

  /**
   * 統合 firehose を pull で受け取る async iterator。`for await (const item of running.stream())`。
   * item は events + 全ログの union (`FlumeStreamItem`)。`item.kind` で判別する。
   * close() / signal abort で iterator は自然に終了し、`break` すると hub から自動 unsubscribe する。
   * consumer が遅れて buffer を超えたら `onOverflow` (既定 drop-oldest) に従う
   */
  stream(options?: FlumeStreamOptions): AsyncIterableIterator<FlumeStreamItem> {
    return this.props.hub.subscribe(options)
  }

  /**
   * `Flume({ signal })` に渡された AbortSignal をそのまま公開する。
   * 直接の controller を持っていない呼び出し元が `running.signal?.aborted`
   * で abort 状態を確認できる。`FlumeConfluence` 経由で開かれたグループでは
   * host の signal ではなく confluence 内部の timeout controller の signal になる点に注意
   */
  get signal(): AbortSignal | undefined {
    return this.props.signal
  }

  private async runClose(): Promise<FlumeClosed> {
    const closeErrors: FlumeCloseError[] = []

    try {
      this.props.log.info({
        action: "flume.close",
        message: `closing ${this.props.sources.length} source(s)`,
      })

      const settled = await Promise.allSettled(
        this.props.sources.map((source) => Promise.resolve().then(() => source.stop())),
      )

      for (const [index, result] of settled.entries()) {
        const source = this.props.sources[index]
        const name = source ? this.sourceName(source) : "?"

        const error =
          result.status === "rejected"
            ? safeNormalizeError({ value: result.reason })
            : result.value instanceof Error
              ? result.value
              : null
        if (error === null) continue

        closeErrors.push({ source: name, error })
        this.props.log.error({
          action: "flume.close.failed",
          message: `${name}: ${safeErrorMessage({ error })}`,
          error,
          detail: { source: name },
        })
      }

      const signal = this.props.signal
      if (signal) {
        const result = attempt(() => signal.removeEventListener("abort", this.onAbort))
        if (result instanceof Error) {
          const error = safeNormalizeError({ value: result })
          this.props.log.error({
            action: "signal.removeListener.failed",
            message: safeErrorMessage({ error }),
            error,
          })
        }
      }
      await this.props.callbackQueue.drain()
      this.props.log.info({ action: "flume.close.complete", message: "all sources closed" })
      await this.props.callbackQueue.drain()

      this.props.hub.close()
      return new FlumeClosed({ finalStatuses: this.snapshotStatuses(), closeErrors })
    } catch (err) {
      const error = safeNormalizeError({ value: err })
      this.props.log.error({
        action: "flume.close.unhandled",
        message: safeErrorMessage({ error }),
        error,
      })
      this.props.hub.close()
      return new FlumeClosed({ finalStatuses: this.snapshotStatuses(), closeErrors })
    }
  }

  private snapshotStatuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.props.sources.map((source) => {
      const name = this.sourceName(source)
      const status = attempt(() => source.status())
      if (status instanceof Error) {
        const error = safeNormalizeError({ value: status })
        this.props.log.error({
          action: "source.status.failed",
          message: `${name}: ${safeErrorMessage({ error })}`,
          error,
          detail: { source: name },
        })
        return { source: name, status: "disconnected" as const }
      }
      return { source: name, status }
    })
  }

  private sourceName(source: FlumeSource): string {
    const result = attempt(() => source.name)
    if (result instanceof Error) return "?"
    if (typeof result !== "string") return "?"
    return result
  }
}
