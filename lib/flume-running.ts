import type { FlumeSourceStatus } from "@/types"
import type { FlumeSource } from "@/flume-source"
import { FlumeLogger } from "@/logger"
import { FlumeStopped, type FlumeStopError } from "@/flume-stopped"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

type Props = {
  sources: ReadonlyArray<FlumeSource>
  signal?: AbortSignal
  log: FlumeLogger
}

/**
 * 稼働中の Flume。stop() で FlumeStopped へ遷移する。signal が abort されると自動 stop。
 * 全ての source 呼び出し・signal 操作・status 読み取りを `attempt` 経由で扱い、
 * `runStop` の最外殻 try/catch で想定外の throw も `FlumeStopped` の resolve に変換する
 */
export class FlumeRunning {
  readonly kind = "running" as const

  private stopPromise: Promise<FlumeStopped> | null = null

  private readonly onAbort: () => void

  constructor(private readonly props: Props) {
    this.onAbort = () => {
      this.props.log.info({ action: "flume.abort", message: "signal aborted, stopping" })
      safeInvokeCallback({
        fn: () => this.stop(),
        onError: (error) => {
          this.props.log.error({
            action: "flume.abort.stop.failed",
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

  stop(): Promise<FlumeStopped> {
    if (this.stopPromise) return this.stopPromise

    this.stopPromise = this.runStop()
    return this.stopPromise
  }

  statuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.snapshotStatuses()
  }

  /**
   * Host が `Flume({ signal })` で渡した AbortSignal をそのまま公開する。
   * 直接の controller を持っていない呼び出し元が `running.signal?.aborted`
   * で abort 状態を確認できる
   */
  get signal(): AbortSignal | undefined {
    return this.props.signal
  }

  private async runStop(): Promise<FlumeStopped> {
    const stopErrors: FlumeStopError[] = []

    try {
      this.props.log.info({
        action: "flume.stop",
        message: `stopping ${this.props.sources.length} source(s)`,
      })

      const settled = await Promise.allSettled(
        this.props.sources.map((source) => Promise.resolve().then(() => source.stop())),
      )

      for (const [index, result] of settled.entries()) {
        if (result.status === "rejected") {
          const source = this.props.sources[index]
          const name = source ? this.sourceName(source) : "?"
          const error = safeNormalizeError({ value: result.reason })
          stopErrors.push({ source: name, error })
          this.props.log.error({
            action: "flume.stop.failed",
            message: `${name}: ${safeErrorMessage({ error })}`,
            error,
            detail: { source: name },
          })
        }
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
      this.props.log.info({ action: "flume.stop.complete", message: "all sources stopped" })

      return new FlumeStopped({ finalStatuses: this.snapshotStatuses(), stopErrors })
    } catch (err) {
      const error = safeNormalizeError({ value: err })
      this.props.log.error({
        action: "flume.stop.unhandled",
        message: safeErrorMessage({ error }),
        error,
      })
      return new FlumeStopped({ finalStatuses: this.snapshotStatuses(), stopErrors })
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
