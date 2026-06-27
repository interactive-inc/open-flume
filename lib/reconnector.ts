import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeRandom } from "@/utils/safe-random"

type Props = {
  maxAttempts: number
  baseDelay: number
  maxDelay: number
  log: FlumeLogger
  deps: Pick<FlumeRuntimeDeps, "setTimeout" | "clearTimeout" | "random">
}

/**
 * 指数バックオフ + ジッタ付きの再接続スケジューラ。
 * `schedule()` の戻り値: 正の delay = 予約成功 / -1 = 試行上限到達 / 0 = cancel 済み or 内部 timer 拒否。
 * setTimeout コールバック内のユーザー fn が throw / reject しても reconnect ループは止めない
 */
export class FlumeReconnector {
  private currentAttempt = 0

  private isAborted = false

  private timer: FlumeTimerHandle | null = null

  constructor(private readonly props: Props) {}

  get attempt(): number {
    return this.currentAttempt
  }

  get aborted(): boolean {
    return this.isAborted
  }

  schedule(fn: () => void): number {
    if (this.isAborted) return 0
    if (this.currentAttempt >= this.props.maxAttempts) return -1

    this.clearTimer()
    const delay = this.computeDelay()

    const timerResult = attempt(() => this.props.deps.setTimeout(() => this.runRetry(fn), delay))
    if (timerResult instanceof Error) {
      this.props.log.error({
        action: "reconnect.timer.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.timer = null
      return 0
    }

    this.currentAttempt++
    this.timer = timerResult
    return delay
  }

  reset(): void {
    this.currentAttempt = 0
  }

  cancel(): void {
    this.isAborted = true
    this.clearTimer()
  }

  private runRetry(fn: () => void): void {
    this.timer = null
    safeInvokeCallback({
      fn,
      onError: (error) => {
        this.props.log.error({
          action: "reconnect.timer.error",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }

  private clearTimer(): void {
    if (this.timer === null) return

    const handle = this.timer
    const result = attempt(() => this.props.deps.clearTimeout(handle))
    if (result instanceof Error) {
      this.props.log.error({
        action: "reconnect.timer.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.timer = null
  }

  private computeDelay(): number {
    const exp = Math.min(this.props.baseDelay * 2 ** this.currentAttempt, this.props.maxDelay)
    return exp * (0.5 + safeRandom({ deps: this.props.deps }) * 0.5)
  }
}
