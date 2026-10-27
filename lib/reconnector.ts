import type { FlumeRuntimeDeps } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeTimeout } from "@/utils/timeout"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeRandom } from "@/utils/safe-random"

// Node / ブラウザの timer は 2^31-1 ms を超える delay を 1ms に丸めるため、その手前で clamp する
const MAX_TIMER_DELAY_MS = 2_147_483_647

type Props = {
  maxAttempts: number
  baseDelay: number
  maxDelay: number
  log: FlumeLogger
  deps: Pick<FlumeRuntimeDeps, "setTimeout" | "clearTimeout" | "random">
}

type ScheduleOptions = {
  /** 計算した backoff がこれ未満でも最低この delay を確保する (identify 間隔 / Retry-After 対応) */
  minDelayMs?: number
}

/**
 * `schedule()` の結果。in-band の数値 sentinel を使わず判別 union で返す:
 * - scheduled: timer 予約済み (delayMs 後に retry)
 * - exhausted: 試行上限到達。呼び出し側は disconnected へ落とす
 * - refused: cancel 済み or 内部 timer が予約を拒否 (deps.setTimeout throw)。retry は発火しない
 */
export type FlumeReconnectSchedule =
  | { kind: "scheduled"; delayMs: number }
  | { kind: "exhausted" }
  | { kind: "refused" }

/**
 * 指数バックオフ + ジッタ付きの再接続スケジューラ。
 * setTimeout コールバック内のユーザー fn が throw / reject しても reconnect ループは止めない。
 * timer の取消し・古い発火の無効化は FlumeTimeout が所有する。
 */
export class FlumeReconnector {
  private currentAttempt = 0

  private isAborted = false

  private readonly timer: FlumeTimeout

  constructor(private readonly props: Props) {
    this.timer = new FlumeTimeout({ ...props, action: "reconnect.timer" })
  }

  get attempt(): number {
    return this.currentAttempt
  }

  get aborted(): boolean {
    return this.isAborted
  }

  schedule(fn: () => void, options?: ScheduleOptions): FlumeReconnectSchedule {
    if (this.isAborted) return { kind: "refused" }
    if (this.currentAttempt >= this.props.maxAttempts) {
      this.timer.cancel()
      return { kind: "exhausted" }
    }

    const delay = this.computeDelay(options?.minDelayMs ?? 0)
    const previousAttempt = this.currentAttempt
    this.currentAttempt++
    const timerResult = this.timer.schedule(() => this.runRetry(fn), delay)
    if (timerResult instanceof Error) {
      this.currentAttempt = previousAttempt
      this.props.log.error({
        action: "reconnect.timer.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      return { kind: "refused" }
    }

    return { kind: "scheduled", delayMs: delay }
  }

  reset(): void {
    this.currentAttempt = 0
    this.timer.cancel()
  }

  cancel(): void {
    this.isAborted = true
    this.timer.cancel()
  }

  private runRetry(fn: () => void): void {
    if (this.isAborted) return
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

  private computeDelay(minDelayMs: number): number {
    const exp = Math.min(this.props.baseDelay * 2 ** this.currentAttempt, this.props.maxDelay)
    const jittered = exp * (0.5 + safeRandom({ deps: this.props.deps }) * 0.5)
    const floor = Number.isFinite(minDelayMs) && minDelayMs > 0 ? minDelayMs : 0

    return Math.min(Math.max(jittered, floor), MAX_TIMER_DELAY_MS)
  }
}
