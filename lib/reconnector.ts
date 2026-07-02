import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
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
 * `generation` は clearTimeout が throw して古い timer が生き残った場合でも
 * stale な発火を無視するための世代トークン
 */
export class FlumeReconnector {
  private currentAttempt = 0

  private isAborted = false

  private timer: FlumeTimerHandle | null = null

  private generation = 0

  constructor(private readonly props: Props) {}

  get attempt(): number {
    return this.currentAttempt
  }

  get aborted(): boolean {
    return this.isAborted
  }

  schedule(fn: () => void, options?: ScheduleOptions): FlumeReconnectSchedule {
    if (this.isAborted) return { kind: "refused" }
    if (this.currentAttempt >= this.props.maxAttempts) return { kind: "exhausted" }

    this.clearTimer()
    const delay = this.computeDelay(options?.minDelayMs ?? 0)
    const scheduledGeneration = ++this.generation

    const timerResult = attempt(() =>
      this.props.deps.setTimeout(() => this.runRetry(fn, scheduledGeneration), delay),
    )
    if (timerResult instanceof Error) {
      this.props.log.error({
        action: "reconnect.timer.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.timer = null
      return { kind: "refused" }
    }

    this.currentAttempt++
    this.timer = timerResult
    return { kind: "scheduled", delayMs: delay }
  }

  reset(): void {
    this.currentAttempt = 0
  }

  cancel(): void {
    this.isAborted = true
    this.generation++
    this.clearTimer()
  }

  private runRetry(fn: () => void, scheduledGeneration: number): void {
    if (this.isAborted) return
    if (scheduledGeneration !== this.generation) return

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

  private computeDelay(minDelayMs: number): number {
    const exp = Math.min(this.props.baseDelay * 2 ** this.currentAttempt, this.props.maxDelay)
    const jittered = exp * (0.5 + safeRandom({ deps: this.props.deps }) * 0.5)
    const floor = Number.isFinite(minDelayMs) && minDelayMs > 0 ? minDelayMs : 0

    return Math.min(Math.max(jittered, floor), MAX_TIMER_DELAY_MS)
  }
}
