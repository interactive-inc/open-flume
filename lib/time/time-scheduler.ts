import type { FlumeCron } from "@/time/parse-cron"
import type { FlumeLogHandler, FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeParseError } from "@/errors/parse-error"
import { flumeCronNext } from "@/time/cron-next"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNow } from "@/utils/safe-now"

// setTimeout は符号付き 32bit を超える遅延で即時発火する実装があるため上限でキャップし、
// 目標時刻まで何度も再武装する (長尺 cron / clock 補正にも強い)
const MAX_TIMEOUT_MS = 2_000_000_000
const FIRE_TOLERANCE_MS = 1_000

type Deps = Pick<FlumeRuntimeDeps, "now" | "setTimeout" | "clearTimeout">

type Props = {
  cron: FlumeCron
  onTick: (firedAt: number) => void
  onLog?: FlumeLogHandler
  deps: Deps
}

/**
 * cron に従って `onTick` を駆動するタイマーループ。外部接続を持たないため reconnect 不要。
 * IO 境界は全て `attempt` 経由で扱い、停止後はコールバックを発火しない
 */
export class FlumeTimeScheduler {
  private readonly log: FlumeLogger

  private isStoppedFlag = false

  private timer: FlumeTimerHandle | null = null

  private target = 0

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({
      source: "time.scheduler",
      handler: props.onLog,
      deps: props.deps,
    })
  }

  get isStopped(): boolean {
    return this.isStoppedFlag
  }

  start(): Error | null {
    const next = flumeCronNext(this.props.cron, safeNow({ deps: this.props.deps }))
    if (next instanceof FlumeParseError) {
      this.log.error({ action: "cron.no-next", message: next.message, error: next })
      return next
    }

    this.target = next
    this.log.info({
      action: "scheduler.start",
      message: `next fire at ${new Date(next).toISOString()}`,
      detail: { target: next },
    })
    this.arm()
    return null
  }

  stop(): void {
    this.isStoppedFlag = true
    this.clearTimer()
  }

  private arm(): void {
    this.clearTimer()

    const delay = Math.max(0, this.target - safeNow({ deps: this.props.deps }))
    const capped = Math.min(delay, MAX_TIMEOUT_MS)

    const result = attempt(() => this.props.deps.setTimeout(() => this.onWake(), capped))
    if (result instanceof Error) {
      this.log.error({
        action: "scheduler.arm.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      this.timer = null
      return
    }
    this.timer = result
  }

  private onWake(): void {
    this.timer = null
    if (this.isStoppedFlag) return

    const remaining = this.target - safeNow({ deps: this.props.deps })
    if (remaining > FIRE_TOLERANCE_MS) {
      this.arm()
      return
    }

    const firedAt = this.target
    safeInvokeCallback({
      fn: () => this.props.onTick(firedAt),
      onError: (error) => {
        this.log.error({
          action: "scheduler.tick.error",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })

    const next = flumeCronNext(this.props.cron, firedAt)
    if (next instanceof FlumeParseError) {
      this.log.error({ action: "cron.no-next", message: next.message, error: next })
      return
    }
    this.target = next
    this.arm()
  }

  private clearTimer(): void {
    if (this.timer === null) return

    const handle = this.timer
    const result = attempt(() => this.props.deps.clearTimeout(handle))
    if (result instanceof Error) {
      this.log.error({
        action: "scheduler.timer.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.timer = null
  }
}
