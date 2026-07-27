import type { FlumeCron } from "@/time/parse-cron"
import type { FlumeLogHandler, FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeParseError } from "@/errors/parse-error"
import { flumeCronNext } from "@/time/cron-next"
import { isDstDuplicateFire } from "@/time/is-dst-duplicate-fire"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNow } from "@/utils/safe-now"

// setTimeout は符号付き 32bit を超える遅延で即時発火する実装があるため上限でキャップし、
// 目標時刻まで何度も再武装する (長尺 cron / clock 補正にも強い)
const MAX_TIMEOUT_MS = 2_000_000_000
const FIRE_TOLERANCE_MS = 1_000
const DST_HISTORY_WINDOW_MS = 2 * 60 * 60 * 1000
const MAX_DST_SKIPS = 180

type Deps = Pick<FlumeRuntimeDeps, "now" | "setTimeout" | "clearTimeout">

type Props = {
  cron: FlumeCron
  onTick: (firedAt: number) => void
  /** cron / timer エラーで恒久停止したとき (dead-but-green 防止)。stop() 起因では呼ばれない */
  onHalt?: () => void
  onLog?: FlumeLogHandler
  deps: Deps
}

/**
 * cron に従って `onTick` を駆動するタイマーループ。外部接続を持たないため reconnect 不要。
 * IO 境界は全て `attempt` 経由で扱い、停止後はコールバックを発火しない。
 * sleep-wake で複数回分を取り逃した場合は 1 回だけ発火して now まで早送りする
 * (取り逃しは `scheduler.skipped` info で観測可能)
 */
export class FlumeTimeScheduler {
  private readonly log: FlumeLogger

  private isStoppedFlag = false

  private timer: FlumeTimerHandle | null = null

  private target = 0

  private readonly recentFires: number[] = []

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

  /**
   * `fromMs` より後の最初のマッチを狙う。catchup と同じ基準時刻を共有できるよう
   * 呼び出し側から 1 つのタイムスタンプを渡す (未指定なら now)
   */
  start(fromMs?: number): Error | null {
    const basis = fromMs ?? safeNow({ deps: this.props.deps })

    const next = flumeCronNext(this.props.cron, basis)
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
    return this.arm()
  }

  stop(): void {
    this.isStoppedFlag = true
    this.clearTimer()
  }

  private arm(): Error | null {
    if (this.isStoppedFlag) return null

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
      this.isStoppedFlag = true
      return result
    }
    this.timer = result
    return null
  }

  private onWake(): void {
    this.timer = null
    if (this.isStoppedFlag) return

    const remaining = this.target - safeNow({ deps: this.props.deps })
    if (remaining > FIRE_TOLERANCE_MS) {
      const error = this.arm()
      if (error instanceof Error) this.halt(error)
      return
    }

    const firedAt = this.target
    this.rememberFire(firedAt)
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

    // onTick 内から stop() された場合に orphan timer を残さない
    if (this.isStoppedFlag) return

    this.advanceAfterFire(firedAt)
  }

  /**
   * fire 後の次ターゲット計算。sleep 明けで firedAt が過去に沈んでいる場合は
   * now まで早送りして取り逃し分の replay burst を防ぐ (catchup は opt-in の別機構)
   */
  private advanceAfterFire(firedAt: number): void {
    const nowMs = safeNow({ deps: this.props.deps })

    const firstAfterFired = flumeCronNext(this.props.cron, firedAt)
    if (firstAfterFired instanceof FlumeParseError) {
      this.halt(firstAfterFired)
      return
    }

    if (firstAfterFired <= nowMs) {
      this.log.info({
        action: "scheduler.skipped",
        message: `late wake: skipped occurrence(s) between ${new Date(firedAt).toISOString()} and now`,
        detail: { latenessMs: nowMs - firedAt, firedAt },
      })
    }

    const next = firstAfterFired > nowMs ? firstAfterFired : flumeCronNext(this.props.cron, nowMs)
    if (next instanceof FlumeParseError) {
      this.halt(next)
      return
    }

    const deduped = this.skipDstDuplicates(next)
    if (deduped instanceof FlumeParseError) {
      this.halt(deduped)
      return
    }

    this.target = deduped
    const armError = this.arm()
    if (armError instanceof Error) this.halt(armError)
  }

  /** cron エラーによる恒久停止。source が接続済みのまま沈黙しないよう onHalt で通知する */
  private halt(error: Error): void {
    this.isStoppedFlag = true
    this.clearTimer()
    this.log.error({ action: "scheduler.halted", message: error.message, error })

    const onHalt = this.props.onHalt
    if (!onHalt) return

    safeInvokeCallback({
      fn: () => onHalt(),
      onError: (haltError) => {
        this.log.error({
          action: "scheduler.halt.error",
          message: safeErrorMessage({ error: haltError }),
          error: haltError,
        })
      },
    })
  }

  private rememberFire(firedAt: number): void {
    this.recentFires.push(firedAt)
    const cutoff = firedAt - DST_HISTORY_WINDOW_MS
    while (this.recentFires[0] !== undefined && this.recentFires[0] < cutoff) {
      this.recentFires.shift()
    }
  }

  private skipDstDuplicates(initialTarget: number): number | FlumeParseError {
    let target = initialTarget

    for (let iteration = 0; iteration < MAX_DST_SKIPS; iteration++) {
      if (!isDstDuplicateFire(this.recentFires, target)) return target
      const next = flumeCronNext(this.props.cron, target)
      if (next instanceof FlumeParseError) return next
      target = next
    }

    return new FlumeParseError(`cron "${this.props.cron.source}" exceeded DST duplicate bound`)
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
