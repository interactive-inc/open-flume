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

type Deps = Pick<FlumeRuntimeDeps, "now" | "setTimeout" | "clearTimeout">

type Props = {
  cron: FlumeCron
  onTick: (firedAt: number) => void
  /** cron エラーでスケジューラが恒久停止したとき (dead-but-green 防止)。stop() 起因では呼ばれない */
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
    this.arm()
    return null
  }

  stop(): void {
    this.isStoppedFlag = true
    this.clearTimer()
  }

  private arm(): void {
    if (this.isStoppedFlag) return

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

    // DST fall-back で同一壁時計分が 2 epoch に存在すると連続 2 回マッチするため 1 つ飛ばす
    const deduped = isDstDuplicateFire(firedAt, next) ? flumeCronNext(this.props.cron, next) : next
    if (deduped instanceof FlumeParseError) {
      this.halt(deduped)
      return
    }

    this.target = deduped
    this.arm()
  }

  /** cron エラーによる恒久停止。source が接続済みのまま沈黙しないよう onHalt で通知する */
  private halt(error: FlumeParseError): void {
    this.isStoppedFlag = true
    this.clearTimer()
    this.log.error({ action: "cron.no-next", message: error.message, error })

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
