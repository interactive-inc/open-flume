import type {
  FlumeCatchupPolicy,
  FlumeSourceStartContext,
  FlumeTimeSourceOptions,
  FlumeTimeStateStatus,
  FlumeTimeTick,
} from "@/types"
import type { FlumeCron } from "@/time/parse-cron"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeStartError } from "@/errors/start-error"
import { parseCron } from "@/time/parse-cron"
import { FlumeTimeScheduler } from "@/time/time-scheduler"
import { flumeCollectCatchupMatches } from "@/time/time-catchup"
import { FlumeSource } from "@/flume-source"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"
import { FlumeTimeState } from "@/time/time-state"

/**
 * cron スケジュールで tick を emit する Source。外部接続を持たないため
 * 起動成功と同時に `connected` になり reconnect の対象外。
 *
 * options.statePersister + options.catchupPolicy を渡すと:
 *  1. 起動時に lastFiredAt を読み出す
 *  2. lastFiredAt から now までの過ぎ去った cron マッチを policy に従って再発火する
 *  3. 各 tick 後に lastFiredAt を直列保存する (混雑中は最新値に集約し、停止は期限内で待つ)
 *
 * 保存先や形式は flume の関知ではなく statePersister の実装が決める (純粋 DI)。
 *
 * DST 制限: fall-back (時計の巻き戻し) の二重発火は dedup で防ぐが、spring-forward
 * (時計の飛び越し) でスキップされた壁時計時刻 (例: 02:30 が存在しない日) にスケジュール
 * された job はその日は実行されない。cron は壁時計 (local time) 基準のため仕様とする
 */
export class FlumeTimeSource extends FlumeSource {
  readonly name = "time" as const

  private scheduler: FlumeTimeScheduler | null = null

  private persistence: FlumeTimeState | null = null

  constructor(private readonly options: FlumeTimeSourceOptions) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")

    const cron = parseCron(this.options.cron)
    if (cron instanceof FlumeParseError) {
      const error = new FlumeStartError(
        `Time source: invalid cron "${this.options.cron}": ${cron.message}`,
        { errorCode: "FLUME_CRON_INVALID", cause: cron },
      )
      ctx.log.error({ action: "source.start.failed", message: safeErrorMessage({ error }), error })
      this.setStatus("disconnected", error.message)
      return error
    }

    const persister = this.options.statePersister ?? null
    this.persistence =
      persister === null
        ? null
        : new FlumeTimeState({
            ctx,
            persister,
            timeoutMs: this.options.stateTimeoutMs,
          })
    const lastFiredAt = (await this.persistence?.load()) ?? null

    if (this.isStopped) return new FlumeStartError("Time source: stopped during state load")

    this.scheduler = new FlumeTimeScheduler({
      cron,
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onTick: (firedAt) => this.handleTick(ctx, firedAt),
      onHalt: (error) => this.handleSchedulerHalt(ctx, error),
    })

    // catchup (<= startedAt) とスケジューラ初回ターゲット (> startedAt) の境界を同一時刻に
    // 揃えるため、タイムスタンプは 1 回だけ取る (分境界の二重発火防止)
    const startedAt = safeNow({ deps: ctx.deps })

    const result = this.scheduler.start(startedAt)
    if (result instanceof Error) {
      const error = new FlumeStartError(`Time source: ${safeErrorMessage({ error: result })}`, {
        errorCode: result instanceof FlumeParseError ? "FLUME_CRON_INVALID" : "FLUME_TIMER_FAILED",
        cause: result,
      })
      this.setStatus("disconnected", error.message)
      return error
    }

    this.setStatus("connected")

    if (lastFiredAt !== null && persister !== null) {
      this.runCatchup({ ctx, cron, lastFiredAt, now: startedAt })
    }

    return null
  }

  protected async disconnect(): Promise<void | Error> {
    this.scheduler?.stop()
    this.scheduler = null
    const error = await this.persistence?.close()
    if (error instanceof Error) return error
  }

  /** 状態保存の混雑・失敗と最後に保存できた時刻。永続化未設定なら null。 */
  stateStatus(): FlumeTimeStateStatus | null {
    return this.persistence?.status() ?? null
  }

  private handleTick(ctx: FlumeSourceStartContext, firedAt: number): void {
    this.emitTick(ctx, firedAt)
    this.persistence?.save(firedAt)
  }

  private emitTick(ctx: FlumeSourceStartContext, firedAt: number): void {
    const tick: FlumeTimeTick = { firedAt, cron: this.options.cron }
    const custom = this.safeMessage(ctx, tick)

    this.emit({
      source: "time",
      type: typeof custom.type === "string" ? custom.type : "tick",
      data: isRecord(custom.data) ? custom.data : { firedAt, cron: this.options.cron },
      meta: this.normalizeMeta(custom.meta, this.options.cron),
      receivedAt: safeNow({ deps: ctx.deps }),
    })
  }

  private runCatchup(props: {
    ctx: FlumeSourceStartContext
    cron: FlumeCron
    lastFiredAt: number
    now: number
  }): void {
    const policy: FlumeCatchupPolicy = this.options.catchupPolicy ?? { mode: "off" }
    if (policy.mode === "off") return

    const collected = flumeCollectCatchupMatches({
      cron: props.cron,
      lastFiredAt: props.lastFiredAt,
      now: props.now,
      policy,
    })

    if (collected instanceof FlumeParseError) {
      props.ctx.log.warn({
        action: "time.catchup.failed",
        message: collected.message,
        error: collected,
      })
      return
    }

    if (collected.truncated) {
      props.ctx.log.warn({
        action: "time.catchup.truncated",
        message: "catchup exceeded the match cap; oldest missed tick(s) were dropped",
        detail: { kept: collected.matches.length, policy: policy.mode },
      })
    }

    const matches = collected.matches
    if (matches.length === 0) return

    props.ctx.log.info({
      action: "time.catchup.fired",
      message: `catchup ${matches.length} missed tick(s) since ${new Date(props.lastFiredAt).toISOString()}`,
      detail: { count: matches.length, policy: policy.mode },
    })

    for (const firedAt of matches) {
      this.emitTick(props.ctx, firedAt)
    }

    const last = matches[matches.length - 1]
    if (last !== undefined) this.persistence?.save(last)
  }

  /** スケジューラが cron エラーで恒久停止した (dead-but-green を防ぐため接続状態を落とす) */
  private handleSchedulerHalt(ctx: FlumeSourceStartContext, error: Error): void {
    ctx.log.error({
      action: "time.scheduler.halted",
      message: `scheduler halted: ${safeErrorMessage({ error })}; time source will not tick again`,
      error,
      errorCode: error instanceof FlumeParseError ? "FLUME_CRON_INVALID" : "FLUME_TIMER_FAILED",
    })
    this.setStatus("disconnected", `scheduler halted: ${safeErrorMessage({ error })}`)
  }

  private safeMessage(ctx: FlumeSourceStartContext, tick: FlumeTimeTick): Record<string, unknown> {
    const message = this.options.message
    if (!message) return {}

    const result = attempt(() => message(tick))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      ctx.log.warn({
        action: "message.error",
        message: safeErrorMessage({ error }),
        error,
        detail: { firedAt: tick.firedAt },
      })
      return {}
    }
    return isRecord(result) ? result : {}
  }

  private normalizeMeta(meta: unknown, cron: string): Record<string, string> {
    if (!isRecord(meta)) return { cron }

    const normalized: Record<string, string> = {}
    for (const key of Object.keys(meta)) {
      const value = meta[key]
      if (typeof value === "string") normalized[key] = value
    }

    if (Object.keys(normalized).length === 0) return { cron }
    return normalized
  }
}
