import type {
  FlumeCatchupPolicy,
  FlumeSourceStartContext,
  FlumeStatePersister,
  FlumeTimeSourceOptions,
  FlumeTimeSourceState,
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
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"

/**
 * cron スケジュールで tick を emit する Source。外部接続を持たないため
 * 起動成功と同時に `connected` になり reconnect の対象外。
 *
 * options.statePersister + options.catchupPolicy を渡すと:
 *  1. 起動時に lastFiredAt を読み出す
 *  2. lastFiredAt から now までの過ぎ去った cron マッチを policy に従って再発火する
 *  3. 各 tick 後に lastFiredAt を保存する (best-effort, ブロックしない)
 *
 * 保存先や形式は flume の関知ではなく statePersister の実装が決める (純粋 DI)
 */
export class FlumeTimeSource extends FlumeSource {
  readonly name = "time" as const

  private scheduler: FlumeTimeScheduler | null = null

  constructor(private readonly options: FlumeTimeSourceOptions) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")

    const cron = parseCron(this.options.cron)
    if (cron instanceof FlumeParseError) {
      const error = new FlumeStartError(
        `Time source: invalid cron "${this.options.cron}": ${cron.message}`,
      )
      ctx.log.error({ action: "source.start.failed", message: safeErrorMessage({ error }), error })
      this.setStatus("disconnected", error.message)
      return error
    }

    const persister = this.options.statePersister ?? null
    const lastFiredAt = persister === null ? null : await this.loadLastFiredAt(ctx, persister)

    this.scheduler = new FlumeTimeScheduler({
      cron,
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onTick: (firedAt) => this.handleTick(ctx, firedAt, persister),
    })

    const result = this.scheduler.start()
    if (result instanceof Error) {
      const error = new FlumeStartError(`Time source: ${safeErrorMessage({ error: result })}`)
      this.setStatus("disconnected", error.message)
      return error
    }

    this.setStatus("connected")

    if (lastFiredAt !== null && persister !== null) {
      this.runCatchup({ ctx, cron, lastFiredAt, persister })
    }

    return null
  }

  protected disconnect(): void {
    this.scheduler?.stop()
    this.scheduler = null
  }

  private handleTick(
    ctx: FlumeSourceStartContext,
    firedAt: number,
    persister: FlumeStatePersister<FlumeTimeSourceState> | null,
  ): void {
    this.emitTick(ctx, firedAt)
    if (persister !== null) this.saveLastFiredAt(ctx, persister, firedAt)
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
    persister: FlumeStatePersister<FlumeTimeSourceState>
  }): void {
    const policy: FlumeCatchupPolicy = this.options.catchupPolicy ?? { mode: "off" }
    if (policy.mode === "off") return

    const matches = flumeCollectCatchupMatches({
      cron: props.cron,
      lastFiredAt: props.lastFiredAt,
      now: safeNow({ deps: props.ctx.deps }),
      policy,
    })

    if (matches instanceof FlumeParseError) {
      props.ctx.log.warn({
        action: "time.catchup.failed",
        message: matches.message,
        error: matches,
      })
      return
    }

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
    if (last !== undefined) this.saveLastFiredAt(props.ctx, props.persister, last)
  }

  private async loadLastFiredAt(
    ctx: FlumeSourceStartContext,
    persister: FlumeStatePersister<FlumeTimeSourceState>,
  ): Promise<number | null> {
    const result = await attempt(() => persister.load())
    if (result instanceof Error) {
      ctx.log.warn({
        action: "time.state.load.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      return null
    }
    if (result === null) return null
    if (typeof result.lastFiredAt !== "number" || !Number.isFinite(result.lastFiredAt)) return null
    return result.lastFiredAt
  }

  private saveLastFiredAt(
    ctx: FlumeSourceStartContext,
    persister: FlumeStatePersister<FlumeTimeSourceState>,
    lastFiredAt: number,
  ): void {
    safeInvokeCallback({
      fn: () => persister.save({ lastFiredAt }),
      onError: (error) => {
        ctx.log.warn({
          action: "time.state.save.error",
          message: safeErrorMessage({ error: safeNormalizeError({ value: error }) }),
          error,
        })
      },
    })
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
