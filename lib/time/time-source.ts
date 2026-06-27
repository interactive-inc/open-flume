import type { FlumeSourceStartContext, FlumeTimeSourceOptions, FlumeTimeTick } from "@/types"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeStartError } from "@/errors/start-error"
import { parseCron } from "@/time/parse-cron"
import { FlumeTimeScheduler } from "@/time/time-scheduler"
import { FlumeSource } from "@/flume-source"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"

/**
 * cron スケジュールで tick を emit する Source。外部接続を持たないため
 * 起動成功と同時に `connected` になり reconnect の対象外。
 * `options.message` で tick ごとの type / data / meta を上書きできる
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

    this.scheduler = new FlumeTimeScheduler({
      cron,
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onTick: (firedAt) => this.handleTick(ctx, firedAt),
    })

    const result = this.scheduler.start()
    if (result instanceof Error) {
      const error = new FlumeStartError(`Time source: ${safeErrorMessage({ error: result })}`)
      this.setStatus("disconnected", error.message)
      return error
    }

    this.setStatus("connected")
    return null
  }

  protected disconnect(): void {
    this.scheduler?.stop()
    this.scheduler = null
  }

  private handleTick(ctx: FlumeSourceStartContext, firedAt: number): void {
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
