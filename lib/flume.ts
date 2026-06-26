import type {
  FlumeEventHandler,
  FlumeLogHandler,
  FlumeReconnectConfig,
  FlumeReconnectOptions,
  FlumeRuntimeDeps,
  FlumeSourceStartContext,
  FlumeStatus,
  FlumeStatusEvent,
  FlumeStatusHandler,
} from "@/types"
import type { FlumeSource } from "@/flume-source"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeLogger } from "@/logger"
import { FlumeRunning } from "@/flume-running"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

type Options = {
  onEvent?: FlumeEventHandler
  signal?: AbortSignal
  onLog?: FlumeLogHandler
  onStatus?: FlumeStatusHandler
  deps?: FlumeRuntimeDeps
  reconnect?: FlumeReconnectOptions
}

type Failure = {
  name: string
  error: Error
}

const noopOnEvent: FlumeEventHandler = () => {}

/**
 * 起動前の Flume。`start()` で `FlumeRunning` へ遷移する。
 * 第一引数は sources、第二引数は cross-cutting options (全て optional)。
 * `onEvent` を省略するとイベントは黙って捨てられる (接続観測専用モード)。
 * いずれかの source 失敗時は既に成功した source を全て `stop()` してロールバックし
 * `FlumeStartError` を返す。
 * `source.start()` / `source.stop()` の sync throw も `Promise.resolve().then` 経由で
 * Promise rejection に正規化して `allSettled` で捕捉する (`start()` は決して reject しない)
 */
export class Flume {
  private consumed = false

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly onEvent: FlumeEventHandler

  constructor(
    private readonly sources: ReadonlyArray<FlumeSource>,
    private readonly options: Options = {},
  ) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "flume", handler: options.onLog, deps: this.deps })
    this.onEvent = options.onEvent ?? noopOnEvent
  }

  async start(): Promise<FlumeRunning | FlumeStartError> {
    const guard = this.guardStart()
    if (guard) return guard

    this.consumed = true
    this.log.info({
      action: "flume.start",
      message: `starting ${this.sources.length} source(s)`,
      detail: { count: this.sources.length },
    })

    const reconnect = resolveFlumeReconnectConfig(this.options.reconnect)

    const settled = await Promise.allSettled(
      this.sources.map((source) => this.safeStart(source, reconnect)),
    )

    const failures: Failure[] = []
    const started: FlumeSource[] = []

    for (const [index, result] of settled.entries()) {
      const source = this.sources[index]
      if (source === undefined) continue
      const name = this.sourceName(source)

      if (result.status === "rejected") {
        failures.push({ name, error: safeNormalizeError({ value: result.reason }) })
        continue
      }

      if (result.value instanceof Error) {
        failures.push({ name, error: result.value })
        continue
      }

      started.push(source)
    }

    if (failures.length > 0) {
      for (const failure of failures) {
        this.log.error({
          action: "flume.source.failed",
          message: `${failure.name}: ${safeErrorMessage({ error: failure.error })}`,
          error: failure.error,
          detail: { source: failure.name },
        })
      }

      await this.rollback(started)

      const detail = failures
        .map((f) => `${f.name}: ${safeErrorMessage({ error: f.error })}`)
        .join("; ")
      const error = new FlumeStartError(
        `Flume.start: ${failures.length} source(s) failed: ${detail}`,
      )
      this.log.error({ action: "flume.start.failed", message: safeErrorMessage({ error }), error })
      return error
    }

    if (this.isSignalAborted()) {
      await this.rollback(this.sources)
      const error = new FlumeStartError("Flume.start: aborted during start")
      this.log.warn({ action: "flume.start.aborted", message: safeErrorMessage({ error }), error })
      return error
    }

    this.log.info({ action: "flume.start.complete", message: "all sources started" })
    return new FlumeRunning({
      sources: this.sources,
      signal: this.options.signal,
      log: this.log,
    })
  }

  private guardStart(): FlumeStartError | null {
    if (this.consumed) {
      const error = new FlumeStartError("Flume.start: already started")
      this.log.warn({ action: "flume.start.refused", message: safeErrorMessage({ error }), error })
      return error
    }

    if (this.isSignalAborted()) {
      const error = new FlumeStartError("Flume.start: signal already aborted")
      this.log.warn({ action: "flume.start.refused", message: safeErrorMessage({ error }), error })
      return error
    }

    return null
  }

  private isSignalAborted(): boolean {
    const signal = this.options.signal
    if (!signal) return false
    const result = attempt(() => signal.aborted === true)
    return result instanceof Error ? true : result
  }

  private sourceName(source: FlumeSource): string {
    const result = attempt(() => source.name)
    if (result instanceof Error) return "?"
    if (typeof result !== "string") return "?"
    return result
  }

  private safeStart(
    source: FlumeSource,
    reconnect: FlumeReconnectConfig | null,
  ): Promise<Error | null> {
    const name = this.sourceName(source)
    const ctx: FlumeSourceStartContext = {
      onEvent: this.onEvent,
      log: this.log.child(name),
      deps: this.deps,
      onStatus: (status, detail) => this.notifyStatus(name, status, detail),
      reconnect,
    }
    return Promise.resolve().then(() => source.start(ctx))
  }

  private notifyStatus(name: string, status: FlumeStatus, detail?: string): void {
    const handler = this.options.onStatus
    if (!handler) return

    const event: FlumeStatusEvent =
      detail !== undefined ? { source: name, status, detail } : { source: name, status }

    safeInvokeCallback({
      fn: () => handler(event),
      onError: (error) => {
        this.log.error({
          action: "onStatus.error",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }

  private async rollback(sources: ReadonlyArray<FlumeSource>): Promise<void> {
    const settled = await Promise.allSettled(
      sources.map((source) => Promise.resolve().then(() => source.stop())),
    )

    for (const [index, result] of settled.entries()) {
      if (result.status === "rejected") {
        const source = sources[index]
        const name = source ? this.sourceName(source) : "?"
        const error = safeNormalizeError({ value: result.reason })
        this.log.error({
          action: "flume.rollback.failed",
          message: `${name}: ${safeErrorMessage({ error })}`,
          error,
          detail: { source: name },
        })
      }
    }
  }
}
