import type { FlumeHandler, FlumeLogHandler, FlumeRuntimeDeps, FlumeSource } from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeLogger } from "@/logger"
import { FlumeRunning } from "@/flume-running"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

type Props = {
  sources: ReadonlyArray<FlumeSource>
  signal?: AbortSignal
  onLog?: FlumeLogHandler
  deps?: FlumeRuntimeDeps
}

type Failure = {
  name: string
  error: Error
}

/**
 * 起動前の Flume。`start()` で `FlumeRunning` へ遷移する。
 * いずれかの source 失敗時は既に成功した source を全て `stop()` してロールバックし
 * `FlumeStartError` を返す。
 * `source.start()` / `source.stop()` の sync throw も `Promise.resolve().then` 経由で
 * Promise rejection に正規化して `allSettled` で捕捉する (`start()` は決して reject しない)
 */
export class Flume {
  private consumed = false

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  constructor(private readonly props: Props) {
    this.deps = props.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "flume", handler: props.onLog, deps: this.deps })
  }

  async start(handler: FlumeHandler): Promise<FlumeRunning | FlumeStartError> {
    const guard = this.guardStart()
    if (guard) return guard

    this.consumed = true
    this.log.info({
      action: "flume.start",
      message: `starting ${this.props.sources.length} source(s)`,
      detail: { count: this.props.sources.length },
    })

    const settled = await Promise.allSettled(
      this.props.sources.map((source) => this.safeStart(source, handler)),
    )

    const failures: Failure[] = []
    const started: FlumeSource[] = []

    for (const [index, result] of settled.entries()) {
      const source = this.props.sources[index]
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
      await this.rollback(this.props.sources)
      const error = new FlumeStartError("Flume.start: aborted during start")
      this.log.warn({ action: "flume.start.aborted", message: safeErrorMessage({ error }), error })
      return error
    }

    this.log.info({ action: "flume.start.complete", message: "all sources started" })
    return new FlumeRunning({
      sources: this.props.sources,
      signal: this.props.signal,
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
    const signal = this.props.signal
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

  private safeStart(source: FlumeSource, handler: FlumeHandler): Promise<Error | null> {
    return Promise.resolve().then(() => source.start(handler, { signal: this.props.signal }))
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
