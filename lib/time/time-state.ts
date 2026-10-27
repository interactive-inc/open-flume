import type {
  FlumeSourceStartContext,
  FlumeStatePersister,
  FlumeTimeSourceState,
  FlumeTimeStateStatus,
} from "@/types"
import { FlumeConnectionError } from "@/errors/connection-error"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { withDeadline } from "@/utils/with-deadline"

/** 一つの保存先を所有する。実行中一件 + 最新の待機値一件だけを保持する。 */
export class FlumeTimeState {
  private readonly loading = new AbortController()
  private readonly timeoutMs: number
  private pending: number | null = null
  private work: Promise<void> | null = null
  private inFlight = false
  private halted = false
  private closed = false
  private coalescedWrites = 0
  private failedWrites = 0
  private lastRequestedAt: number | null = null
  private lastSavedAt: number | null = null
  private error: Error | null = null

  constructor(
    private readonly props: {
      ctx: FlumeSourceStartContext
      persister: FlumeStatePersister<FlumeTimeSourceState>
      timeoutMs?: number
    },
  ) {
    const value = props.timeoutMs
    this.timeoutMs =
      typeof value === "number" && Number.isFinite(value) && value >= 1 && value <= 2_147_483_647
        ? Math.floor(value)
        : 30_000
  }

  status(): FlumeTimeStateStatus {
    return Object.freeze({
      phase: this.halted ? "halted" : this.work ? "saving" : "idle",
      pendingWrites: Number(this.inFlight) + Number(this.pending !== null),
      coalescedWrites: this.coalescedWrites,
      failedWrites: this.failedWrites,
      lastRequestedAt: this.lastRequestedAt,
      lastSavedAt: this.lastSavedAt,
      error: this.error,
    })
  }

  async load(): Promise<number | null> {
    const result = await withDeadline({
      ...this.deadline("load"),
      signal: this.loading.signal,
      run: (signal) => this.props.persister.load({ signal }),
    })
    if (this.closed) return null
    const parsed = attempt(() => {
      if (result instanceof Error || result === null) return result
      if (
        isRecord(result) &&
        typeof result.lastFiredAt === "number" &&
        Number.isFinite(result.lastFiredAt) &&
        Number.isFinite(new Date(result.lastFiredAt).getTime())
      )
        return result.lastFiredAt
      return new FlumeConnectionError("state load returned an invalid lastFiredAt", {
        errorCode: "FLUME_STATE_FAILED",
        context: { operation: "time.state.load", phase: "validate" },
      })
    })
    if (parsed instanceof Error) {
      this.props.ctx.log.warn({
        action: "time.state.load.error",
        message: safeErrorMessage({ error: parsed }),
        error: parsed,
      })
      return null
    }
    this.lastSavedAt = parsed
    return parsed
  }

  save(lastFiredAt: number): void {
    if (this.closed) return
    this.lastRequestedAt = lastFiredAt
    if (this.pending !== null) this.coalescedWrites++
    this.pending = lastFiredAt
    if (this.work || this.halted) return
    this.startSaving()
  }

  async close(): Promise<Error | null> {
    this.closed = true
    this.loading.abort()
    while (this.work) await this.work
    return this.error
  }

  private startSaving(): void {
    const done = Promise.withResolvers<void>()
    this.work = done.promise
    void this.flush().then(() => {
      this.work = null
      if (this.pending !== null && !this.halted) this.startSaving()
      done.resolve()
    })
  }

  private async flush(): Promise<void> {
    while (this.pending !== null && !this.halted) {
      const lastFiredAt = this.pending
      this.pending = null
      const result = await withDeadline({
        ...this.deadline("save"),
        run: async (signal) => {
          this.inFlight = true
          const saved = await attempt(() =>
            this.props.persister.save(Object.freeze({ lastFiredAt }), { signal }),
          )
          this.inFlight = false
          if (!(saved instanceof Error)) this.lastSavedAt = lastFiredAt
          return saved
        },
      })
      if (!(result instanceof Error)) {
        this.error = null
        continue
      }
      this.error = result
      this.failedWrites++
      // 中断非対応の IO が後から完了し得る。新しい書込を始めて順序を逆転させない。
      if (
        result instanceof FlumeConnectionError &&
        ["FLUME_TIMEOUT", "FLUME_TIMER_FAILED", "FLUME_ABORTED"].includes(result.errorCode)
      ) {
        this.halted = true
        if (this.pending === null) this.pending = lastFiredAt
      }
      this.props.ctx.log.warn({
        action: "time.state.save.error",
        message: safeErrorMessage({ error: result }),
        error: result,
        detail: { lastFiredAt, halted: this.halted },
      })
    }
  }

  private deadline(phase: "load" | "save") {
    return {
      context: () => ({ operation: `time.state.${phase}`, phase }),
      deps: this.props.ctx.deps,
      log: this.props.ctx.log,
      timeoutMs: this.timeoutMs,
    }
  }
}
