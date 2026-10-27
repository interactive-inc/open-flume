import type {
  FlumeSourceStartContext,
  FlumeStatePersister,
  FlumeTimeSourceState,
  FlumeTimeStateStatus,
} from "@/types"
import { FlumeOperationError } from "@/errors/operation-error"
import { FlumeStateError } from "@/errors/state-error"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { withDeadline } from "@/utils/with-deadline"

type Lifecycle = Readonly<
  { kind: "open" } | { kind: "closing" | "closed"; done: Promise<FlumeStateError | null> }
>
type WriteAttempt = Readonly<{ lastFiredAt: number; done: Promise<void> }>
type IO = "not-started" | "pending" | "settled"
type Writer = Readonly<
  | { kind: "idle"; error: FlumeStateError | null }
  | {
      kind: "saving"
      attempt: WriteAttempt
      pending: number | null
      io: IO
      error: FlumeStateError | null
    }
  | {
      kind: "halted"
      attempt: WriteAttempt
      pending: number | null
      io: IO
      error: FlumeStateError
    }
>

/**
 * 保存先を一つ所有する。idle → saving → idle / halted。halted から保存は再開しない。
 * lifecycle は open → closing → closed。closing 以降は受理を止め、既存の saving だけを待つ。
 * 保存中は最新値一件だけ待機させる。期限切れ後の IO 完了は観測値だけを更新する。
 */
export class FlumeTimeState {
  private readonly loading = new AbortController()
  private readonly timeoutMs: number
  private lifecycle: Lifecycle = Object.freeze({ kind: "open" })
  private writer: Writer = Object.freeze({ kind: "idle", error: null })
  private coalescedWrites = 0
  private failedWrites = 0
  private lastRequestedAt: number | null = null
  private lastSavedAt: number | null = null

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
    const writer = this.writer
    return Object.freeze({
      lifecycle: this.lifecycle.kind,
      phase: writer.kind,
      pendingWrites:
        writer.kind === "idle"
          ? 0
          : Number(writer.io === "pending") + Number(writer.pending !== null),
      coalescedWrites: this.coalescedWrites,
      failedWrites: this.failedWrites,
      lastRequestedAt: this.lastRequestedAt,
      lastSavedAt: this.lastSavedAt,
      error: writer.error,
    })
  }

  async load(): Promise<number | null> {
    const result = await withDeadline({
      ...this.deadline("load"),
      signal: this.loading.signal,
      run: (signal) => this.props.persister.load({ signal }),
    })
    if (this.lifecycle.kind !== "open") return null
    const parsed = attempt(() => {
      if (result instanceof Error || result === null) return result
      if (
        isRecord(result) &&
        typeof result.lastFiredAt === "number" &&
        Number.isFinite(result.lastFiredAt) &&
        Number.isFinite(new Date(result.lastFiredAt).getTime())
      )
        return result.lastFiredAt
      return new FlumeStateError("state load returned an invalid lastFiredAt", {
        context: { operation: "time.state.load", phase: "validate" },
      })
    })
    if (parsed instanceof Error) {
      const error = this.stateError(parsed, "load")
      this.props.ctx.log.warn({ action: "time.state.load.error", message: error.message, error })
      return null
    }
    this.lastSavedAt = parsed
    return parsed
  }

  save(lastFiredAt: number): void {
    if (this.lifecycle.kind !== "open") return
    this.lastRequestedAt = lastFiredAt
    const writer = this.writer
    if (writer.kind === "idle") {
      this.startSaving(lastFiredAt)
      return
    }
    if (writer.pending !== null) this.coalescedWrites++
    this.writer = Object.freeze({ ...writer, pending: lastFiredAt })
  }

  close(): Promise<FlumeStateError | null> {
    if (this.lifecycle.kind !== "open") return this.lifecycle.done
    const done = Promise.resolve().then(async () => {
      this.loading.abort()
      while (this.writer.kind === "saving") await this.writer.attempt.done
      this.lifecycle = Object.freeze({ kind: "closed", done })
      return this.writer.error
    })
    this.lifecycle = Object.freeze({ kind: "closing", done })
    return done
  }

  private startSaving(lastFiredAt: number): void {
    const finished = Promise.withResolvers<void>()
    const write = Object.freeze({ lastFiredAt, done: finished.promise })
    this.writer = Object.freeze({
      kind: "saving",
      attempt: write,
      pending: null,
      io: "not-started",
      error: this.writer.error,
    })
    const completion = attempt(() =>
      withDeadline({
        ...this.deadline("save"),
        run: async (signal) => {
          this.updateIO(write, "pending")
          const saved = await attempt(() =>
            this.props.persister.save(Object.freeze({ lastFiredAt }), { signal }),
          )
          this.updateIO(write, "settled")
          if (!(saved instanceof Error)) {
            this.lastSavedAt = lastFiredAt
            const writer = this.writer
            if (
              writer.kind === "halted" &&
              writer.attempt === write &&
              writer.pending === lastFiredAt
            )
              this.writer = Object.freeze({ ...writer, pending: null })
          }
          return saved
        },
      }),
    )
    void completion.then((result) => {
      this.finishSaving(write, result)
      finished.resolve()
    })
  }

  private updateIO(write: WriteAttempt, io: IO): void {
    const writer = this.writer
    if (writer.kind !== "idle" && writer.attempt === write)
      this.writer = Object.freeze({ ...writer, io })
  }

  private finishSaving(write: WriteAttempt, result: void | Error): void {
    const writer = this.writer
    if (writer.kind !== "saving" || writer.attempt !== write) return
    const error = result instanceof Error ? this.stateError(result, "save") : null
    this.writer = Object.freeze({ ...writer, error })
    if (error) {
      this.failedWrites++
      // 中断された IO は未確定。古い書込と後続を競合させない。
      if (result instanceof FlumeOperationError)
        this.writer = Object.freeze({
          ...writer,
          kind: "halted",
          pending:
            writer.pending ?? (this.lastSavedAt === write.lastFiredAt ? null : write.lastFiredAt),
          error,
        })
      this.props.ctx.log.warn({
        action: "time.state.save.error",
        message: error.message,
        error,
        detail: { lastFiredAt: write.lastFiredAt, halted: this.writer.kind === "halted" },
      })
    }
    // 診断 callback が save / close に再入し得るため、更新後の状態から次の値を読む。
    if (this.writer.kind !== "saving") return
    const pending = this.writer.pending
    this.writer = Object.freeze({ kind: "idle", error: this.writer.error })
    if (pending !== null) this.startSaving(pending)
  }

  private stateError(error: Error, phase: "load" | "save"): FlumeStateError {
    if (error instanceof FlumeStateError) return error
    return new FlumeStateError(`state ${phase} failed: ${safeErrorMessage({ error })}`, {
      context: { operation: `time.state.${phase}`, phase },
      cause: error,
      errorCode: error instanceof FlumeOperationError ? error.errorCode : "FLUME_STATE_FAILED",
    })
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
