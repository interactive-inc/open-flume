import type { FlumeEvent, FlumeSourceStartContext, FlumeStatus } from "@/types"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeStatusEmitter } from "@/source-helpers/flume-status-emitter"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { FlumeSerialQueue } from "@/utils/serial-queue"

/**
 * 全 Source の基底クラス。protocol 固有のロジック (`connect` / `disconnect`) のみ
 * subclass に実装させ、queue / status / handler 安全呼び出しといった共通の
 * cross-cutting concern は base が引き受ける。Flume 側で全 source に注入される
 * `FlumeSourceStartContext` (handler / log / deps / onStatus / reconnect) を
 * `start()` で受け取り、subclass の `connect(ctx)` に手渡す。
 *
 * `ctx.signal` の購読も base が行う: connect 中に abort されたら `stop()` を発火して
 * 進行中の接続を中断する (subclass の `disconnect()` が pending な connect を解決する契約)。
 * connect 完了後の abort は Flume / FlumeRunning が runClose 経由で駆動する。
 *
 * subclass のテンプレート:
 *
 * ```ts
 * export class MySource extends FlumeSource {
 *   readonly name = "my-source"
 *
 *   constructor(private readonly options: { apiKey: string }) {
 *     super()
 *   }
 *
 *   protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
 *     // 接続して onEvent で this.emit({...}) / 状態遷移で this.setStatus(...)
 *     return null
 *   }
 *
 *   protected disconnect(): void { ... }
 * }
 * ```
 */
export abstract class FlumeSource {
  abstract readonly name: string

  private consumed = false

  private stopped = false

  private stopPromise: Promise<Error | null> | null = null

  private ctx: FlumeSourceStartContext | null = null

  private statusEmitter: FlumeStatusEmitter | null = null

  private abortHandler: (() => void) | null = null

  private readonly queue = new FlumeSerialQueue()

  async start(ctx: FlumeSourceStartContext): Promise<Error | null> {
    if (this.consumed) {
      return new FlumeStartError(`${this.name}: already started`)
    }
    if (this.stopped) {
      // stop() 済みの source を start すると「二度と stop できない接続」が生まれるため拒否する
      return new FlumeStartError(`${this.name}: already stopped`)
    }
    this.consumed = true

    this.ctx = ctx
    this.statusEmitter = new FlumeStatusEmitter({ log: ctx.log, onStatus: ctx.onStatus })

    if (this.isSignalAborted(ctx)) {
      return new FlumeStartError(`${this.name}: aborted before connect`)
    }

    this.attachAbortListener(ctx)

    const result = await attempt(async () => await this.connect(ctx))

    this.detachAbortListener(ctx)

    return result instanceof Error ? safeNormalizeError({ value: result }) : result
  }

  /**
   * 冪等。`disconnect()` の throw は捕捉して `Error` として返す (公開境界から reject しない)。
   * Flume.runClose / Flume.rollback は戻り値の Error を `flume.close.failed` /
   * `flume.rollback.failed` として firehose に流す
   */
  async stop(): Promise<Error | null> {
    if (this.stopPromise !== null) return this.stopPromise
    this.stopped = true
    this.stopPromise = this.runStop()
    return this.stopPromise
  }

  private async runStop(): Promise<Error | null> {
    const disconnectResult = await attempt(async () => {
      await this.disconnect()
    })

    await this.queue.drain()
    this.statusEmitter?.set("disconnected")
    this.ctx = null

    return disconnectResult instanceof Error ? disconnectResult : null
  }

  status(): FlumeStatus {
    return this.statusEmitter?.value ?? "disconnected"
  }

  /**
   * subclass が受信した protocol イベントを `FlumeEvent` として handler へ流す。
   * handler の throw / async reject は queue 内で catch + log し、後続を止めない
   */
  protected emit(event: FlumeEvent): void {
    const ctx = this.ctx
    if (!ctx) return

    this.queue.add(async () => {
      const result = await attempt(() => Promise.resolve(ctx.onEvent(event)))
      if (result instanceof Error) {
        ctx.log.error({
          action: "onEvent.error",
          message: safeErrorMessage({ error: result }),
          error: result,
        })
      }
    })
  }

  /**
   * subclass が protocol 状態遷移をユーザーに通知する。同一 (status, detail) の連続は冪等
   */
  protected setStatus(status: FlumeStatus, detail?: string): void {
    this.statusEmitter?.set(status, detail)
  }

  /** subclass が現在の status を読みたい場合 */
  protected get currentStatus(): FlumeStatus {
    return this.statusEmitter?.value ?? "disconnected"
  }

  /** subclass が start ctx を再参照したい場合 (stop 後は null) */
  protected get context(): FlumeSourceStartContext | null {
    return this.ctx
  }

  private isSignalAborted(ctx: FlumeSourceStartContext): boolean {
    const signal = ctx.signal
    if (!signal) return false

    const result = attempt(() => signal.aborted === true)
    return result instanceof Error ? true : result
  }

  private attachAbortListener(ctx: FlumeSourceStartContext): void {
    const signal = ctx.signal
    if (!signal) return

    const handler = () => {
      void attempt(async () => {
        await this.stop()
      })
    }

    const result = attempt(() => signal.addEventListener("abort", handler, { once: true }))
    if (result instanceof Error) {
      ctx.log.warn({
        action: "signal.addListener.failed",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      return
    }
    this.abortHandler = handler
  }

  private detachAbortListener(ctx: FlumeSourceStartContext): void {
    const handler = this.abortHandler
    if (!handler) return
    this.abortHandler = null

    const signal = ctx.signal
    if (!signal) return

    attempt(() => signal.removeEventListener("abort", handler))
  }

  /** protocol 接続。subclass 実装 */
  protected abstract connect(ctx: FlumeSourceStartContext): Promise<Error | null>

  /** protocol 切断。subclass 実装。base が `stop()` 内で必ず呼ぶ */
  protected abstract disconnect(): Promise<void> | void
}
