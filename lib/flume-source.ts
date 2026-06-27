import type { FlumeEvent, FlumeSourceStartContext, FlumeStatus } from "@/types"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeStatusEmitter } from "@/source-helpers/flume-status-emitter"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { FlumeSerialQueue } from "@/utils/serial-queue"

/**
 * 全 Source の基底クラス。protocol 固有のロジック (`connect` / `disconnect`) のみ
 * subclass に実装させ、queue / status / handler 安全呼び出しといった共通の
 * cross-cutting concern は base が引き受ける。Flume 側で全 source に注入される
 * `FlumeSourceStartContext` (handler / log / deps / onStatus / reconnect) を
 * `start()` で受け取り、subclass の `connect(ctx)` に手渡す。
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

  private ctx: FlumeSourceStartContext | null = null

  private statusEmitter: FlumeStatusEmitter | null = null

  private readonly queue = new FlumeSerialQueue()

  async start(ctx: FlumeSourceStartContext): Promise<Error | null> {
    if (this.consumed) {
      return new FlumeStartError(`${this.name}: already started`)
    }
    this.consumed = true

    this.ctx = ctx
    this.statusEmitter = new FlumeStatusEmitter({ log: ctx.log, onStatus: ctx.onStatus })

    return await this.connect(ctx)
  }

  async stop(): Promise<void> {
    if (this.stopped) return
    this.stopped = true

    // disconnect() の throw は意図的に握らず再 throw する。Flume.runClose /
    // Flume.rollback は allSettled で吸収して `flume.close.failed` /
    // `flume.rollback.failed` として firehose に流すので、ここで catch する
    // と同じ事象が二重ログになる。基底クラスの責務は queue.drain と
    // statusEmitter の最終化までで、エラー観測は Flume 側に委ねる。
    try {
      await this.disconnect()
    } finally {
      await this.queue.drain()
      this.statusEmitter?.set("disconnected")
      this.ctx = null
    }
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

  /** protocol 接続。subclass 実装 */
  protected abstract connect(ctx: FlumeSourceStartContext): Promise<Error | null>

  /** protocol 切断。subclass 実装。base が `stop()` 内で必ず呼ぶ */
  protected abstract disconnect(): Promise<void> | void
}
