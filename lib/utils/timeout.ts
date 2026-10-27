import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import type { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"

type Props = {
  deps: Pick<FlumeRuntimeDeps, "setTimeout" | "clearTimeout">
  log: FlumeLogger
  action: string
}

type Armed = Readonly<{ id: symbol; handle: FlumeTimerHandle }>

/** タイマー一個の所有者。取消しを先に確定し、clearTimeout が失敗しても古い発火を無効にする。 */
export class FlumeTimeout {
  private armed: Armed | null = null
  private revision = 0

  constructor(private readonly props: Props) {}

  get isArmed(): boolean {
    return this.armed !== null
  }

  schedule(callback: () => void, delayMs: number): Error | null {
    const revision = ++this.revision
    this.clearTimer()
    // clear の失敗通知中に stop / schedule が再入した場合、新しい操作を優先する。
    if (revision !== this.revision) return null
    const id = Symbol()
    this.armed = Object.freeze({ id, handle: null })
    const delay = Math.min(Math.max(0, delayMs), 2_147_483_647)
    const handle = attempt(() =>
      this.props.deps.setTimeout(() => {
        if (this.armed?.id !== id) return
        this.armed = null
        const result = attempt(callback)
        if (result instanceof Error) this.report("fire.error", result)
      }, delay),
    )
    if (handle instanceof Error) {
      if (this.armed?.id === id) this.armed = null
      return handle
    }
    if (this.armed?.id === id) this.armed = Object.freeze({ id, handle })
    else {
      const cleared = attempt(() => this.props.deps.clearTimeout(handle))
      if (cleared instanceof Error) this.report("clear.error", cleared)
    }
    return null
  }

  cancel(): void {
    this.revision++
    this.clearTimer()
  }

  private clearTimer(): void {
    const armed = this.armed
    if (!armed) return
    this.armed = null
    const result = attempt(() => this.props.deps.clearTimeout(armed.handle))
    if (result instanceof Error) this.report("clear.error", result)
  }

  private report(suffix: string, error: Error): void {
    this.props.log.error({
      errorCode: "FLUME_TIMER_FAILED",
      action: `${this.props.action}.${suffix}`,
      message: safeErrorMessage({ error }),
      error,
    })
  }
}
