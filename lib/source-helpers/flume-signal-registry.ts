import type { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

type Props = {
  log: FlumeLogger
  onAbort: () => void
}

/**
 * Source 群に共通する AbortSignal の登録・解除・abort 判定を集約する。
 * 異常な polyfill / poisoned getter / 凍結 signal を吸収するため境界呼び出しは全て try/catch で
 * 包み、失敗時はログに流して握り潰す
 */
export class FlumeSignalRegistry {
  private readonly signals: AbortSignal[] = []

  constructor(private readonly props: Props) {}

  isAnyAborted(extra?: AbortSignal): boolean {
    if (this.isAborted(extra)) return true

    for (const signal of this.signals) {
      if (this.isAborted(signal)) return true
    }

    return false
  }

  register(signal: AbortSignal | undefined): void {
    if (!signal) return

    const result = attempt(() => signal.addEventListener("abort", this.props.onAbort, { once: true }))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      this.props.log.error({
        action: "signal.addListener.failed",
        message: safeErrorMessage({ error }),
        error,
      })
      return
    }
    this.signals.push(signal)
  }

  unregisterAll(): void {
    for (const signal of this.signals) {
      const result = attempt(() => signal.removeEventListener("abort", this.props.onAbort))
      if (result instanceof Error) {
        const error = safeNormalizeError({ value: result })
        this.props.log.error({
          action: "signal.removeListener.failed",
          message: safeErrorMessage({ error }),
          error,
        })
      }
    }

    this.signals.length = 0
  }

  get size(): number {
    return this.signals.length
  }

  private isAborted(signal: AbortSignal | undefined): boolean {
    if (!signal) return false
    const result = attempt(() => signal.aborted === true)
    return result instanceof Error ? true : result
  }
}
