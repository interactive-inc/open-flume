import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"

type Props = {
  maxAttempts: number
  baseDelay: number
  maxDelay: number
  deps: Pick<FlumeRuntimeDeps, "setTimeout" | "clearTimeout" | "random">
}

export class FlumeReconnector {

  attempt = 0

  aborted = false

  private timer: FlumeTimerHandle | null = null

  constructor(private readonly props: Props) {}

  schedule(fn: () => void): number {
    if (this.aborted) return 0
    if (this.attempt >= this.props.maxAttempts) return -1

    const delay = this.nextDelay()
    this.timer = this.props.deps.setTimeout(fn, delay)
    return delay
  }

  reset(): void {
    this.attempt = 0
  }

  cancel(): void {
    this.aborted = true

    if (this.timer !== null) {
      this.props.deps.clearTimeout(this.timer)
      this.timer = null
    }
  }

  private nextDelay(): number {
    const exp = Math.min(
      this.props.baseDelay * 2 ** this.attempt,
      this.props.maxDelay,
    )

    const jitter = exp * (0.5 + this.props.deps.random() * 0.5)
    this.attempt++
    return jitter
  }
}
