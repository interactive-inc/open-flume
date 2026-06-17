import type { FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"

type Props = {
  onSend: () => void
  onZombie: () => void
  deps: Pick<FlumeRuntimeDeps, "setInterval" | "clearInterval">
}

export class FlumeDiscordHeartbeat {

  private timer: FlumeTimerHandle | null = null

  private ackReceived = true

  constructor(private readonly props: Props) {}

  start(intervalMs: number): void {
    this.stop()
    this.ackReceived = true

    this.timer = this.props.deps.setInterval(() => {
      if (!this.ackReceived) {
        this.props.onZombie()
        return
      }
      this.ackReceived = false
      this.props.onSend()
    }, intervalMs)
  }

  stop(): void {
    if (this.timer === null) return

    this.props.deps.clearInterval(this.timer)
    this.timer = null
  }

  ack(): void {
    this.ackReceived = true
  }

  isRunning(): boolean {
    return this.timer !== null
  }
}
