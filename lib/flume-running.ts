import type { FlumeSource, FlumeSourceStatus } from "@/types"
import { FlumeStopped } from "@/flume-stopped"

type Props = {
  sources: ReadonlyArray<FlumeSource>
  signal?: AbortSignal
}

/**
 * 稼働中の Flume。stop() で FlumeStopped へ遷移する。signal が abort されると自動 stop
 */
export class FlumeRunning {

  private stopPromise: Promise<FlumeStopped> | null = null

  private readonly onAbort: () => void

  constructor(private readonly props: Props) {
    this.onAbort = () => { void this.stop() }

    if (props.signal) {
      props.signal.addEventListener("abort", this.onAbort, { once: true })
    }
  }

  stop(): Promise<FlumeStopped> {
    if (this.stopPromise) return this.stopPromise

    this.stopPromise = this.runStop()

    return this.stopPromise
  }

  statuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.props.sources.map((source) => ({ name: source.name, status: source.status() }))
  }

  private async runStop(): Promise<FlumeStopped> {
    await Promise.allSettled(this.props.sources.map((source) => source.stop()))
    this.props.signal?.removeEventListener("abort", this.onAbort)

    return new FlumeStopped({
      finalStatuses: this.props.sources.map((source) => ({ name: source.name, status: source.status() })),
    })
  }
}
