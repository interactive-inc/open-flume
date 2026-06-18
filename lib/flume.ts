import type { FlumeHandler, FlumeSource } from "@/types"
import { FlumeRunning } from "@/flume-running"

type Props = {
  sources: ReadonlyArray<FlumeSource>
  signal?: AbortSignal
}

/**
 * 起動前の Flume。start() で FlumeRunning へ遷移する
 */
export class Flume {

  private consumed = false

  constructor(private readonly props: Props) {}

  async start(handler: FlumeHandler): Promise<FlumeRunning | Error> {
    if (this.consumed) {
      return new Error("Flume.start: already started")
    }

    if (this.props.signal?.aborted) {
      return new Error("Flume.start: signal already aborted")
    }

    this.consumed = true

    const settled = await Promise.allSettled(
      this.props.sources.map((source) => source.start(handler)),
    )

    const failures: Array<{ name: string; error: Error }> = []
    const started: FlumeSource[] = []

    for (let i = 0; i < settled.length; i++) {
      const result = settled[i]
      const source = this.props.sources[i]

      if (result === undefined || source === undefined) continue

      if (result.status === "rejected") {
        const reason = result.reason
        failures.push({ name: source.name, error: reason instanceof Error ? reason : new Error(String(reason)) })
      } else if (result.value instanceof Error) {
        failures.push({ name: source.name, error: result.value })
      } else {
        started.push(source)
      }
    }

    if (failures.length > 0) {
      await Promise.allSettled(started.map((source) => source.stop()))

      const detail = failures.map((f) => `${f.name}: ${f.error.message}`).join("; ")
      return new Error(`Flume.start: ${failures.length} source(s) failed: ${detail}`)
    }

    if (this.props.signal?.aborted) {
      await Promise.allSettled(this.props.sources.map((source) => source.stop()))

      return new Error("Flume.start: aborted during start")
    }

    return new FlumeRunning({ sources: this.props.sources, signal: this.props.signal })
  }
}
