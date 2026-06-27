import type { FlumeStreamItem, FlumeStreamOptions } from "@/types"
import { FlumeStream } from "@/flume-stream"

const DEFAULT_BUFFER = 1000

/**
 * firehose (`onEvent` / `stream()`) の item を複数の pull consumer へ fan-out する内部ハブ。
 * subscriber が居なければ publish は実質 no-op。Flume 停止時に close() で全 stream を終端する
 */
export class FlumeStreamHub {
  private readonly streams = new Set<FlumeStream>()

  private closed = false

  publish(item: FlumeStreamItem): void {
    if (this.closed) return
    for (const stream of this.streams) stream.push(item)
  }

  subscribe(options?: FlumeStreamOptions): FlumeStream {
    const stream = new FlumeStream({
      buffer: options?.buffer ?? DEFAULT_BUFFER,
      onOverflow: options?.onOverflow ?? "drop-oldest",
      onClose: () => this.streams.delete(stream),
    })

    if (this.closed) {
      stream.close()
      return stream
    }

    this.streams.add(stream)
    return stream
  }

  close(): void {
    if (this.closed) return
    this.closed = true

    for (const stream of this.streams) stream.close()
    this.streams.clear()
  }
}
