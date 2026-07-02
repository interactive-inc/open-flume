import type { FlumeStreamItem, FlumeStreamOptions } from "@/types"
import { FlumeStream } from "@/flume-stream"

const DEFAULT_BUFFER = 1000

type Props = {
  /** stream の buffer 溢れ通知 (stream ごとに初回 1 回)。Flume が warn ログへ橋渡しする */
  onDrop?: (input: { dropped: number }) => void
}

/**
 * NaN / Infinity / 0 以下を弾いて必ず 1 以上の有限整数にする。
 * 不正値で backpressure 上限が実質無効化される (比較が常に false → 無制限成長) のを防ぐ
 */
function sanitizeBufferSize(value: number | undefined): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return DEFAULT_BUFFER
  if (value < 1) return 1
  return Math.floor(value)
}

/**
 * firehose (`onEvent` / `stream()`) の item を複数の pull consumer へ fan-out する内部ハブ。
 * subscriber が居なければ publish は実質 no-op。Flume 停止時に close() で全 stream を終端する
 */
export class FlumeStreamHub {
  private readonly streams = new Set<FlumeStream>()

  private closed = false

  constructor(private readonly props: Props = {}) {}

  get isClosed(): boolean {
    return this.closed
  }

  publish(item: FlumeStreamItem): void {
    if (this.closed) return
    for (const stream of this.streams) stream.push(item)
  }

  subscribe(options?: FlumeStreamOptions): FlumeStream {
    const stream = new FlumeStream({
      buffer: sanitizeBufferSize(options?.buffer),
      onOverflow: options?.onOverflow ?? "drop-oldest",
      onClose: () => this.streams.delete(stream),
      onDrop: this.props.onDrop,
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
