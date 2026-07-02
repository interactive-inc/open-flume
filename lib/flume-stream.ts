import type { FlumeStreamItem, FlumeStreamOverflow } from "@/types"

type Props = {
  buffer: number
  onOverflow: FlumeStreamOverflow
  onClose: () => void
  /** buffer 溢れで item を落とした時の通知 (この stream で最初の 1 回だけ発火する) */
  onDrop?: (input: { dropped: number }) => void
}

type Resolver = (result: IteratorResult<FlumeStreamItem>) => void

function doneResult(): IteratorResult<FlumeStreamItem> {
  return { value: undefined, done: true }
}

/**
 * push (`FlumeStreamHub.publish`) を pull (`for await`) に変換する async iterator。
 * consumer が待っていれば即 resolve、いなければ buffer に積み、溢れたら onOverflow に従う。
 * hub.close() ではバッファ済み item を吐き切ってから done へ落ちる (graceful tail drain)。
 * consumer 側の `return()` (break / 例外) はバッファを破棄して即 done になる (iterator 仕様)。
 * drop の観測は onDrop 経由 — drop 通知自体が firehose に還流して再帰しないよう初回のみ発火
 */
export class FlumeStream implements AsyncIterableIterator<FlumeStreamItem> {
  private readonly items: FlumeStreamItem[] = []

  private readonly resolvers: Resolver[] = []

  private closed = false

  private droppedCount = 0

  constructor(private readonly props: Props) {}

  push(item: FlumeStreamItem): void {
    if (this.closed) return

    const resolver = this.resolvers.shift()
    if (resolver) {
      resolver({ value: item, done: false })
      return
    }

    if (this.items.length >= this.props.buffer) {
      this.recordDrop()
      if (this.props.onOverflow === "drop-newest") return
      this.items.shift()
    }
    this.items.push(item)
  }

  close(): void {
    if (this.closed) return
    this.closed = true

    while (this.resolvers.length > 0) {
      const resolver = this.resolvers.shift()
      if (resolver) resolver(doneResult())
    }
  }

  get dropped(): number {
    return this.droppedCount
  }

  next(): Promise<IteratorResult<FlumeStreamItem>> {
    const item = this.items.shift()
    if (item !== undefined) return Promise.resolve({ value: item, done: false })

    if (this.closed) return Promise.resolve(doneResult())

    return new Promise<IteratorResult<FlumeStreamItem>>((resolve) => this.resolvers.push(resolve))
  }

  return(): Promise<IteratorResult<FlumeStreamItem>> {
    this.items.splice(0)
    this.close()
    this.props.onClose()
    return Promise.resolve(doneResult())
  }

  throw(error?: unknown): Promise<IteratorResult<FlumeStreamItem>> {
    this.items.splice(0)
    this.close()
    this.props.onClose()
    return Promise.reject(error)
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<FlumeStreamItem> {
    return this
  }

  private recordDrop(): void {
    this.droppedCount++
    if (this.droppedCount > 1) return

    this.props.onDrop?.({ dropped: 1 })
  }
}
