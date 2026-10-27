import type { FlumeStreamItem, FlumeStreamOverflow } from "@/types"

type Props = {
  buffer: number
  onOverflow: FlumeStreamOverflow
  onClose: () => void
  /** 破棄ごとに通知する。first はこの stream の最初の破棄だけ true */
  onDrop?: (input: { dropped: number; first: boolean }) => void
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

  /** 最初の購読だけで使う。履歴を確定した後に観測者へ通知し、再入した新着との順序を守る。 */
  seed(items: ReadonlyArray<FlumeStreamItem>): void {
    if (this.closed) return
    const dropped = Math.max(0, items.length - this.props.buffer)
    const kept =
      this.props.onOverflow === "drop-newest"
        ? items.slice(0, this.props.buffer)
        : items.slice(dropped)
    this.items.push(...kept)
    if (dropped > 0) this.recordDrop(dropped)
  }

  push(item: FlumeStreamItem): void {
    if (this.closed) return

    const resolver = this.resolvers.shift()
    if (resolver) {
      resolver({ value: item, done: false })
      return
    }

    if (this.items.length >= this.props.buffer) {
      if (this.props.onOverflow !== "drop-newest") {
        this.items.shift()
        this.items.push(item)
      }
      this.recordDrop()
      return
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

  private recordDrop(dropped = 1): void {
    this.droppedCount += dropped
    this.props.onDrop?.({ dropped, first: this.droppedCount === dropped })
  }
}
