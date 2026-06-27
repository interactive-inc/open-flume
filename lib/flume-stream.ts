import type { FlumeStreamItem, FlumeStreamOverflow } from "@/types"

type Props = {
  buffer: number
  onOverflow: FlumeStreamOverflow
  onClose: () => void
}

type Resolver = (result: IteratorResult<FlumeStreamItem>) => void

const DONE: IteratorResult<FlumeStreamItem> = { value: undefined, done: true }

/**
 * push (`FlumeStreamHub.publish`) を pull (`for await`) に変換する async iterator。
 * consumer が待っていれば即 resolve、いなければ buffer に積み、溢れたら onOverflow に従う。
 * `return()` (break / 例外) と hub.close() のどちらでも自然に done へ落ちる
 */
export class FlumeStream implements AsyncIterableIterator<FlumeStreamItem> {
  private readonly items: FlumeStreamItem[] = []

  private readonly resolvers: Resolver[] = []

  private closed = false

  constructor(private readonly props: Props) {}

  push(item: FlumeStreamItem): void {
    if (this.closed) return

    const resolver = this.resolvers.shift()
    if (resolver) {
      resolver({ value: item, done: false })
      return
    }

    if (this.items.length >= this.props.buffer) {
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
      if (resolver) resolver(DONE)
    }
  }

  next(): Promise<IteratorResult<FlumeStreamItem>> {
    const item = this.items.shift()
    if (item !== undefined) return Promise.resolve({ value: item, done: false })

    if (this.closed) return Promise.resolve(DONE)

    return new Promise<IteratorResult<FlumeStreamItem>>((resolve) => this.resolvers.push(resolve))
  }

  return(): Promise<IteratorResult<FlumeStreamItem>> {
    this.close()
    this.props.onClose()
    return Promise.resolve(DONE)
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<FlumeStreamItem> {
    return this
  }
}
