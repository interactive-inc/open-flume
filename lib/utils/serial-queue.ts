type OverflowInput = {
  dropped: number
  depth: number
}

type Props = {
  maxDepth?: number
  onOverflow?: (input: OverflowInput) => void
}

/**
 * 投入順を保ったまま task を直列実行する。各 task は前の完了を待ってから走る。
 * task が throw しても後続には伝播しない (キュー自体は止まらない)。
 * maxDepth を超えた場合は新規 task を drop し onOverflow に通知。
 * cancel() 後の add() は no-op となり drain() は即時 resolve する
 */
export class FlumeSerialQueue {
  private chain: Promise<void> = Promise.resolve()

  private depth = 0

  private cancelled = false

  constructor(private readonly props: Props = {}) {}

  add(task: () => Promise<void>): void {
    if (this.cancelled) return

    if (this.props.maxDepth !== undefined && this.depth >= this.props.maxDepth) {
      this.props.onOverflow?.({ dropped: 1, depth: this.depth })
      return
    }

    this.depth++
    this.chain = this.chain.then(async () => {
      try {
        await task()
      } catch {
        // task 例外は queue を止めない (sources 側で log.error する)
      } finally {
        this.depth--
      }
    })
  }

  async drain(): Promise<void> {
    await this.chain
  }

  cancel(): void {
    this.cancelled = true
    this.depth = 0
    this.chain = Promise.resolve()
  }

  size(): number {
    return this.depth
  }

  isCancelled(): boolean {
    return this.cancelled
  }
}
