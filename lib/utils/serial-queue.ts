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
 * cancel() 後は add() が no-op になり、既に積まれた未実行 task も実行せずに流れ落ちる。
 * drain() は待機中に追加された task も含めてキューが空になるまで待つ
 */
export class FlumeSerialQueue {
  private chain: Promise<void> = Promise.resolve()

  private depth = 0

  private cancelled = false

  constructor(private readonly props: Props = {}) {}

  add(task: () => Promise<void>): Promise<void> {
    if (this.cancelled) return Promise.resolve()

    if (this.props.maxDepth !== undefined && this.depth >= this.props.maxDepth) {
      this.props.onOverflow?.({ dropped: 1, depth: this.depth })
      return Promise.resolve()
    }

    this.depth++
    const completion = this.chain.then(async () => {
      try {
        if (!this.cancelled) await task()
      } catch {
        // task 例外は queue を止めない (sources 側で log.error する)
      } finally {
        this.depth--
      }
    })
    this.chain = completion
    return completion
  }

  async drain(): Promise<void> {
    while (true) {
      const current = this.chain
      await current
      if (this.chain === current) return
    }
  }

  cancel(): void {
    this.cancelled = true
  }

  size(): number {
    return this.depth
  }

  isCancelled(): boolean {
    return this.cancelled
  }
}
