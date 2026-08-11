/**
 * 投入順を保ったまま task を直列実行する。各 task は前の完了を待ってから走る。
 * task が throw しても後続には伝播しない (キュー自体は止まらない)。
 * drain() は待機中に追加された task も含めてキューが空になるまで待つ
 */
export class FlumeSerialQueue {
  private chain: Promise<void> = Promise.resolve()

  add(task: () => Promise<void>): Promise<void> {
    const completion = this.chain.then(async () => {
      try {
        await task()
      } catch {
        // task 例外は queue を止めない (sources 側で log.error する)
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
}
