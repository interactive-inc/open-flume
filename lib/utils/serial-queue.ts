/**
 * 投入順を保ったまま task を直列実行する。各 task は前の完了を待ってから走る。
 * task が throw しても後続には伝播しない (キュー自体は止まらない)
 */
export class FlumeSerialQueue {

  private chain: Promise<void> = Promise.resolve()

  add(task: () => Promise<void>): void {
    this.chain = this.chain.then(task).catch(() => {})
  }

  async drain(): Promise<void> {
    await this.chain
  }
}
