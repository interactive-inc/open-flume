type Props = {
  maxSize: number
}

export class FlumeGitHubSeenCache {
  private seen = new Map<string, string>()

  constructor(private readonly props: Props) {}

  has(id: string, updatedAt: string): boolean {
    return this.seen.get(id) === updatedAt
  }

  /**
   * 既存キーは delete → set で挿入順を末尾へ更新する (LRU)。
   * これをしないと trim() が「長寿命で更新され続ける thread」から先に追い出す
   */
  add(id: string, updatedAt: string): void {
    this.seen.delete(id)
    this.seen.set(id, updatedAt)
  }

  trim(): void {
    if (this.seen.size <= this.props.maxSize) return

    let removeCount = this.seen.size - this.props.maxSize

    for (const id of this.seen.keys()) {
      if (removeCount <= 0) break
      this.seen.delete(id)
      removeCount--
    }
  }

  get size(): number {
    return this.seen.size
  }
}
