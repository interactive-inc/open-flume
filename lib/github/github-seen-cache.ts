type Props = {
  maxSize: number
}

export class FlumeGitHubSeenCache {
  private seen = new Map<string, string>()

  constructor(private readonly props: Props) {}

  has(id: string, updatedAt: string): boolean {
    return this.seen.get(id) === updatedAt
  }

  add(id: string, updatedAt: string): void {
    this.seen.set(id, updatedAt)
  }

  trim(): void {
    if (this.seen.size <= this.props.maxSize) return

    const entries = [...this.seen.entries()]
    this.seen = new Map(entries.slice(entries.length - this.props.maxSize))
  }

  get size(): number {
    return this.seen.size
  }
}
