type Props = {
  maxSize: number
}

/**
 * Slack envelope_id の LRU 風キャッシュ。Slack は ack 失敗時に同じ envelope を再送するため、
 * source レイヤで handler への重複配送を防ぐ
 */
export class FlumeSlackSeenCache {

  private seen = new Set<string>()

  constructor(private readonly props: Props) {}

  has(envelopeId: string): boolean {
    return this.seen.has(envelopeId)
  }

  add(envelopeId: string): void {
    this.seen.add(envelopeId)
  }

  trim(): void {
    if (this.seen.size <= this.props.maxSize) return

    const entries = [...this.seen]
    this.seen = new Set(entries.slice(entries.length - this.props.maxSize))
  }

  get size(): number {
    return this.seen.size
  }
}
