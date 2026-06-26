import type { FlumeRuntimeDeps } from "@/types"
import { safeNow } from "@/utils/safe-now"

type Props = {
  maxSize: number
  ttlMs: number
  deps: Pick<FlumeRuntimeDeps, "now">
}

/**
 * Slack envelope_id を maxSize / ttlMs で制限するキャッシュ。Slack は ack 失敗時に同じ envelope を
 * 再送してくるため、source 層で重複配送を防ぐ。TTL を過ぎたエントリは has() が false を返し、
 * trim() で容量超過分が古い順に落とされる
 */
export class FlumeSlackSeenCache {
  private seen = new Map<string, number>()

  constructor(private readonly props: Props) {}

  has(envelopeId: string): boolean {
    const timestamp = this.seen.get(envelopeId)
    if (timestamp === undefined) return false
    if (safeNow({ deps: this.props.deps }) - timestamp > this.props.ttlMs) {
      this.seen.delete(envelopeId)
      return false
    }
    return true
  }

  add(envelopeId: string): void {
    this.seen.set(envelopeId, safeNow({ deps: this.props.deps }))
  }

  trim(): void {
    const cutoff = safeNow({ deps: this.props.deps }) - this.props.ttlMs

    for (const [id, timestamp] of this.seen) {
      if (timestamp < cutoff) this.seen.delete(id)
    }

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
