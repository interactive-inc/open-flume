import type { FlumeCatchupPolicy, FlumeRuntimeDeps } from "@/types"
import type { FlumeCron } from "@/time/parse-cron"
import { FlumeParseError } from "@/errors/parse-error"
import { flumeCronNext } from "@/time/cron-next"

const DEFAULT_MISSED_WINDOW_MS = 24 * 60 * 60 * 1000
const MAX_CATCHUP_MATCHES = 10_000

// flumeCronNext と同じ趣旨の有限性ガード。lastOnly は件数無制限のためこの反復数で打ち切る
// (分単位 cron の 30 日ギャップ ≒ 43,200 回でも余裕で now に到達する)
const MAX_WALK_ITERATIONS = 500_000

type Props = {
  cron: FlumeCron
  lastFiredAt: number
  now: number
  policy: FlumeCatchupPolicy
}

export type FlumeCatchupMatches = {
  matches: ReadonlyArray<number>
  /** missed: 上限超過で古いマッチを切り捨てた / lastOnly: 反復上限で now まで走査しきれなかった */
  truncated: boolean
}

/**
 * `lastFiredAt` から `now` までに過ぎ去った cron マッチを policy に従って列挙する。
 *
 * - policy.mode === "off"      : 常に空
 * - policy.mode === "lastOnly" : 過ぎ去ったマッチの中で最も新しいもの 1 件 (件数上限なし・O(1) メモリ)
 * - policy.mode === "missed"   : maxWindowMs (既定 24h) 以内に過ぎ去ったすべてのマッチ。
 *                                window の起点は `max(lastFiredAt, now - maxWindowMs)`。
 *                                10,000 件を超えた場合は古い方を捨てて新しい 10,000 件を返し
 *                                truncated: true で通知する
 *
 * 到達不能 cron や catastrophic な policy ミス指定の場合は FlumeParseError を返す
 * (catchup 列挙だけで失敗させる。source 本体の起動は別判断)
 */
export function flumeCollectCatchupMatches(props: Props): FlumeCatchupMatches | FlumeParseError {
  const policy = props.policy

  if (policy.mode === "off") return { matches: [], truncated: false }
  if (props.lastFiredAt >= props.now) return { matches: [], truncated: false }

  if (policy.mode === "lastOnly") {
    return collectLastOnly({ cron: props.cron, windowStart: props.lastFiredAt, now: props.now })
  }

  const windowStart = Math.max(
    props.lastFiredAt,
    props.now - (policy.maxWindowMs ?? DEFAULT_MISSED_WINDOW_MS),
  )
  return collectMissed({ cron: props.cron, windowStart, now: props.now })
}

type WalkProps = {
  cron: FlumeCron
  windowStart: number
  now: number
}

function collectLastOnly(props: WalkProps): FlumeCatchupMatches | FlumeParseError {
  let latest: number | null = null
  let cursor = props.windowStart

  for (let iteration = 0; iteration < MAX_WALK_ITERATIONS; iteration++) {
    const next = flumeCronNext(props.cron, cursor)
    if (next instanceof FlumeParseError) return next
    if (next > props.now) {
      return { matches: latest === null ? [] : [latest], truncated: false }
    }

    latest = next
    cursor = next
  }

  return { matches: latest === null ? [] : [latest], truncated: true }
}

function collectMissed(props: WalkProps): FlumeCatchupMatches | FlumeParseError {
  const collected: number[] = []
  let cursor = props.windowStart
  let hasReachedNow = false

  for (let iteration = 0; iteration < MAX_WALK_ITERATIONS; iteration++) {
    const next = flumeCronNext(props.cron, cursor)
    if (next instanceof FlumeParseError) return next
    if (next > props.now) {
      hasReachedNow = true
      break
    }

    collected.push(next)
    cursor = next
  }

  if (collected.length > MAX_CATCHUP_MATCHES) {
    return { matches: collected.slice(collected.length - MAX_CATCHUP_MATCHES), truncated: true }
  }
  return { matches: collected, truncated: !hasReachedNow }
}

export type CatchupDeps = Pick<FlumeRuntimeDeps, "now">
