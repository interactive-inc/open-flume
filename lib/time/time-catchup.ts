import type { FlumeCatchupPolicy, FlumeRuntimeDeps } from "@/types"
import type { FlumeCron } from "@/time/parse-cron"
import { FlumeParseError } from "@/errors/parse-error"
import { flumeCronNext } from "@/time/cron-next"
import { isDstDuplicateFire } from "@/time/is-dst-duplicate-fire"

const DEFAULT_MISSED_WINDOW_MS = 24 * 60 * 60 * 1000
const MAX_CATCHUP_MATCHES = 10_000
const MINUTE_MS = 60_000

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
  let lookbackMs = MINUTE_MS

  while (true) {
    const recentStart = Math.max(props.windowStart, props.now - lookbackMs)
    const walked = walkMatches({ ...props, windowStart: recentStart })
    if (walked instanceof FlumeParseError) return walked

    const latest = walked[walked.length - 1]
    if (latest !== undefined) return { matches: [latest], truncated: false }
    if (recentStart === props.windowStart) return { matches: [], truncated: false }

    lookbackMs *= 2
  }
}

function collectMissed(props: WalkProps): FlumeCatchupMatches | FlumeParseError {
  let lookbackMs = MAX_CATCHUP_MATCHES * MINUTE_MS

  while (true) {
    const recentStart = Math.max(props.windowStart, props.now - lookbackMs)
    const walked = walkMatches({ ...props, windowStart: recentStart })
    if (walked instanceof FlumeParseError) return walked

    if (walked.length >= MAX_CATCHUP_MATCHES || recentStart === props.windowStart) {
      const matches = walked.slice(Math.max(0, walked.length - MAX_CATCHUP_MATCHES))
      const older = hasMatchBefore(props.cron, props.windowStart, recentStart)
      if (older instanceof FlumeParseError) return older
      return { matches, truncated: walked.length > MAX_CATCHUP_MATCHES || older }
    }

    lookbackMs *= 2
  }
}

function walkMatches(props: WalkProps): number[] | FlumeParseError {
  const matches: number[] = []
  let cursor = props.windowStart

  while (true) {
    const next = flumeCronNext(props.cron, cursor)
    if (next instanceof FlumeParseError) return next
    if (next > props.now) return matches

    if (!isDstDuplicateFire(matches, next)) matches.push(next)
    cursor = next
  }
}

function hasMatchBefore(
  cron: FlumeCron,
  windowStart: number,
  recentStart: number,
): boolean | FlumeParseError {
  if (recentStart === windowStart) return false

  const first = flumeCronNext(cron, windowStart)
  if (first instanceof FlumeParseError) return first
  return first <= recentStart
}

export type CatchupDeps = Pick<FlumeRuntimeDeps, "now">
