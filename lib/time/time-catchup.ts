import type { FlumeCatchupPolicy, FlumeRuntimeDeps } from "@/types"
import type { FlumeCron } from "@/time/parse-cron"
import { FlumeParseError } from "@/errors/parse-error"
import { flumeCronNext } from "@/time/cron-next"

const DEFAULT_MISSED_WINDOW_MS = 24 * 60 * 60 * 1000
const MAX_CATCHUP_MATCHES = 10_000

type Props = {
  cron: FlumeCron
  lastFiredAt: number
  now: number
  policy: FlumeCatchupPolicy
}

/**
 * `lastFiredAt` から `now` までに過ぎ去った cron マッチを policy に従って列挙する。
 *
 * - policy.mode === "off"      : 常に空配列
 * - policy.mode === "lastOnly" : 過ぎ去ったマッチの中で最も新しいもの 1 件
 * - policy.mode === "missed"   : maxWindowMs (既定 24h) 以内に過ぎ去ったすべてのマッチ。
 *                                window の起点は `max(lastFiredAt, now - maxWindowMs)`
 *
 * 到達不能 cron や catastrophic な policy ミス指定の場合は FlumeParseError を返す
 * (catchup 列挙だけで失敗させる。source 本体の起動は別判断)
 */
export function flumeCollectCatchupMatches(props: Props): ReadonlyArray<number> | FlumeParseError {
  if (props.policy.mode === "off") return []
  if (props.lastFiredAt >= props.now) return []

  const windowStart =
    props.policy.mode === "missed"
      ? Math.max(props.lastFiredAt, props.now - (props.policy.maxWindowMs ?? DEFAULT_MISSED_WINDOW_MS))
      : props.lastFiredAt

  const matches: number[] = []
  let cursor = windowStart

  for (let i = 0; i < MAX_CATCHUP_MATCHES; i++) {
    const next = flumeCronNext(props.cron, cursor)
    if (next instanceof FlumeParseError) return next
    if (next > props.now) break

    matches.push(next)
    cursor = next
  }

  if (props.policy.mode === "lastOnly") {
    const last = matches[matches.length - 1]
    return last === undefined ? [] : [last]
  }
  return matches
}

export type CatchupDeps = Pick<FlumeRuntimeDeps, "now">
