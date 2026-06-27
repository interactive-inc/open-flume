import type { FlumeCron } from "@/time/parse-cron"
import { FlumeParseError } from "@/errors/parse-error"

const MINUTE_MS = 60_000

// 到達不能な cron (例: 2 月 30 日) を無限ループさせないための上限。
// 月/日/時の単位でジャンプするので通常は数百回で収束する
const MAX_ITERATIONS = 500_000

/**
 * `afterMs` より後の最初の cron マッチ時刻 (epoch ms) を壁時計 (local time) で求める。
 * 到達不能なら FlumeParseError を返す
 */
export function flumeCronNext(cron: FlumeCron, afterMs: number): number | FlumeParseError {
  let candidate = Math.floor(afterMs / MINUTE_MS) * MINUTE_MS + MINUTE_MS

  for (let iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
    const date = new Date(candidate)

    if (!cron.months.has(date.getMonth() + 1)) {
      candidate = new Date(date.getFullYear(), date.getMonth() + 1, 1, 0, 0, 0, 0).getTime()
      continue
    }
    if (!matchesDay(cron, date)) {
      candidate = new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate() + 1,
        0,
        0,
        0,
        0,
      ).getTime()
      continue
    }
    if (!cron.hours.has(date.getHours())) {
      candidate = new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours() + 1,
        0,
        0,
        0,
      ).getTime()
      continue
    }
    if (!cron.minutes.has(date.getMinutes())) {
      candidate += MINUTE_MS
      continue
    }

    return candidate
  }

  return new FlumeParseError(`cron "${cron.source}" has no next time within bound`)
}

function matchesDay(cron: FlumeCron, date: Date): boolean {
  const domMatch = cron.daysOfMonth.has(date.getDate())
  const dowMatch = cron.daysOfWeek.has(date.getDay())

  if (cron.domRestricted && cron.dowRestricted) return domMatch || dowMatch
  if (cron.domRestricted) return domMatch
  if (cron.dowRestricted) return dowMatch
  return true
}
