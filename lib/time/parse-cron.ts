import { FlumeParseError } from "@/errors/parse-error"
import { parseCronField } from "@/time/parse-cron-field"

export type FlumeCron = {
  source: string
  minutes: ReadonlySet<number>
  hours: ReadonlySet<number>
  daysOfMonth: ReadonlySet<number>
  months: ReadonlySet<number>
  daysOfWeek: ReadonlySet<number>
  /** day-of-month フィールドが `*` 以外か。dow と両方制限時は OR マッチ (標準 cron 準拠) */
  domRestricted: boolean
  dowRestricted: boolean
}

/**
 * 5 フィールド cron 式をパースする。dow は 0-7 を許可し 7 を 0 (日曜) に正規化する
 */
export function parseCron(expression: string): FlumeCron | FlumeParseError {
  const trimmed = expression.trim()
  const fields = trimmed.split(/\s+/)
  if (fields.length !== 5) {
    return new FlumeParseError(`cron must have 5 fields, got ${fields.length}: "${expression}"`)
  }

  const minutes = parseCronField(fields[0] ?? "", 0, 59)
  if (minutes instanceof FlumeParseError) return minutes

  const hours = parseCronField(fields[1] ?? "", 0, 23)
  if (hours instanceof FlumeParseError) return hours

  const daysOfMonth = parseCronField(fields[2] ?? "", 1, 31)
  if (daysOfMonth instanceof FlumeParseError) return daysOfMonth

  const months = parseCronField(fields[3] ?? "", 1, 12)
  if (months instanceof FlumeParseError) return months

  const rawDaysOfWeek = parseCronField(fields[4] ?? "", 0, 7)
  if (rawDaysOfWeek instanceof FlumeParseError) return rawDaysOfWeek

  const daysOfWeek = new Set<number>()
  for (const value of rawDaysOfWeek) daysOfWeek.add(value === 7 ? 0 : value)

  return {
    source: trimmed,
    minutes,
    hours,
    daysOfMonth,
    months,
    daysOfWeek,
    domRestricted: fields[2] !== "*",
    dowRestricted: fields[4] !== "*",
  }
}
