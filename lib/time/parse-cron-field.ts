import { FlumeParseError } from "@/errors/parse-error"

/**
 * 単一 cron フィールド (minute など) の spec を許可値の Set に展開する。
 * 対応: `*` / `* /n` / `a` / `a-b` / `a-b/n` とそれらのカンマ区切り。名前 (JAN, MON) は非対応
 */
export function parseCronField(
  spec: string,
  min: number,
  max: number,
): Set<number> | FlumeParseError {
  const values = new Set<number>()

  for (const part of spec.split(",")) {
    const expanded = expandCronPart(part, min, max)
    if (expanded instanceof FlumeParseError) return expanded

    for (const value of expanded) values.add(value)
  }

  if (values.size === 0) return new FlumeParseError(`cron field empty: "${spec}"`)
  return values
}

function expandCronPart(part: string, min: number, max: number): number[] | FlumeParseError {
  let range = part
  let step = 1

  const slash = part.indexOf("/")
  if (slash !== -1) {
    range = part.slice(0, slash)
    const parsed = Number(part.slice(slash + 1))
    if (!Number.isInteger(parsed) || parsed <= 0) {
      return new FlumeParseError(`invalid cron step: "${part}"`)
    }
    step = parsed
  }

  const bounds = resolveBounds(range, min, max)
  if (bounds instanceof FlumeParseError) return bounds

  const numbers: number[] = []
  for (let value = bounds.lo; value <= bounds.hi; value += step) numbers.push(value)
  return numbers
}

function resolveBounds(
  range: string,
  min: number,
  max: number,
): { lo: number; hi: number } | FlumeParseError {
  if (range === "*") return { lo: min, hi: max }

  const dash = range.indexOf("-")
  const lo = dash === -1 ? Number(range) : Number(range.slice(0, dash))
  const hi = dash === -1 ? lo : Number(range.slice(dash + 1))

  if (!Number.isInteger(lo) || !Number.isInteger(hi)) {
    return new FlumeParseError(`invalid cron range: "${range}"`)
  }
  if (lo < min || hi > max || lo > hi) {
    return new FlumeParseError(`cron value out of range [${min}-${max}]: "${range}"`)
  }
  return { lo, hi }
}
