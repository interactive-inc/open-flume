import { FlumeParseError } from "@/errors/parse-error"

/**
 * 単一 cron フィールド (minute など) の spec を許可値の Set に展開する。
 * 対応: `*` / `* /n` / `a` / `a-b` / `a-b/n` とそれらのカンマ区切り。名前 (JAN, MON) は非対応。
 * 空トークン (`"5,"` / `"-5"` / `"/5"` など) は `Number("") === 0` の暗黙変換で
 * 0 に化けるため、数値が期待される位置の空文字列は明示的に拒否する
 */
export function parseCronField(
  spec: string,
  min: number,
  max: number,
): Set<number> | FlumeParseError {
  const values = new Set<number>()

  for (const part of spec.split(",")) {
    if (part === "") return new FlumeParseError(`empty cron list segment: "${spec}"`)

    const expanded = expandCronPart(part, min, max)
    if (expanded instanceof FlumeParseError) return expanded

    for (const value of expanded) values.add(value)
  }

  if (values.size === 0) return new FlumeParseError(`cron field empty: "${spec}"`)
  return values
}

function expandCronPart(part: string, min: number, max: number): number[] | FlumeParseError {
  const slash = part.indexOf("/")
  const range = slash === -1 ? part : part.slice(0, slash)
  const stepToken = slash === -1 ? null : part.slice(slash + 1)

  if (stepToken === "") return new FlumeParseError(`invalid cron step: "${part}"`)

  const step = stepToken === null ? 1 : Number(stepToken)
  if (!Number.isInteger(step) || step <= 0) {
    return new FlumeParseError(`invalid cron step: "${part}"`)
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
  const loToken = dash === -1 ? range : range.slice(0, dash)
  const hiToken = dash === -1 ? loToken : range.slice(dash + 1)

  if (loToken === "" || hiToken === "") {
    return new FlumeParseError(`invalid cron range: "${range}"`)
  }

  const lo = Number(loToken)
  const hi = Number(hiToken)

  if (!Number.isInteger(lo) || !Number.isInteger(hi)) {
    return new FlumeParseError(`invalid cron range: "${range}"`)
  }
  if (lo < min || hi > max || lo > hi) {
    return new FlumeParseError(`cron value out of range [${min}-${max}]: "${range}"`)
  }
  return { lo, hi }
}
