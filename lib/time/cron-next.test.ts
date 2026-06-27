import { describe, it, expect } from "vitest"
import { FlumeParseError } from "@/errors/parse-error"
import { flumeCronNext } from "@/time/cron-next"
import { parseCron } from "@/time/parse-cron"

function cron(expression: string) {
  const result = parseCron(expression)
  if (result instanceof FlumeParseError) throw result
  return result
}

function nextDate(expression: string, from: Date): Date {
  const result = flumeCronNext(cron(expression), from.getTime())
  if (result instanceof FlumeParseError) throw result
  return new Date(result)
}

describe("flumeCronNext", () => {
  it("returns the next minute boundary for '* * * * *'", () => {
    const from = new Date(2026, 0, 1, 10, 30, 25)
    const next = nextDate("* * * * *", from)

    expect(next.getTime()).toBe(new Date(2026, 0, 1, 10, 31, 0, 0).getTime())
  })

  it("is strictly after the input even on an exact minute boundary", () => {
    const from = new Date(2026, 0, 1, 10, 30, 0, 0)
    const next = nextDate("* * * * *", from)

    expect(next.getTime()).toBe(new Date(2026, 0, 1, 10, 31, 0, 0).getTime())
  })

  it("fires hourly at minute 0 for '0 * * * *'", () => {
    const next = nextDate("0 * * * *", new Date(2026, 0, 1, 10, 30, 0))

    expect(next.getMinutes()).toBe(0)
    expect(next.getHours()).toBe(11)
  })

  it("rolls to next day for a specific hour/minute", () => {
    const next = nextDate("0 9 * * *", new Date(2026, 0, 1, 10, 0, 0))

    expect(next.getDate()).toBe(2)
    expect(next.getHours()).toBe(9)
    expect(next.getMinutes()).toBe(0)
  })

  it("matches '*/15' steps", () => {
    const next = nextDate("*/15 * * * *", new Date(2026, 0, 1, 10, 7, 0))

    expect(next.getMinutes()).toBe(15)
  })

  it("uses OR semantics when both dom and dow are restricted", () => {
    // 1 日 OR 月曜。2026-01-01 は木曜なので、次は 1/5 (月) ではなく
    // dom=1 を最初に満たす翌月 1 日… ではなく当日 09:00 が既に過ぎていれば直近の該当日。
    // ここでは 2026-01-02(金) 開始 → 次の該当は 1/5(月)
    const next = nextDate("0 0 1 * 1", new Date(2026, 0, 2, 0, 0, 0))

    expect(next.getDate()).toBe(5)
    expect(next.getDay()).toBe(1)
  })

  it("returns an error for an unreachable date (Feb 30)", () => {
    const result = flumeCronNext(cron("0 0 30 2 *"), new Date(2026, 0, 1).getTime())

    expect(result).toBeInstanceOf(FlumeParseError)
  })
})
