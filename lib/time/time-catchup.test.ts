import { describe, it, expect } from "vitest"
import { FlumeParseError } from "@/errors/parse-error"
import { parseCron } from "@/time/parse-cron"
import { flumeCollectCatchupMatches } from "@/time/time-catchup"
import type { FlumeCatchupMatches } from "@/time/time-catchup"

const MINUTE_MS = 60_000

function cron(expression: string) {
  const result = parseCron(expression)
  if (result instanceof FlumeParseError) throw result
  return result
}

function unwrap(result: FlumeCatchupMatches | FlumeParseError): FlumeCatchupMatches {
  if (result instanceof FlumeParseError) throw result
  return result
}

describe("flumeCollectCatchupMatches", () => {
  it("off returns no matches", () => {
    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("* * * * *"),
        lastFiredAt: 0,
        now: 10 * MINUTE_MS,
        policy: { mode: "off" },
      }),
    )

    expect(collected.matches).toEqual([])
    expect(collected.truncated).toBe(false)
  })

  it("lastOnly returns the single latest match across a 30-day gap with a minute cron", () => {
    const now = 30 * 24 * 60 * MINUTE_MS

    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("* * * * *"),
        lastFiredAt: 0,
        now,
        policy: { mode: "lastOnly" },
      }),
    )

    expect(collected.matches).toEqual([now])
    expect(collected.truncated).toBe(false)
  })

  it("missed keeps the newest matches and reports truncation past the cap", () => {
    const now = 20_000 * MINUTE_MS

    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("* * * * *"),
        lastFiredAt: 0,
        now,
        policy: { mode: "missed", maxWindowMs: 15_000 * MINUTE_MS },
      }),
    )

    expect(collected.truncated).toBe(true)
    expect(collected.matches).toHaveLength(10_000)

    // 古い方 (5,001 分目〜10,000 分目) が捨てられ、新しい 10,000 件が残る
    expect(collected.matches[0]).toBe(10_001 * MINUTE_MS)
    expect(collected.matches[collected.matches.length - 1]).toBe(now)
  })

  it("missed within the cap is not truncated", () => {
    const now = 10 * MINUTE_MS

    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("* * * * *"),
        lastFiredAt: 5 * MINUTE_MS,
        now,
        policy: { mode: "missed" },
      }),
    )

    expect(collected.matches).toEqual([
      6 * MINUTE_MS,
      7 * MINUTE_MS,
      8 * MINUTE_MS,
      9 * MINUTE_MS,
      10 * MINUTE_MS,
    ])
    expect(collected.truncated).toBe(false)
  })

  it("returns empty when lastFiredAt is not in the past", () => {
    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("* * * * *"),
        lastFiredAt: 10 * MINUTE_MS,
        now: 10 * MINUTE_MS,
        policy: { mode: "lastOnly" },
      }),
    )

    expect(collected.matches).toEqual([])
    expect(collected.truncated).toBe(false)
  })
})
