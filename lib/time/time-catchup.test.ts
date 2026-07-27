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

  it("keeps the newest matches across a gap larger than the old walk bound", () => {
    const now = 600_000 * MINUTE_MS
    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("* * * * *"),
        lastFiredAt: 0,
        now,
        policy: { mode: "missed", maxWindowMs: now },
      }),
    )

    expect(collected.matches).toHaveLength(10_000)
    expect(collected.matches[0]).toBe(590_001 * MINUTE_MS)
    expect(collected.matches[collected.matches.length - 1]).toBe(now)
    expect(collected.truncated).toBe(true)
  })

  it("keeps the newest matches when a sparse cron needs an expanded search window", () => {
    const now = 60_000 * MINUTE_MS
    const collected = unwrap(
      flumeCollectCatchupMatches({
        cron: cron("*/3 * * * *"),
        lastFiredAt: 0,
        now,
        policy: { mode: "missed", maxWindowMs: now },
      }),
    )

    expect(collected.matches).toHaveLength(10_000)
    expect(collected.matches[0]).toBe(30_003 * MINUTE_MS)
    expect(collected.matches[collected.matches.length - 1]).toBe(now)
    expect(collected.truncated).toBe(true)
  })

  it("deduplicates every repeated wall-clock minute during DST fall-back", () => {
    const originalTz = process.env.TZ
    process.env.TZ = "America/New_York"

    try {
      const collected = unwrap(
        flumeCollectCatchupMatches({
          cron: cron("*/15 1 * * *"),
          lastFiredAt: Date.UTC(2021, 10, 7, 4, 59),
          now: Date.UTC(2021, 10, 7, 6, 45),
          policy: { mode: "missed", maxWindowMs: 2 * 60 * MINUTE_MS },
        }),
      )

      expect(collected.matches).toEqual([
        Date.UTC(2021, 10, 7, 5, 0),
        Date.UTC(2021, 10, 7, 5, 15),
        Date.UTC(2021, 10, 7, 5, 30),
        Date.UTC(2021, 10, 7, 5, 45),
      ])
    } finally {
      if (originalTz === undefined) delete process.env.TZ
      else process.env.TZ = originalTz
    }
  })
})
