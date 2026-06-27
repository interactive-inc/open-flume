import { describe, it, expect } from "vitest"
import { FlumeParseError } from "@/errors/parse-error"
import { parseCron } from "@/time/parse-cron"

function unwrap(result: ReturnType<typeof parseCron>) {
  if (result instanceof FlumeParseError) throw result
  return result
}

describe("parseCron", () => {
  it("parses a fully wildcard expression", () => {
    const cron = unwrap(parseCron("* * * * *"))

    expect(cron.minutes.size).toBe(60)
    expect(cron.hours.size).toBe(24)
    expect(cron.domRestricted).toBe(false)
    expect(cron.dowRestricted).toBe(false)
  })

  it("parses a single minute/hour", () => {
    const cron = unwrap(parseCron("30 9 * * *"))

    expect([...cron.minutes]).toEqual([30])
    expect([...cron.hours]).toEqual([9])
  })

  it("parses step values", () => {
    const cron = unwrap(parseCron("*/15 * * * *"))

    expect([...cron.minutes]).toEqual([0, 15, 30, 45])
  })

  it("parses ranges and lists", () => {
    const cron = unwrap(parseCron("0 9-11,17 * * *"))

    expect([...cron.hours]).toEqual([9, 10, 11, 17])
  })

  it("normalizes day-of-week 7 to 0 (Sunday)", () => {
    const cron = unwrap(parseCron("0 0 * * 7"))

    expect([...cron.daysOfWeek]).toEqual([0])
    expect(cron.dowRestricted).toBe(true)
  })

  it("flags dom restriction independently of dow", () => {
    const cron = unwrap(parseCron("0 0 1 * *"))

    expect(cron.domRestricted).toBe(true)
    expect(cron.dowRestricted).toBe(false)
  })

  it("rejects wrong field count", () => {
    expect(parseCron("* * * *")).toBeInstanceOf(FlumeParseError)
    expect(parseCron("* * * * * *")).toBeInstanceOf(FlumeParseError)
  })

  it("rejects out-of-range values", () => {
    expect(parseCron("60 * * * *")).toBeInstanceOf(FlumeParseError)
    expect(parseCron("0 24 * * *")).toBeInstanceOf(FlumeParseError)
    expect(parseCron("0 0 0 * *")).toBeInstanceOf(FlumeParseError)
  })

  it("rejects invalid step and inverted ranges", () => {
    expect(parseCron("*/0 * * * *")).toBeInstanceOf(FlumeParseError)
    expect(parseCron("10-5 * * * *")).toBeInstanceOf(FlumeParseError)
    expect(parseCron("a * * * *")).toBeInstanceOf(FlumeParseError)
  })
})
