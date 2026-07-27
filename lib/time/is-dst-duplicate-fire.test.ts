import { describe, it, expect, beforeAll, afterAll } from "vitest"
import { isDstDuplicateFire } from "@/time/is-dst-duplicate-fire"

const originalTz = process.env.TZ

describe("isDstDuplicateFire", () => {
  beforeAll(() => {
    process.env.TZ = "America/New_York"
  })

  afterAll(() => {
    if (originalTz === undefined) {
      delete process.env.TZ
      return
    }
    process.env.TZ = originalTz
  })

  it("detects the fall-back duplicate wall-clock minute (01:30 EDT vs 01:30 EST)", () => {
    const firstOneThirty = Date.UTC(2021, 10, 7, 5, 30)
    const secondOneThirty = Date.UTC(2021, 10, 7, 6, 30)

    expect(isDstDuplicateFire([firstOneThirty], secondOneThirty)).toBe(true)
  })

  it("ignores a normal next-minute advance", () => {
    const firedAt = Date.UTC(2021, 10, 7, 5, 30)

    expect(isDstDuplicateFire([firedAt], firedAt + 60_000)).toBe(false)
  })

  it("ignores targets more than 2 hours ahead even on the same wall-clock minute", () => {
    const firedAt = Date.UTC(2021, 10, 7, 5, 30)
    const nextDay = Date.UTC(2021, 10, 8, 6, 30)

    expect(isDstDuplicateFire([firedAt], nextDay)).toBe(false)
  })

  it("ignores non-forward targets", () => {
    const firedAt = Date.UTC(2021, 10, 7, 5, 30)

    expect(isDstDuplicateFire([firedAt], firedAt)).toBe(false)
    expect(isDstDuplicateFire([firedAt], firedAt - 60_000)).toBe(false)
  })
})
