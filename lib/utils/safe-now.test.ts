import { describe, expect, it } from "vitest"
import { safeNow } from "@/utils/safe-now"

const expectCloseToNow = (value: number) => {
  const now = Date.now()
  expect(value).toBeGreaterThanOrEqual(now - 5000)
  expect(value).toBeLessThanOrEqual(now + 5000)
}

describe("safeNow", () => {
  it("returns deps.now() when it succeeds", () => {
    expect(safeNow({ deps: { now: () => 12345 } })).toBe(12345)
  })

  it("falls back to Date.now() when deps.now() throws", () => {
    const value = safeNow({
      deps: {
        now: () => {
          throw new Error("boom")
        },
      },
    })

    expectCloseToNow(value)
  })

  it("falls back to Date.now() when deps.now() returns NaN", () => {
    expectCloseToNow(safeNow({ deps: { now: () => Number.NaN } }))
  })

  it("falls back to Date.now() when deps.now() returns Infinity", () => {
    expectCloseToNow(safeNow({ deps: { now: () => Number.POSITIVE_INFINITY } }))
  })
})
