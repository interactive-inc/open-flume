import { describe, expect, it } from "vitest"
import { safeNow } from "@/utils/safe-now"

describe("safeNow", () => {
  it("returns deps.now() when it succeeds", () => {
    expect(safeNow({ deps: { now: () => 12345 } })).toBe(12345)
  })

  it("returns 0 when deps.now() throws", () => {
    expect(
      safeNow({
        deps: {
          now: () => {
            throw new Error("boom")
          },
        },
      }),
    ).toBe(0)
  })

  it("returns 0 when deps.now() returns NaN", () => {
    expect(safeNow({ deps: { now: () => Number.NaN } })).toBe(0)
  })

  it("returns 0 when deps.now() returns Infinity", () => {
    expect(safeNow({ deps: { now: () => Number.POSITIVE_INFINITY } })).toBe(0)
  })
})
