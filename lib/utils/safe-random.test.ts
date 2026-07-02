import { describe, expect, it } from "vitest"
import { safeRandom } from "@/utils/safe-random"

const expectInRandomRange = (value: number) => {
  expect(value).toBeGreaterThanOrEqual(0)
  expect(value).toBeLessThan(1)
}

describe("safeRandom", () => {
  it("returns deps.random() when in [0, 1)", () => {
    expect(safeRandom({ deps: { random: () => 0.42 } })).toBe(0.42)
  })

  it("falls back to Math.random() when deps.random() throws", () => {
    const value = safeRandom({
      deps: {
        random: () => {
          throw new Error("boom")
        },
      },
    })

    expectInRandomRange(value)
  })

  it("falls back to Math.random() when deps.random() returns NaN", () => {
    expectInRandomRange(safeRandom({ deps: { random: () => Number.NaN } }))
  })

  it("falls back to Math.random() when deps.random() returns >= 1", () => {
    expectInRandomRange(safeRandom({ deps: { random: () => 1.5 } }))
  })

  it("falls back to Math.random() when deps.random() returns < 0", () => {
    expectInRandomRange(safeRandom({ deps: { random: () => -0.1 } }))
  })

  it("accepts 0 as a valid value", () => {
    expect(safeRandom({ deps: { random: () => 0 } })).toBe(0)
  })
})
