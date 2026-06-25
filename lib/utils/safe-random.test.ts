import { describe, expect, it } from "vitest"
import { safeRandom } from "@/utils/safe-random"

describe("safeRandom", () => {
  it("returns deps.random() when in [0, 1)", () => {
    expect(safeRandom({ deps: { random: () => 0.42 } })).toBe(0.42)
  })

  it("returns 0.5 when deps.random() throws", () => {
    expect(
      safeRandom({
        deps: {
          random: () => {
            throw new Error("boom")
          },
        },
      }),
    ).toBe(0.5)
  })

  it("returns 0.5 when deps.random() returns NaN", () => {
    expect(safeRandom({ deps: { random: () => Number.NaN } })).toBe(0.5)
  })

  it("returns 0.5 when deps.random() returns >= 1", () => {
    expect(safeRandom({ deps: { random: () => 1.5 } })).toBe(0.5)
  })

  it("returns 0.5 when deps.random() returns < 0", () => {
    expect(safeRandom({ deps: { random: () => -0.1 } })).toBe(0.5)
  })

  it("accepts 0 as a valid value", () => {
    expect(safeRandom({ deps: { random: () => 0 } })).toBe(0)
  })
})
