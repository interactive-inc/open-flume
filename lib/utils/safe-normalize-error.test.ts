import { describe, expect, it } from "vitest"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

describe("safeNormalizeError", () => {
  it("returns the same Error instance when input is already Error", () => {
    const err = new Error("x")
    expect(safeNormalizeError({ value: err })).toBe(err)
  })

  it("wraps string in Error", () => {
    const result = safeNormalizeError({ value: "raw" })
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("raw")
  })

  it("wraps cursed object whose toString throws into a fallback Error", () => {
    const cursed = {
      [Symbol.toPrimitive]() {
        throw new Error("sym-throws")
      },
    }
    const result = safeNormalizeError({ value: cursed })
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("<unprintable error>")
  })

  it("wraps Error subclass whose message getter throws", () => {
    class CursedError extends Error {
      override get message(): string {
        throw new Error("getter-throws")
      }
    }
    const result = safeNormalizeError({ value: new CursedError() })
    expect(result).toBeInstanceOf(Error)
  })
})
