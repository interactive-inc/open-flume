import { describe, expect, it } from "vitest"
import { safeStringify } from "@/utils/safe-stringify"

describe("safeStringify", () => {
  it("returns string for serializable input", () => {
    expect(safeStringify({ a: 1 })).toBe('{"a":1}')
  })

  it("returns Error for cyclic structures", () => {
    const a: Record<string, unknown> = {}
    a.self = a
    expect(safeStringify(a)).toBeInstanceOf(Error)
  })

  it("returns Error when toJSON throws", () => {
    const cursed = {
      toJSON: () => {
        throw new Error("bad")
      },
    }
    expect(safeStringify(cursed)).toBeInstanceOf(Error)
  })
})
