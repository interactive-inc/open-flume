import { describe, it, expect } from "vitest"
import { safeJsonParse } from "@/utils/safe-json-parse"

describe("safeJsonParse", () => {
  it("parses valid JSON", () => {
    expect(safeJsonParse('{"a":1}')).toEqual({ a: 1 })
  })

  it("returns null for invalid JSON", () => {
    expect(safeJsonParse("not json")).toBe(null)
  })

  it("returns null for empty string", () => {
    expect(safeJsonParse("")).toBe(null)
  })

  it("parses arrays", () => {
    expect(safeJsonParse("[1,2,3]")).toEqual([1, 2, 3])
  })

  it("parses primitive values", () => {
    expect(safeJsonParse("42")).toBe(42)
    expect(safeJsonParse('"hello"')).toBe("hello")
    expect(safeJsonParse("null")).toBe(null)
  })
})
