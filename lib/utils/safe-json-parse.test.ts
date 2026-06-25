import { describe, it, expect } from "vitest"
import { FlumeParseError } from "@/errors/parse-error"
import { safeJsonParse } from "@/utils/safe-json-parse"

describe("safeJsonParse", () => {
  it("parses valid JSON", () => {
    expect(safeJsonParse('{"a":1}')).toEqual({ a: 1 })
  })

  it("returns FlumeParseError for invalid JSON", () => {
    const result = safeJsonParse("not json")

    expect(result).toBeInstanceOf(FlumeParseError)
  })

  it("returns FlumeParseError for empty string", () => {
    const result = safeJsonParse("")

    expect(result).toBeInstanceOf(FlumeParseError)
  })

  it("FlumeParseError preserves the underlying SyntaxError as cause", () => {
    const result = safeJsonParse("{{{")

    expect(result).toBeInstanceOf(FlumeParseError)
    if (result instanceof FlumeParseError) {
      expect(result.cause).toBeInstanceOf(SyntaxError)
    }
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
