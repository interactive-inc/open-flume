import { describe, it, expect } from "vitest"
import { isRecord } from "@/utils/is-record"

describe("isRecord", () => {
  it("returns true for plain objects", () => {
    expect(isRecord({})).toBe(true)
    expect(isRecord({ a: 1 })).toBe(true)
  })

  it("returns false for null", () => {
    expect(isRecord(null)).toBe(false)
  })

  it("returns false for primitives", () => {
    expect(isRecord(42)).toBe(false)
    expect(isRecord("hello")).toBe(false)
    expect(isRecord(true)).toBe(false)
    expect(isRecord(undefined)).toBe(false)
  })

  it("returns true for arrays", () => {
    expect(isRecord([])).toBe(true)
  })
})
