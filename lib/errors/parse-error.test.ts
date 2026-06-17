import { describe, it, expect } from "vitest"
import { FlumeParseError } from "@/errors/parse-error"

describe("FlumeParseError", () => {
  it("has name FlumeParseError", () => {
    const error = new FlumeParseError("bad input")
    expect(error.name).toBe("FlumeParseError")
  })

  it("stores message", () => {
    const error = new FlumeParseError("invalid JSON")
    expect(error.message).toBe("invalid JSON")
  })

  it("extends Error", () => {
    const error = new FlumeParseError("fail")
    expect(error instanceof Error).toBe(true)
  })

  it("is frozen", () => {
    const error = new FlumeParseError("fail")
    expect(Object.isFrozen(error)).toBe(true)
  })
})
