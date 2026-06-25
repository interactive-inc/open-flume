import { describe, it, expect } from "vitest"
import { FlumeConnectionError } from "@/errors/connection-error"

describe("FlumeConnectionError", () => {
  it("is instanceof Error", () => {
    const err = new FlumeConnectionError("fail")

    expect(err).toBeInstanceOf(Error)
  })

  it("name is FlumeConnectionError", () => {
    const err = new FlumeConnectionError("fail")

    expect(err.name).toBe("FlumeConnectionError")
  })

  it("message is set correctly", () => {
    const err = new FlumeConnectionError("timeout")

    expect(err.message).toBe("timeout")
  })

  it("is frozen", () => {
    const err = new FlumeConnectionError("fail")

    expect(Object.isFrozen(err)).toBe(true)
  })

  it("code defaults to null", () => {
    const err = new FlumeConnectionError("fail")

    expect(err.code).toBe(null)
  })

  it("preserves code when provided", () => {
    const err = new FlumeConnectionError("auth", { code: 4004 })

    expect(err.code).toBe(4004)
  })

  it("preserves cause when provided", () => {
    const inner = new Error("inner")
    const err = new FlumeConnectionError("outer", { cause: inner })

    expect(err.cause).toBe(inner)
  })
})
