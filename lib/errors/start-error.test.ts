import { describe, it, expect } from "vitest"
import { FlumeStartError } from "@/errors/start-error"

describe("FlumeStartError", () => {
  it("is instanceof Error", () => {
    const err = new FlumeStartError("fail")

    expect(err).toBeInstanceOf(Error)
  })

  it("name is FlumeStartError", () => {
    const err = new FlumeStartError("fail")

    expect(err.name).toBe("FlumeStartError")
  })

  it("message is set correctly", () => {
    const err = new FlumeStartError("aborted")

    expect(err.message).toBe("aborted")
  })

  it("is frozen", () => {
    const err = new FlumeStartError("fail")

    expect(Object.isFrozen(err)).toBe(true)
  })

  it("preserves cause when provided", () => {
    const inner = new Error("inner")
    const err = new FlumeStartError("outer", { cause: inner })

    expect(err.cause).toBe(inner)
  })

  it("cause is undefined when not provided", () => {
    const err = new FlumeStartError("no-cause")

    expect(err.cause).toBeUndefined()
  })
})
