import { describe, it, expect } from "vitest"
import { FlumeHttpError } from "@/errors/http-error"

describe("FlumeHttpError", () => {
  it("is instanceof Error", () => {
    const err = new FlumeHttpError({ message: "not found", status: 404 })

    expect(err).toBeInstanceOf(Error)
  })

  it("name is FlumeHttpError", () => {
    const err = new FlumeHttpError({ message: "not found", status: 404 })

    expect(err.name).toBe("FlumeHttpError")
  })

  it("status is set", () => {
    const err = new FlumeHttpError({ message: "forbidden", status: 403 })

    expect(err.status).toBe(403)
  })

  it("message is set", () => {
    const err = new FlumeHttpError({ message: "bad request", status: 400 })

    expect(err.message).toBe("bad request")
  })

  it("is frozen", () => {
    const err = new FlumeHttpError({ message: "error", status: 500 })

    expect(Object.isFrozen(err)).toBe(true)
  })

  it("preserves cause when provided", () => {
    const inner = new Error("ECONNREFUSED")
    const err = new FlumeHttpError({ message: "transport", status: 0, cause: inner })

    expect(err.cause).toBe(inner)
  })
})
