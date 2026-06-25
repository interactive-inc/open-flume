import { describe, expect, it } from "vitest"
import { FlumeHttpError } from "@/errors/http-error"
import { safeReadText } from "@/utils/safe-read-text"

describe("safeReadText", () => {
  it("returns text on success", async () => {
    const response = { text: () => Promise.resolve("hello"), status: 200 } as unknown as Response
    expect(await safeReadText({ response, context: "test" })).toBe("hello")
  })

  it("returns FlumeHttpError on text() rejection, carrying status and cause", async () => {
    const response = {
      text: () => Promise.reject(new Error("stream broken")),
      status: 502,
    } as unknown as Response

    const result = await safeReadText({ response, context: "ctx" })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.status).toBe(502)
      expect(result.message).toContain("ctx")
      expect(result.cause).toBeInstanceOf(Error)
    }
  })

  it("returns FlumeHttpError when text() throws synchronously", async () => {
    const response = {
      text: () => { throw new Error("sync") },
      status: 500,
    } as unknown as Response

    const result = await safeReadText({ response, context: "ctx" })
    expect(result).toBeInstanceOf(FlumeHttpError)
  })
})
