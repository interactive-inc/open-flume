import { describe, expect, it } from "vitest"
import { attempt } from "@/utils/attempt"

describe("attempt (sync)", () => {
  it("returns the value on success", () => {
    expect(attempt(() => 42)).toBe(42)
  })

  it("returns Error when fn throws", () => {
    const result = attempt(() => {
      throw new Error("boom")
    })
    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toBe("boom")
  })

  it("normalizes non-Error throws to Error", () => {
    const result = attempt((): number => {
      throw "raw"
    })
    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toBe("raw")
  })

  it("captures throws from new", () => {
    class Bad {
      constructor() {
        throw new TypeError("nope")
      }
    }
    const result = attempt(() => new Bad())
    expect(result).toBeInstanceOf(Error)
  })
})

describe("attempt (async)", () => {
  it("returns the resolved value on success", async () => {
    expect(await attempt(async () => 42)).toBe(42)
  })

  it("returns Error when promise rejects", async () => {
    const result = await attempt(async () => {
      throw new Error("boom")
    })
    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toBe("boom")
  })

  it("returns Error when fn throws synchronously before returning Promise", async () => {
    const result = await attempt<number>((): Promise<number> => {
      throw new Error("sync")
    })
    expect(result).toBeInstanceOf(Error)
  })

  it("normalizes non-Error rejections to Error", async () => {
    const result = await attempt(async (): Promise<number> => {
      throw "string"
    })
    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toBe("string")
  })

  it("handles a function that returns a Promise (not async)", async () => {
    const result = await attempt(() => Promise.resolve(7))
    expect(result).toBe(7)
  })
})
