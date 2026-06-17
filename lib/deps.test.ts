import { describe, it, expect } from "vitest"
import { createFlumeDefaultDeps } from "@/deps"

describe("createFlumeDefaultDeps", () => {
  it("returns object with all required fields", () => {
    const deps = createFlumeDefaultDeps()
    expect(typeof deps.fetch).toBe("function")
    expect(typeof deps.now).toBe("function")
    expect(typeof deps.random).toBe("function")
    expect(typeof deps.setTimeout).toBe("function")
    expect(typeof deps.clearTimeout).toBe("function")
    expect(typeof deps.setInterval).toBe("function")
    expect(typeof deps.clearInterval).toBe("function")
  })

  it("now() returns a timestamp", () => {
    const deps = createFlumeDefaultDeps()
    const now = deps.now()
    expect(typeof now).toBe("number")
    expect(now).toBeGreaterThan(0)
  })

  it("random() returns a number between 0 and 1", () => {
    const deps = createFlumeDefaultDeps()
    const val = deps.random()
    expect(val).toBeGreaterThanOrEqual(0)
    expect(val).toBeLessThan(1)
  })
})
