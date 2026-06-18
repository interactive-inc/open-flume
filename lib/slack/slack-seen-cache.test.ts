import { describe, it, expect } from "vitest"
import { FlumeSlackSeenCache } from "@/slack/slack-seen-cache"

describe("FlumeSlackSeenCache", () => {
  it("reports unseen ids as false, seen as true", () => {
    const cache = new FlumeSlackSeenCache({ maxSize: 10 })

    expect(cache.has("e1")).toBe(false)
    cache.add("e1")
    expect(cache.has("e1")).toBe(true)
  })

  it("trims down to maxSize, evicting oldest", () => {
    const cache = new FlumeSlackSeenCache({ maxSize: 3 })

    cache.add("a")
    cache.add("b")
    cache.add("c")
    cache.add("d")
    cache.add("e")
    cache.trim()

    expect(cache.size).toBe(3)
    expect(cache.has("a")).toBe(false)
    expect(cache.has("b")).toBe(false)
    expect(cache.has("c")).toBe(true)
    expect(cache.has("d")).toBe(true)
    expect(cache.has("e")).toBe(true)
  })
})
