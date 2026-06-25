import { describe, it, expect } from "vitest"
import { FlumeSlackSeenCache } from "@/slack/slack-seen-cache"

const fixedDeps = (nowValue: { value: number }) => ({ now: () => nowValue.value })

describe("FlumeSlackSeenCache", () => {
  it("reports unseen ids as false, seen as true", () => {
    const now = { value: 1000 }
    const cache = new FlumeSlackSeenCache({ maxSize: 10, ttlMs: 60_000, deps: fixedDeps(now) })

    expect(cache.has("e1")).toBe(false)
    cache.add("e1")
    expect(cache.has("e1")).toBe(true)
  })

  it("expires entries after the TTL elapses", () => {
    const now = { value: 1000 }
    const cache = new FlumeSlackSeenCache({ maxSize: 10, ttlMs: 60_000, deps: fixedDeps(now) })

    cache.add("e1")
    expect(cache.has("e1")).toBe(true)

    now.value = 1000 + 60_001

    expect(cache.has("e1")).toBe(false)
  })

  it("trim drops expired entries first then enforces maxSize by insertion order", () => {
    const now = { value: 0 }
    const cache = new FlumeSlackSeenCache({ maxSize: 3, ttlMs: 1000, deps: fixedDeps(now) })

    cache.add("old-1")
    cache.add("old-2")
    now.value = 2000
    cache.add("fresh-a")
    cache.add("fresh-b")
    cache.add("fresh-c")
    cache.add("fresh-d")
    cache.trim()

    expect(cache.size).toBe(3)
    expect(cache.has("old-1")).toBe(false)
    expect(cache.has("old-2")).toBe(false)
    expect(cache.has("fresh-b")).toBe(true)
    expect(cache.has("fresh-c")).toBe(true)
    expect(cache.has("fresh-d")).toBe(true)
  })
})
