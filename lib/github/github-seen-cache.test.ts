import { describe, it, expect } from "vitest"
import { FlumeGitHubSeenCache } from "@/github/github-seen-cache"

describe("FlumeGitHubSeenCache", () => {
  it("returns false for unknown id", () => {
    const cache = new FlumeGitHubSeenCache({ maxSize: 10 })

    expect(cache.has("unknown", "2024-01-01")).toBe(false)
  })

  it("returns true after add with same id and updatedAt", () => {
    const cache = new FlumeGitHubSeenCache({ maxSize: 10 })

    cache.add("id-1", "2024-01-01")

    expect(cache.has("id-1", "2024-01-01")).toBe(true)
  })

  it("returns false if updatedAt differs", () => {
    const cache = new FlumeGitHubSeenCache({ maxSize: 10 })

    cache.add("id-1", "2024-01-01")

    expect(cache.has("id-1", "2024-02-01")).toBe(false)
  })

  it("trim removes oldest entries when over maxSize", () => {
    const cache = new FlumeGitHubSeenCache({ maxSize: 2 })

    cache.add("id-1", "2024-01-01")
    cache.add("id-2", "2024-01-02")
    cache.add("id-3", "2024-01-03")
    cache.trim()

    expect(cache.size).toBe(2)
    expect(cache.has("id-1", "2024-01-01")).toBe(false)
    expect(cache.has("id-2", "2024-01-02")).toBe(true)
    expect(cache.has("id-3", "2024-01-03")).toBe(true)
  })

  it("trim does nothing when under maxSize", () => {
    const cache = new FlumeGitHubSeenCache({ maxSize: 5 })

    cache.add("id-1", "2024-01-01")
    cache.add("id-2", "2024-01-02")
    cache.trim()

    expect(cache.size).toBe(2)
  })
})
