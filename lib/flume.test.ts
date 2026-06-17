import { describe, it, expect, vi } from "vitest"
import { Flume } from "@/flume"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { FlumeSlackSource } from "@/slack/slack-source"
import { FlumeGitHubSource } from "@/github/github-source"

const mockWebSocket = vi.fn()
Object.assign(mockWebSocket, { OPEN: 1, CLOSED: 3 })

const mockDeps = {
  fetch: vi.fn(),
  WebSocket: mockWebSocket,
  now: () => 1000,
  random: () => 0.5,
  setTimeout: vi.fn(),
  clearTimeout: vi.fn(),
  setInterval: vi.fn(),
  clearInterval: vi.fn(),
}

describe("Flume", () => {
  it("discord returns a FlumeDiscordSource instance", () => {
    const flume = new Flume({ deps: mockDeps })

    const source = flume.discord({ token: "test-token" })

    expect(source).toBeInstanceOf(FlumeDiscordSource)
  })

  it("slack returns a FlumeSlackSource instance", () => {
    const flume = new Flume({ deps: mockDeps })

    const source = flume.slack({ appToken: "xapp-test" })

    expect(source).toBeInstanceOf(FlumeSlackSource)
  })

  it("github returns a FlumeGitHubSource instance", () => {
    const flume = new Flume({ deps: mockDeps })

    const source = flume.github({ token: "ghp-test" })

    expect(source).toBeInstanceOf(FlumeGitHubSource)
  })
})
