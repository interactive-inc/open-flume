import { describe, it, expect } from "vitest"
import { FlumeDiscordGatewaySession } from "@/discord/discord-gateway-session"

describe("FlumeDiscordGatewaySession", () => {
  it("empty() creates session with all null fields", () => {
    const session = FlumeDiscordGatewaySession.empty()

    expect(session.sessionId).toBe(null)
    expect(session.resumeUrl).toBe(null)
    expect(session.seq).toBe(null)
  })

  it("canResume() returns false for empty session", () => {
    const session = FlumeDiscordGatewaySession.empty()

    expect(session.canResume()).toBe(false)
  })

  it("withReady() sets sessionId and resumeUrl, canResume() returns true", () => {
    const session = FlumeDiscordGatewaySession.empty().withReady(
      "sid-1",
      "wss://resume.example.com",
    )

    expect(session.sessionId).toBe("sid-1")
    expect(session.resumeUrl).toBe("wss://resume.example.com")
    expect(session.canResume()).toBe(true)
  })

  it("withSeq() updates seq preserving other fields", () => {
    const session = FlumeDiscordGatewaySession.empty()
      .withReady("sid-1", "wss://resume.example.com")
      .withSeq(42)

    expect(session.seq).toBe(42)
    expect(session.sessionId).toBe("sid-1")
    expect(session.resumeUrl).toBe("wss://resume.example.com")
  })

  it("withReset() returns empty session", () => {
    const session = FlumeDiscordGatewaySession.empty()
      .withReady("sid-1", "wss://resume.example.com")
      .withSeq(42)
      .withReset()

    expect(session.sessionId).toBe(null)
    expect(session.resumeUrl).toBe(null)
    expect(session.seq).toBe(null)
  })

  it("instances are frozen", () => {
    const session = FlumeDiscordGatewaySession.empty()

    expect(Object.isFrozen(session)).toBe(true)
  })

  it("withSeq creates new instance", () => {
    const original = FlumeDiscordGatewaySession.empty().withReady(
      "sid-1",
      "wss://resume.example.com",
    )
    const updated = original.withSeq(10)

    expect(updated).not.toBe(original)
  })
})
