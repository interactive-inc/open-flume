import { describe, it, expect, vi } from "vitest"
import { obtainSlackUrl } from "@/slack/obtain-slack-url"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeHttpError } from "@/errors/http-error"

const createMockDeps = (body: unknown, status = 200, headers?: Record<string, string>) => {
  return {
    fetch: vi.fn().mockResolvedValue({
      text: () => Promise.resolve(typeof body === "string" ? body : JSON.stringify(body)),
      status,
      headers: {
        get: (name: string) => headers?.[name.toLowerCase()] ?? null,
      },
    }),
    now: () => 1000,
  }
}

describe("obtainSlackUrl", () => {
  const appToken = "xapp-test-token"

  it("returns url when response is ok with url", async () => {
    const deps = createMockDeps({ ok: true, url: "wss://example.com" })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBe("wss://example.com")
  })

  it("returns FlumeHttpError when ok is false", async () => {
    const deps = createMockDeps({ ok: false, error: "invalid_auth" })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
  })

  it("carries the Slack error string as code when ok is false", async () => {
    const deps = createMockDeps({ ok: false, error: "invalid_auth" })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.code).toBe("invalid_auth")
    }
  })

  it("returns FlumeHttpError with retryAfterMs on 429 with integer Retry-After", async () => {
    const deps = createMockDeps({ ok: false, error: "ratelimited" }, 429, { "retry-after": "7" })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.status).toBe(429)
      expect(result.retryAfterMs).toBe(7_000)
    }
  })

  it("returns FlumeHttpError with null retryAfterMs on 429 without Retry-After", async () => {
    const deps = createMockDeps({ ok: false, error: "ratelimited" }, 429)

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.status).toBe(429)
      expect(result.retryAfterMs).toBeNull()
    }
  })

  it("treats HTTP-date Retry-After as null retryAfterMs", async () => {
    const deps = createMockDeps({ ok: false, error: "ratelimited" }, 429, {
      "retry-after": "Wed, 21 Oct 2026 07:28:00 GMT",
    })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.retryAfterMs).toBeNull()
    }
  })

  it("does not reject when the mock response has no headers object", async () => {
    const deps = {
      fetch: vi.fn().mockResolvedValue({
        text: () => Promise.resolve(JSON.stringify({ ok: true, url: "wss://x" })),
        status: 200,
      }),
      now: () => 1000,
    }

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBe("wss://x")
  })

  it("returns FlumeHttpError when ok is true but url is missing", async () => {
    const deps = createMockDeps({ ok: true })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
  })

  it("returns FlumeHttpError for invalid response shape", async () => {
    const deps = createMockDeps({ unexpected: "data" })

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
  })

  it("returns FlumeHttpError when body is not JSON, with cause", async () => {
    const deps = createMockDeps("not-json")

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.cause).toBeDefined()
    }
  })

  it("returns FlumeConnectionError when fetch throws (transport failure)", async () => {
    const deps = {
      fetch: vi.fn().mockRejectedValue(new Error("network failure")),
      now: () => 1000,
    }

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeConnectionError)
  })

  it("passes the AbortSignal through to fetch init", async () => {
    const deps = createMockDeps({ ok: true, url: "wss://x" })
    const controller = new AbortController()

    await obtainSlackUrl({ appToken, deps, signal: controller.signal })

    const init = deps.fetch.mock.calls[0]![1]
    expect(init.signal).toBe(controller.signal)
  })
})
