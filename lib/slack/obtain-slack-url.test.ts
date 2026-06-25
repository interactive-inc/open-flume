import { describe, it, expect, vi } from "vitest"
import { obtainSlackUrl } from "@/slack/obtain-slack-url"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeHttpError } from "@/errors/http-error"

const createMockDeps = (body: unknown, status = 200) => {
  return {
    fetch: vi.fn().mockResolvedValue({
      text: () => Promise.resolve(typeof body === "string" ? body : JSON.stringify(body)),
      status,
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
