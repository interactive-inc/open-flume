import { describe, it, expect, vi } from "vitest"
import { obtainSlackUrl } from "@/slack/obtain-slack-url"
import { FlumeHttpError } from "@/errors/http-error"

describe("obtainSlackUrl", () => {
  const appToken = "xapp-test-token"

  const createMockDeps = (body: unknown, status = 200) => {
    return {
      fetch: vi.fn().mockResolvedValue({
        json: () => Promise.resolve(body),
        status,
      }),
      now: () => 1000,
    }
  }

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

  it("returns FlumeHttpError with status 0 when fetch throws", async () => {
    const deps = {
      fetch: vi.fn().mockRejectedValue(new Error("network failure")),
      now: () => 1000,
    }

    const result = await obtainSlackUrl({ appToken, deps })

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.status).toBe(0)
    }
  })
})
