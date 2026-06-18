import { describe, it, expect, vi } from "vitest"
import type { FlumeRuntimeDeps } from "@/types"
import { FlumeLogger } from "@/logger"
import { safeFetch } from "@/utils/safe-fetch"

function createLog(): FlumeLogger {
  const deps: Pick<FlumeRuntimeDeps, "now"> = { now: () => 1 }
  return new FlumeLogger({ source: "test", deps, handler: () => {} })
}

describe("safeFetch", () => {
  it("returns the Response on success", async () => {
    const response = new Response("ok")
    const fetch = vi.fn().mockResolvedValue(response)

    const result = await safeFetch({ fetch, url: "https://x", log: createLog() })

    expect(result).toBe(response)
  })

  it("returns Error on network failure", async () => {
    const fetch = vi.fn().mockRejectedValue(new Error("ECONNREFUSED"))

    const result = await safeFetch({ fetch, url: "https://x", log: createLog() })

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toBe("ECONNREFUSED")
  })

  it("wraps non-Error thrown values", async () => {
    const fetch = vi.fn().mockRejectedValue("string thrown")

    const result = await safeFetch({ fetch, url: "https://x", log: createLog() })

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toBe("string thrown")
  })
})
