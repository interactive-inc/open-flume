import { describe, it, expect, vi } from "vitest"
import { FlumeGitHubPoller } from "@/github/github-poller"
import { FlumeHttpError } from "@/errors/http-error"
import type { FlumeLog, FlumeRuntimeDeps } from "@/types"

function makeNotification(id: string, updatedAt: string) {
  return {
    id,
    reason: "mention",
    unread: true,
    updated_at: updatedAt,
    subject: { title: "Test", url: null, type: "Issue" },
    repository: { full_name: "owner/repo" },
  }
}

const timerHandle = globalThis.setTimeout(() => {}, 0)
globalThis.clearTimeout(timerHandle)

function makeJsonResponse(body: unknown, status = 200, extraHeaders: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...extraHeaders },
  })
}

function makeBrokenBodyResponse(headers: Record<string, string>) {
  const stream = new ReadableStream({
    start(controller) {
      controller.error(new Error("body stream failure"))
    },
  })
  return new Response(stream, { status: 200, headers })
}

type Deps = Pick<
  FlumeRuntimeDeps,
  "fetch" | "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout" | "now"
>

function createTestDeps(overrides?: Partial<Deps>) {
  let intervalCallback: (() => void) | null = null

  const mockFetch = vi.fn<Deps["fetch"]>()
  const mockSetInterval = vi.fn((fn: () => void, _ms: number) => {
    intervalCallback = fn
    return timerHandle
  })
  const mockClearInterval = vi.fn()

  const deps: Deps = {
    fetch: mockFetch,
    setInterval: mockSetInterval,
    clearInterval: mockClearInterval,
    setTimeout: vi.fn((_fn: () => void, _ms: number) => timerHandle),
    clearTimeout: vi.fn(),
    now: () => 1000000,
    ...overrides,
  }

  const getIntervalCallback = () => intervalCallback

  return { deps, mockFetch, mockSetInterval, mockClearInterval, getIntervalCallback }
}

function flushPromises() {
  return new Promise<void>((resolve) => globalThis.setTimeout(resolve, 0))
}

describe("FlumeGitHubPoller", () => {
  it.each([200, 401, 403, 503])(
    "retains HTTP %s and request identity when a response fails",
    async (status) => {
      const test = createTestDeps()
      test.mockFetch.mockResolvedValueOnce(
        new Response("invalid JSON", {
          status,
          headers: { "X-GitHub-Request-Id": "test-request" },
        }),
      )
      const poller = new FlumeGitHubPoller({
        token: "test-token",
        interval: 60,
        deps: test.deps,
        onConnected: vi.fn(),
        onDisconnected: vi.fn(),
        onNotifications: vi.fn(),
      })
      const result = await poller.start()
      expect(result).toMatchObject({
        status,
        context: { httpStatus: status, requestId: "test-request", page: 1 },
        errorCode:
          status === 200
            ? "FLUME_PARSE_FAILED"
            : status === 503
              ? "FLUME_HTTP_FAILED"
              : "FLUME_AUTH_FAILED",
      })
      poller.stop()
    },
  )

  it("preserves the original body failure while classifying a 403", async () => {
    const test = createTestDeps()
    const cause = new Error("body interrupted")
    test.mockFetch.mockResolvedValueOnce(
      new Response(
        new ReadableStream({
          start(controller) {
            controller.error(cause)
          },
        }),
        { status: 403 },
      ),
    )
    const poller = new FlumeGitHubPoller({
      token: "test-token",
      interval: 60,
      deps: test.deps,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onNotifications: vi.fn(),
    })
    expect(await poller.start()).toMatchObject({ status: 403, cause: { cause } })
    poller.stop()
  })
  it("start() does initial poll and calls onConnected", async () => {
    const test = createTestDeps()
    const onConnected = vi.fn()
    const onNotifications = vi.fn()

    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("1", "2024-01-01T00:00:00Z")]),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications,
      onConnected,
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    expect(onConnected).toHaveBeenCalledTimes(1)
    expect(onNotifications).not.toHaveBeenCalled()
  })

  it("subsequent polls emit only fresh notifications", async () => {
    const test = createTestDeps()
    const onNotifications = vi.fn()

    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("1", "2024-01-01T00:00:00Z")]),
    )
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("2", "2024-01-02T00:00:00Z")]),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()

    expect(onNotifications).toHaveBeenCalledTimes(1)
    expect(onNotifications.mock.calls[0]![0]).toHaveLength(1)
    expect(onNotifications.mock.calls[0]![0]![0].id).toBe("2")
  })

  it("duplicate notifications are filtered by seen cache", async () => {
    const test = createTestDeps()
    const onNotifications = vi.fn()

    const notification = makeNotification("1", "2024-01-01T00:00:00Z")
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([notification]))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([notification]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()

    expect(onNotifications).not.toHaveBeenCalled()
  })

  it("updated notifications (same id, different updated_at) are emitted", async () => {
    const test = createTestDeps()
    const onNotifications = vi.fn()

    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("1", "2024-01-01T00:00:00Z")]),
    )
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("1", "2024-01-02T00:00:00Z")]),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()

    expect(onNotifications).toHaveBeenCalledTimes(1)
    expect(onNotifications.mock.calls[0]![0]![0].updated_at).toBe("2024-01-02T00:00:00Z")
  })

  it("HTTP error increments consecutiveErrors and calls onDisconnected after 3", async () => {
    const test = createTestDeps()
    const onDisconnected = vi.fn()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200))
    test.mockFetch.mockResolvedValue(makeJsonResponse(null, 500))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected,
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()

    cb!()
    await flushPromises()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()

    expect(onDisconnected).toHaveBeenCalledWith("HTTP 500")
  })

  it("network error increments consecutiveErrors and calls onDisconnected after 3", async () => {
    const test = createTestDeps()
    const onDisconnected = vi.fn()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200))
    test.mockFetch.mockRejectedValue(new Error("network failure"))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected,
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()

    cb!()
    await flushPromises()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()

    expect(onDisconnected).toHaveBeenCalledWith("network failure")
  })

  it("stop() clears interval and sets stopped", async () => {
    const test = createTestDeps()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()
    poller.stop()

    expect(test.mockClearInterval).toHaveBeenCalledWith(timerHandle)
    expect(poller.isStopped).toBe(true)
  })

  it("sends correct Authorization header", async () => {
    const test = createTestDeps()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_secret123",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    const fetchCall = test.mockFetch.mock.calls[0]!
    const init = fetchCall[1]
    const headers = init?.headers
    expect(headers).toEqual(expect.objectContaining({ Authorization: "Bearer ghp_secret123" }))
  })

  it("429 with Retry-After pauses polling for that many seconds", async () => {
    const mockSetTimeout = vi.fn((_fn: () => void, _ms: number) => timerHandle)
    const test = createTestDeps({ setTimeout: mockSetTimeout })

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(
      new Response("", { status: 429, headers: { "Retry-After": "5" } }),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    test.getIntervalCallback()!()
    await flushPromises()

    expect(test.mockClearInterval).toHaveBeenCalledWith(timerHandle)
    expect(mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 5000)
  })

  it("403 with X-RateLimit-Remaining 0 computes delay from X-RateLimit-Reset", async () => {
    const mockSetTimeout = vi.fn((_fn: () => void, _ms: number) => timerHandle)
    const test = createTestDeps({ setTimeout: mockSetTimeout, now: () => 1_000_000 })

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(
      new Response("", {
        status: 403,
        headers: { "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1100" },
      }),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    test.getIntervalCallback()!()
    await flushPromises()

    // now=1_000_000ms → 1000s。reset=1100s → 100s 後
    expect(mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 100_000)
  })

  it("detects a body-only secondary rate limit and pauses for 60 seconds", async () => {
    const mockSetTimeout = vi.fn((_fn: () => void, _ms: number) => timerHandle)
    const test = createTestDeps({ setTimeout: mockSetTimeout })

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse({ message: "You have exceeded a secondary rate limit." }, 403, {
        "X-RateLimit-Remaining": "42",
      }),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()
    test.getIntervalCallback()!()
    await flushPromises()

    expect(mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 60_000)
    expect(test.mockClearInterval).toHaveBeenCalledWith(timerHandle)
  })

  it("rate limit without usable headers defaults to 60s", async () => {
    const mockSetTimeout = vi.fn((_fn: () => void, _ms: number) => timerHandle)
    const test = createTestDeps({ setTimeout: mockSetTimeout })

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(new Response("", { status: 429 }))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    test.getIntervalCallback()!()
    await flushPromises()

    expect(mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 60_000)
  })

  it("rate limit does not count as a failure (no onDisconnected)", async () => {
    const test = createTestDeps()
    const onDisconnected = vi.fn()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValue(new Response("", { status: 429 }))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected,
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()

    expect(onDisconnected).not.toHaveBeenCalled()
  })

  it("resumes polling when the rate-limit timer fires", async () => {
    let rateTimerCallback: (() => void) | null = null
    const mockSetTimeout = vi.fn((fn: () => void, _ms: number) => {
      rateTimerCallback = fn
      return timerHandle
    })
    const test = createTestDeps({ setTimeout: mockSetTimeout })

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(new Response("", { status: 429 }))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()
    expect(test.mockSetInterval).toHaveBeenCalledTimes(1)

    test.getIntervalCallback()!()
    await flushPromises()

    rateTimerCallback!()
    expect(test.mockSetInterval).toHaveBeenCalledTimes(2)
  })

  it("rate limit on the initial poll does not start the interval immediately", async () => {
    const mockSetTimeout = vi.fn((_fn: () => void, _ms: number) => timerHandle)
    const test = createTestDeps({ setTimeout: mockSetTimeout })

    test.mockFetch.mockResolvedValueOnce(new Response("", { status: 429 }))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    // 一時停止タイマーだけが張られ、interval は張られない
    expect(mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 60_000)
    expect(test.mockSetInterval).not.toHaveBeenCalled()
  })

  it("does not start the interval when rate-limit timer scheduling fails", async () => {
    const mockSetTimeout = vi.fn((_fn: () => void, ms: number) => {
      if (ms === 60_000) throw new Error("timer unavailable")
      return timerHandle
    })
    const test = createTestDeps({ setTimeout: mockSetTimeout })
    const onDisconnected = vi.fn()

    test.mockFetch.mockResolvedValueOnce(new Response("", { status: 429 }))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected,
      deps: test.deps,
    })

    await poller.start()

    expect(test.mockSetInterval).not.toHaveBeenCalled()
    expect(onDisconnected).toHaveBeenCalledWith("rate-limit timer scheduling rejected by runtime")
  })

  it("does not resume polling if stopped before the rate-limit timer fires", async () => {
    let rateTimerCallback: (() => void) | null = null
    const mockSetTimeout = vi.fn((fn: () => void, _ms: number) => {
      rateTimerCallback = fn
      return timerHandle
    })
    const test = createTestDeps({ setTimeout: mockSetTimeout })

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(new Response("", { status: 429 }))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()
    test.getIntervalCallback()!()
    await flushPromises()

    poller.stop()
    rateTimerCallback!()

    expect(test.mockSetInterval).toHaveBeenCalledTimes(1)
  })

  it("follows Link rel=next, aggregates pages, and sends conditional headers only on page 1", async () => {
    const test = createTestDeps()
    const onNotifications = vi.fn()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200, { ETag: 'W/"etag-1"' }))
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("2", "2024-01-02T00:00:00Z")], 200, {
        Link: '<https://api.github.com/notifications?page=2&per_page=50>; rel="next"',
      }),
    )
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("3", "2024-01-03T00:00:00Z")]),
    )

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    test.getIntervalCallback()!()
    await flushPromises()

    expect(onNotifications).toHaveBeenCalledTimes(1)
    expect(onNotifications.mock.calls[0]![0]).toHaveLength(2)
    expect(onNotifications.mock.calls[0]![0]![0].id).toBe("2")
    expect(onNotifications.mock.calls[0]![0]![1].id).toBe("3")

    expect(test.mockFetch.mock.calls[2]![0]).toBe(
      "https://api.github.com/notifications?page=2&per_page=50",
    )
    expect(test.mockFetch.mock.calls[1]![1]?.headers).toEqual(
      expect.objectContaining({ "If-None-Match": 'W/"etag-1"' }),
    )

    const pageTwoHeaders = test.mockFetch.mock.calls[2]![1]?.headers
    expect(pageTwoHeaders).not.toHaveProperty("If-None-Match")
    expect(pageTwoHeaders).not.toHaveProperty("If-Modified-Since")
  })

  it("continues past 10 pages without committing a truncated result", async () => {
    const test = createTestDeps()
    const logs: FlumeLog[] = []
    let page = 0

    test.mockFetch.mockImplementation(() => {
      page += 1
      const headers: Record<string, string> =
        page < 11
          ? { Link: `<https://api.github.com/notifications?page=${page + 1}>; rel="next"` }
          : {}
      return Promise.resolve(makeJsonResponse([], 200, headers))
    })

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onLog: (log) => logs.push(log),
      deps: test.deps,
    })

    await poller.start()

    expect(test.mockFetch).toHaveBeenCalledTimes(11)

    const truncatedWarns = logs.filter((log) => log.action === "poll.pages.truncated")
    expect(truncatedWarns).toHaveLength(0)
  })

  it("does not commit validators when body read fails, next poll sends the previous ETag", async () => {
    const test = createTestDeps()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200, { ETag: 'W/"good"' }))
    test.mockFetch.mockResolvedValueOnce(makeBrokenBodyResponse({ ETag: 'W/"bad"' }))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()

    expect(test.mockFetch.mock.calls[2]![1]?.headers).toEqual(
      expect.objectContaining({ "If-None-Match": 'W/"good"' }),
    )
  })

  it("re-emits connected after recovering from disconnected", async () => {
    const test = createTestDeps()
    const onConnected = vi.fn()
    const onDisconnected = vi.fn()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse(null, 500))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse(null, 500))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse(null, 500))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected,
      onDisconnected,
      deps: test.deps,
    })

    await poller.start()
    expect(onConnected).toHaveBeenCalledTimes(1)

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()
    cb!()
    await flushPromises()

    expect(onDisconnected).toHaveBeenCalledTimes(1)

    cb!()
    await flushPromises()

    expect(onConnected).toHaveBeenCalledTimes(2)
  })

  it("403 with Retry-After and nonzero remaining pauses, then polls immediately after the pause", async () => {
    let rateTimerCallback: (() => void) | null = null
    const mockSetTimeout = vi.fn((fn: () => void, _ms: number) => {
      rateTimerCallback = fn
      return timerHandle
    })
    const test = createTestDeps({ setTimeout: mockSetTimeout })
    const onDisconnected = vi.fn()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    test.mockFetch.mockResolvedValueOnce(
      new Response("", {
        status: 403,
        headers: { "Retry-After": "7", "X-RateLimit-Remaining": "42" },
      }),
    )
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 60,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected,
      deps: test.deps,
    })

    await poller.start()

    test.getIntervalCallback()!()
    await flushPromises()

    expect(onDisconnected).not.toHaveBeenCalled()
    expect(mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 7000)
    expect(test.mockFetch).toHaveBeenCalledTimes(2)

    rateTimerCallback!()
    await flushPromises()

    // 一時停止明けは interval 1 周分を待たずに即 1 回 poll し、interval も再開する
    expect(test.mockFetch).toHaveBeenCalledTimes(3)
    expect(test.mockSetInterval).toHaveBeenCalledTimes(2)
  })

  it("follows X-Poll-Interval in both directions with the user interval as floor", async () => {
    const test = createTestDeps()

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200, { "X-Poll-Interval": "120" }))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200, { "X-Poll-Interval": "10" }))
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const poller = new FlumeGitHubPoller({
      token: "ghp_test",
      interval: 30,
      onNotifications: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps: test.deps,
    })

    await poller.start()

    // 初回 poll がヘッダ 120s を採用し、start() が 120s で interval を張る
    expect(test.mockSetInterval).toHaveBeenLastCalledWith(expect.any(Function), 120_000)

    test.getIntervalCallback()!()
    await flushPromises()

    // ヘッダ 10s はユーザー下限 30s でクランプしつつ狭める方向にも追従する
    expect(test.mockSetInterval).toHaveBeenLastCalledWith(expect.any(Function), 30_000)

    test.getIntervalCallback()!()
    await flushPromises()

    // ヘッダ無しは現在の実効値を維持する (再スケジュールなし)
    expect(test.mockSetInterval).toHaveBeenCalledTimes(2)
  })
})

describe("notification transaction regressions", () => {
  it("keeps HTTP metadata and the original failure when adding request context", async () => {
    const test = createTestDeps()
    const failure = new FlumeHttpError({
      message: "request rejected",
      status: 503,
      code: "service_unavailable",
      retryAfterMs: 1000,
    })
    test.mockFetch.mockRejectedValueOnce(failure)
    const poller = new FlumeGitHubPoller({
      token: "test-token",
      interval: 60,
      deps: test.deps,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onNotifications: vi.fn(),
    })
    expect(await poller.start()).toMatchObject({
      status: 503,
      code: "service_unavailable",
      retryAfterMs: 1000,
      cause: failure,
      errorCode: "FLUME_HTTP_FAILED",
      context: { operation: "github.notifications", phase: "request", page: 1 },
    })
    poller.stop()
  })

  it.each([
    "https://example.com/notifications?page=2",
    "https://api.github.com/other?page=2",
    "http://api.github.com/notifications?page=2",
    "https://user:secret@api.github.com/notifications?page=2",
  ])("rejects an untrusted next URL without forwarding the token: %s", async (next) => {
    const test = createTestDeps()
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([], 200, { Link: `<${next}>; rel="next"` }),
    )
    const poller = new FlumeGitHubPoller({
      token: "test-token",
      interval: 60,
      deps: test.deps,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onNotifications: vi.fn(),
    })
    expect(await poller.start()).toBeInstanceOf(Error)
    expect(test.mockFetch).toHaveBeenCalledTimes(1)
    expect(test.mockFetch.mock.calls[0]?.[1]?.redirect).toBe("error")
    poller.stop()
  })

  it.each(["{broken", "{}", '[{"id":"incomplete"}]'])(
    "rejects malformed bootstrap bodies: %s",
    async (body) => {
      const test = createTestDeps()
      test.mockFetch.mockResolvedValueOnce(new Response(body))
      const connected = vi.fn()
      const poller = new FlumeGitHubPoller({
        token: "test-token",
        interval: 60,
        deps: test.deps,
        onConnected: connected,
        onDisconnected: vi.fn(),
        onNotifications: vi.fn(),
      })
      expect(await poller.start()).toBeInstanceOf(Error)
      expect(connected).not.toHaveBeenCalled()
      poller.stop()
    },
  )

  it("does not commit a partial page or validator after schema failure", async () => {
    const test = createTestDeps()
    const delivered = vi.fn()
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([], 200, { ETag: '"previous"' }))
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("new", "2026-01-01T00:00:00Z")], 200, {
        ETag: '"partial"',
        Link: '<https://api.github.com/notifications?page=2>; rel="next"',
      }),
    )
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([{ id: "invalid" }]))
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("new", "2026-01-01T00:00:00Z")]),
    )
    const poller = new FlumeGitHubPoller({
      token: "test-token",
      interval: 60,
      deps: test.deps,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onNotifications: delivered,
    })
    await poller.start()
    test.getIntervalCallback()?.()
    await flushPromises()
    expect(delivered).not.toHaveBeenCalled()
    test.getIntervalCallback()?.()
    await flushPromises()
    expect(test.mockFetch.mock.calls[3]?.[1]?.headers).toMatchObject({
      "If-None-Match": '"previous"',
    })
    expect(delivered.mock.calls[0]?.[0]).toHaveLength(1)
    poller.stop()
  })

  it.each(["fetch", "body"])("bounds a hung %s and aborts the request", async (phase) => {
    const timers: Array<() => void> = []
    const test = createTestDeps({
      setTimeout: (fn) => {
        timers.push(fn)
        return timers.length
      },
    })
    if (phase === "fetch") test.mockFetch.mockImplementation(() => new Promise(() => {}))
    else test.mockFetch.mockResolvedValue(new Response(new ReadableStream()))
    const poller = new FlumeGitHubPoller({
      token: "test-token",
      interval: 60,
      deps: test.deps,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      onNotifications: vi.fn(),
    })
    const starting = poller.start()
    await flushPromises()
    timers[0]?.()
    const result = await starting
    expect(result).toMatchObject({
      errorCode: "FLUME_TIMEOUT",
      context: {
        operation: "github.notifications",
        page: 1,
        phase: phase === "fetch" ? "request" : "response body",
      },
    })
    expect(test.mockFetch.mock.calls[0]?.[1]?.signal?.aborted).toBe(true)
    poller.stop()
  })

  it("counts malformed bodies as failures and reports recovery", async () => {
    const test = createTestDeps()
    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))
    for (let index = 0; index < 3; index++)
      test.mockFetch.mockResolvedValueOnce(new Response("bad JSON"))
    test.mockFetch.mockResolvedValueOnce(new Response(null, { status: 304 }))
    const connected = vi.fn()
    const disconnected = vi.fn()
    const poller = new FlumeGitHubPoller({
      token: "test-token",
      interval: 60,
      deps: test.deps,
      onConnected: connected,
      onDisconnected: disconnected,
      onNotifications: vi.fn(),
    })
    await poller.start()
    for (let index = 0; index < 3; index++) {
      test.getIntervalCallback()?.()
      await flushPromises()
    }
    expect(disconnected).toHaveBeenCalledTimes(1)
    test.getIntervalCallback()?.()
    await flushPromises()
    expect(connected).toHaveBeenCalledTimes(2)
    poller.stop()
  })
})
