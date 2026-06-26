import { describe, it, expect, vi } from "vitest"
import { FlumeGitHubPoller } from "@/github/github-poller"
import type { FlumeRuntimeDeps } from "@/types"

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

function makeJsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })
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

    expect(onDisconnected).toHaveBeenCalledWith("network error")
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
})
