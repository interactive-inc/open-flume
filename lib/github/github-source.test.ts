import { describe, it, expect, vi } from "vitest"
import { FlumeGitHubSource } from "@/github/github-source"
import { flumeExtractGitHubMeta } from "@/github/extract-github-meta"
import type { FlumeEvent, FlumeRuntimeDeps, FlumeStatus } from "@/types"

const timerHandle = globalThis.setTimeout(() => {}, 0)
globalThis.clearTimeout(timerHandle)

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

function makeJsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })
}

function createMockDeps() {
  let intervalCallback: (() => void) | null = null

  const mockFetch = vi.fn<FlumeRuntimeDeps["fetch"]>()

  const deps: FlumeRuntimeDeps = {
    fetch: mockFetch,
    now: () => 1000000,
    setInterval: vi.fn((fn: () => void, _ms: number) => {
      intervalCallback = fn
      return timerHandle
    }),
    clearInterval: vi.fn(),
    setTimeout: vi.fn((_fn: () => void, _ms: number) => timerHandle),
    clearTimeout: vi.fn(),
    random: () => 0.5,
    WebSocket: globalThis.WebSocket,
  }

  const getIntervalCallback = () => intervalCallback

  return { deps, mockFetch, getIntervalCallback }
}

function flushPromises() {
  return new Promise<void>((resolve) => globalThis.setTimeout(resolve, 0))
}

describe("FlumeGitHubSource", () => {
  it("start() creates poller and forwards notifications as FlumeEvents", async () => {
    const test = createMockDeps()
    const receivedEvents: FlumeEvent[] = []

    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("1", "2024-01-01T00:00:00Z")]),
    )
    test.mockFetch.mockResolvedValueOnce(
      makeJsonResponse([makeNotification("2", "2024-01-02T00:00:00Z")]),
    )

    const source = new FlumeGitHubSource({
      token: "ghp_test",
      pollInterval: 60,
      deps: test.deps,
    })

    await source.start((event) => {
      receivedEvents.push(event)
    })

    const cb = test.getIntervalCallback()
    cb!()
    await flushPromises()

    expect(receivedEvents).toHaveLength(1)
    expect(receivedEvents[0]!.source).toBe("github")
    expect(receivedEvents[0]!.type).toBe("notification")
    expect(receivedEvents[0]!.meta.repository).toBe("owner/repo")
  })

  it("stop() stops poller and sets status to disconnected", async () => {
    const test = createMockDeps()
    const statuses: FlumeStatus[] = []

    test.mockFetch.mockResolvedValueOnce(makeJsonResponse([]))

    const source = new FlumeGitHubSource({
      token: "ghp_test",
      pollInterval: 60,
      deps: test.deps,
      onStatus: (status) => {
        statuses.push(status)
      },
    })

    await source.start(vi.fn())
    await source.stop()

    expect(source.status()).toBe("disconnected")
    expect(statuses).toContain("disconnected")
  })

  it("status() returns disconnected initially", () => {
    const test = createMockDeps()

    const source = new FlumeGitHubSource({
      token: "ghp_test",
      deps: test.deps,
    })

    expect(source.status()).toBe("disconnected")
  })

  it("aborted signal prevents start", async () => {
    const test = createMockDeps()
    const controller = new AbortController()
    controller.abort()

    const source = new FlumeGitHubSource({
      token: "ghp_test",
      deps: test.deps,
      signal: controller.signal,
    })

    await source.start(vi.fn())

    expect(test.mockFetch).not.toHaveBeenCalled()
  })
})

describe("flumeExtractGitHubMeta", () => {
  it("extracts all fields from notification", () => {
    const meta = flumeExtractGitHubMeta({
      id: "123",
      reason: "mention",
      unread: true,
      updated_at: "2024-01-01T00:00:00Z",
      subject: { title: "Bug", url: null, type: "Issue" },
      repository: { full_name: "owner/repo" },
    })
    expect(meta.event_type).toBe("notification")
    expect(meta.reason).toBe("mention")
    expect(meta.subject_type).toBe("Issue")
    expect(meta.repository).toBe("owner/repo")
    expect(meta.thread_id).toBe("123")
  })
})
