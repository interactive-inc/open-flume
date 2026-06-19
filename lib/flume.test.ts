import { describe, it, expect, vi } from "vitest"
import type { FlumeEvent, FlumeHandler, FlumeSource, FlumeStatus } from "@/types"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { FlumeStopped } from "@/flume-stopped"

type MockSource = FlumeSource & {
  emit: (event: FlumeEvent) => void
  startCount: number
  stopCount: number
}

function createMockSource(options?: {
  failOnStart?: Error
  returnErrorOnStart?: Error
  name?: "discord" | "slack" | "github"
}): MockSource {
  let handler: FlumeHandler | null = null
  let currentStatus: FlumeStatus = "disconnected"
  const mock: MockSource = {
    name: options?.name ?? "discord",
    startCount: 0,
    stopCount: 0,
    async start(h: FlumeHandler): Promise<Error | null> {
      mock.startCount += 1
      if (options?.failOnStart) throw options.failOnStart
      if (options?.returnErrorOnStart) {
        return options.returnErrorOnStart
      }
      handler = h
      currentStatus = "connected"
      return null
    },
    async stop(): Promise<void> {
      mock.stopCount += 1
      currentStatus = "disconnected"
    },
    status(): FlumeStatus {
      return currentStatus
    },
    emit(event: FlumeEvent): void {
      handler?.(event)
    },
  }

  return mock
}

describe("Flume", () => {
  it("start returns FlumeRunning and propagates handler", async () => {
    const a = createMockSource({ name: "discord" })
    const b = createMockSource({ name: "slack" })
    const flume = new Flume({ sources: [a, b] })
    const handler = vi.fn()

    const result = await flume.start(handler)

    expect(result).toBeInstanceOf(FlumeRunning)
    expect(a.startCount).toBe(1)
    expect(b.startCount).toBe(1)
  })

  it("merges events from every source into one stream", async () => {
    const a = createMockSource()
    const b = createMockSource()
    const flume = new Flume({ sources: [a, b] })
    const handler = vi.fn()
    await flume.start(handler)

    a.emit({ source: "discord", type: "x", data: {}, meta: {}, receivedAt: 1 })
    b.emit({ source: "slack", type: "y", data: {}, meta: {}, receivedAt: 2 })

    expect(handler).toHaveBeenCalledTimes(2)
  })

  it("rolls back started sources when one throws", async () => {
    const a = createMockSource()
    const b = createMockSource({ failOnStart: new Error("boom"), name: "slack" })
    const flume = new Flume({ sources: [a, b] })

    const result = await flume.start(vi.fn())

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toContain("slack: boom")
    expect(a.stopCount).toBe(1)
  })

  it("rolls back started sources when one returns error", async () => {
    const a = createMockSource()
    const b = createMockSource({ returnErrorOnStart: new Error("connect refused"), name: "slack" })
    const flume = new Flume({ sources: [a, b] })

    const result = await flume.start(vi.fn())

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toContain("slack: connect refused")
    expect(a.stopCount).toBe(1)
  })

  it("refuses double-start", async () => {
    const a = createMockSource()
    const flume = new Flume({ sources: [a] })
    await flume.start(vi.fn())

    const second = await flume.start(vi.fn())

    expect(second).toBeInstanceOf(Error)
    expect(a.startCount).toBe(1)
  })

  it("refuses start if signal already aborted", async () => {
    const a = createMockSource()
    const controller = new AbortController()
    controller.abort()
    const flume = new Flume({ sources: [a], signal: controller.signal })

    const result = await flume.start(vi.fn())

    expect(result).toBeInstanceOf(Error)
    expect(a.startCount).toBe(0)
  })
})

describe("FlumeRunning", () => {
  it("stop returns FlumeStopped and stops every source", async () => {
    const a = createMockSource()
    const b = createMockSource()
    const flume = new Flume({ sources: [a, b] })

    const running = await flume.start(vi.fn())
    if (running instanceof Error) throw running

    const stopped = await running.stop()

    expect(stopped).toBeInstanceOf(FlumeStopped)
    expect(a.stopCount).toBe(1)
    expect(b.stopCount).toBe(1)
  })

  it("stop is idempotent and concurrent-safe", async () => {
    const a = createMockSource()
    const flume = new Flume({ sources: [a] })

    const running = await flume.start(vi.fn())
    if (running instanceof Error) throw running

    const [first, second] = await Promise.all([running.stop(), running.stop()])

    expect(first).toBe(second)
    expect(a.stopCount).toBe(1)
  })

  it("aborts all sources when signal fires", async () => {
    const a = createMockSource()
    const controller = new AbortController()
    const flume = new Flume({ sources: [a], signal: controller.signal })
    await flume.start(vi.fn())

    controller.abort()
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(a.stopCount).toBe(1)
  })

  it("statuses reflects underlying source state", async () => {
    const a = createMockSource({ name: "discord" })
    const b = createMockSource({ name: "slack" })
    const flume = new Flume({ sources: [a, b] })

    const running = await flume.start(vi.fn())
    if (running instanceof Error) throw running

    expect(running.statuses()).toEqual([
      { name: "discord", status: "connected" },
      { name: "slack", status: "connected" },
    ])
  })
})

describe("FlumeStopped", () => {
  it("exposes a snapshot of final statuses without raw sources", async () => {
    const a = createMockSource({ name: "discord" })
    const flume = new Flume({ sources: [a] })

    const running = await flume.start(vi.fn())
    if (running instanceof Error) throw running

    const stopped = await running.stop()

    expect(stopped.statuses()).toEqual([{ name: "discord", status: "disconnected" }])
  })
})
