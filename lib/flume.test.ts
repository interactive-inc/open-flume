import { describe, it, expect, vi } from "vitest"
import type { FlumeEvent, FlumeSourceStartContext, FlumeStatusEvent } from "@/types"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { FlumeStopped } from "@/flume-stopped"
import { FlumeSource } from "@/flume-source"

type MockOptions = {
  failOnConnect?: Error
  returnErrorOnConnect?: Error
  name?: "discord" | "slack" | "github"
}

class MockSource extends FlumeSource {
  readonly name: "discord" | "slack" | "github"

  startCount = 0

  stopCount = 0

  pushEvent: ((event: FlumeEvent) => void) | null = null

  constructor(private readonly mockOptions: MockOptions = {}) {
    super()
    this.name = mockOptions.name ?? "discord"
  }

  protected async connect(_ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.startCount += 1
    if (this.mockOptions.failOnConnect) throw this.mockOptions.failOnConnect
    if (this.mockOptions.returnErrorOnConnect) return this.mockOptions.returnErrorOnConnect

    this.setStatus("connected")
    this.pushEvent = (event) => this.emit(event)
    return null
  }

  protected disconnect(): void {
    this.stopCount += 1
    this.pushEvent = null
  }
}

describe("Flume", () => {
  it("constructs with only sources (no options)", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume([a])

    const result = await flume.start()

    expect(result).toBeInstanceOf(FlumeRunning)
    expect(a.startCount).toBe(1)
  })

  it("drops events silently when onEvent is omitted", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume([a])
    await flume.start()

    a.pushEvent!({ source: "discord", type: "x", data: {}, meta: {}, receivedAt: 1 })

    expect(a.startCount).toBe(1)
  })

  it("start returns FlumeRunning and starts every source", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const flume = new Flume([a, b], { onEvent: vi.fn() })

    const result = await flume.start()

    expect(result).toBeInstanceOf(FlumeRunning)
    expect(a.startCount).toBe(1)
    expect(b.startCount).toBe(1)
  })

  it("merges events from every source into one stream", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const onEvent = vi.fn()
    const flume = new Flume([a, b], { onEvent })
    await flume.start()

    a.pushEvent!({ source: "discord", type: "x", data: {}, meta: {}, receivedAt: 1 })
    b.pushEvent!({ source: "slack", type: "y", data: {}, meta: {}, receivedAt: 2 })

    await vi.waitFor(() => expect(onEvent).toHaveBeenCalledTimes(2))
  })

  it("rolls back started sources when one throws", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ failOnConnect: new Error("boom"), name: "slack" })
    const flume = new Flume([a, b], { onEvent: vi.fn() })

    const result = await flume.start()

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toContain("slack: boom")
    expect(a.stopCount).toBe(1)
  })

  it("rolls back started sources when one returns error", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({
      returnErrorOnConnect: new Error("connect refused"),
      name: "slack",
    })
    const flume = new Flume([a, b], { onEvent: vi.fn() })

    const result = await flume.start()

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toContain("slack: connect refused")
    expect(a.stopCount).toBe(1)
  })

  it("refuses double-start", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume([a], { onEvent: vi.fn() })
    await flume.start()

    const second = await flume.start()

    expect(second).toBeInstanceOf(Error)
    expect(a.startCount).toBe(1)
  })

  it("rolls back two started sources when the third fails", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const c = new MockSource({
      returnErrorOnConnect: new Error("c-failed"),
      name: "github",
    })
    const flume = new Flume([a, b, c], { onEvent: vi.fn() })

    const result = await flume.start()

    expect(result).toBeInstanceOf(Error)
    expect(a.stopCount).toBe(1)
    expect(b.stopCount).toBe(1)
    expect(c.stopCount).toBe(0)
  })

  it("aggregates messages from multiple failing sources", async () => {
    const a = new MockSource({
      returnErrorOnConnect: new Error("a-failed"),
      name: "discord",
    })
    const b = new MockSource({
      returnErrorOnConnect: new Error("b-failed"),
      name: "slack",
    })
    const flume = new Flume([a, b], { onEvent: vi.fn() })

    const result = await flume.start()

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) {
      expect(result.message).toContain("discord: a-failed")
      expect(result.message).toContain("slack: b-failed")
    }
  })

  it("forwards status events with source name through onStatus", async () => {
    const a = new MockSource({ name: "discord" })
    const events: FlumeStatusEvent[] = []
    const flume = new Flume([a], {
      onEvent: vi.fn(),
      onStatus: (e) => events.push(e),
    })

    await flume.start()

    expect(events).toContainEqual({ source: "discord", status: "connected" })
  })

  it("logs rollback failures via onLog when a source's stop() rejects", async () => {
    const a = new MockSource({ name: "discord" })
    class FailingStop extends MockSource {
      protected override async disconnect(): Promise<void> {
        this.stopCount += 1
        throw new Error("stop-failed")
      }
    }
    const failingA = new FailingStop({ name: "discord" })
    const b = new MockSource({
      returnErrorOnConnect: new Error("b-failed"),
      name: "slack",
    })
    void a
    const captured: Array<{ action: string; level: string }> = []
    const flume = new Flume([failingA, b], {
      onEvent: vi.fn(),
      onLog: (log) => {
        captured.push({ action: log.action, level: log.level })
      },
    })

    await flume.start()

    expect(captured.some((c) => c.action === "flume.rollback.failed")).toBe(true)
  })

  it("refuses start if signal already aborted", async () => {
    const a = new MockSource({ name: "discord" })
    const controller = new AbortController()
    controller.abort()
    const flume = new Flume([a], {
      onEvent: vi.fn(),
      signal: controller.signal,
    })

    const result = await flume.start()

    expect(result).toBeInstanceOf(Error)
    expect(a.startCount).toBe(0)
  })
})

describe("FlumeRunning", () => {
  it("stop returns FlumeStopped and stops every source", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const flume = new Flume([a, b], { onEvent: vi.fn() })

    const running = await flume.start()
    if (running instanceof Error) throw running

    const stopped = await running.stop()

    expect(stopped).toBeInstanceOf(FlumeStopped)
    expect(a.stopCount).toBe(1)
    expect(b.stopCount).toBe(1)
  })

  it("stop is idempotent and concurrent-safe", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume([a], { onEvent: vi.fn() })

    const running = await flume.start()
    if (running instanceof Error) throw running

    const [first, second] = await Promise.all([running.stop(), running.stop()])

    expect(first).toBe(second)
    expect(a.stopCount).toBe(1)
  })

  it("aborts all sources when signal fires", async () => {
    const a = new MockSource({ name: "discord" })
    const controller = new AbortController()
    const flume = new Flume([a], {
      onEvent: vi.fn(),
      signal: controller.signal,
    })
    await flume.start()

    controller.abort()
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(a.stopCount).toBe(1)
  })

  it("statuses reflects underlying source state", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const flume = new Flume([a, b], { onEvent: vi.fn() })

    const running = await flume.start()
    if (running instanceof Error) throw running

    expect(running.statuses()).toEqual([
      { source: "discord", status: "connected" },
      { source: "slack", status: "connected" },
    ])
  })
})

describe("FlumeStopped", () => {
  it("exposes a snapshot of final statuses without raw sources", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume([a], { onEvent: vi.fn() })

    const running = await flume.start()
    if (running instanceof Error) throw running

    const stopped = await running.stop()

    expect(stopped.statuses()).toEqual([{ source: "discord", status: "disconnected" }])
  })
})
