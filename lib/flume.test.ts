import { describe, it, expect, vi } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import type { FlumeEvent, FlumeLog, FlumeSourceStartContext, FlumeStreamItem } from "@/types"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { FlumeClosed } from "@/flume-closed"
import { FlumeSource } from "@/flume-source"

type MockOptions = {
  failOnConnect?: Error
  returnErrorOnConnect?: Error
  failOnDisconnect?: Error
  name?: "discord" | "slack" | "github"
}

class MockSource extends FlumeSource {
  readonly name: "discord" | "slack" | "github"

  startCount = 0

  stopCount = 0

  capturedSignal: AbortSignal | undefined = undefined

  pushEvent: ((event: FlumeEvent) => void) | null = null

  constructor(private readonly mockOptions: MockOptions = {}) {
    super()
    this.name = mockOptions.name ?? "discord"
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.startCount += 1
    this.capturedSignal = ctx.signal
    if (this.mockOptions.failOnConnect) throw this.mockOptions.failOnConnect
    if (this.mockOptions.returnErrorOnConnect) return this.mockOptions.returnErrorOnConnect

    this.setStatus("connected")
    this.pushEvent = (event) => this.emit(event)
    return null
  }

  protected disconnect(): void {
    this.stopCount += 1
    this.pushEvent = null
    if (this.mockOptions.failOnDisconnect) throw this.mockOptions.failOnDisconnect
  }
}

describe("Flume", () => {
  it("constructs with only sources (no options)", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a] })

    const result = await flume.open()

    expect(result).toBeInstanceOf(FlumeRunning)
    expect(a.startCount).toBe(1)
  })

  it("drops events silently when onEvent is omitted", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a] })
    await flume.open()

    a.pushEvent!({ source: "discord", type: "x", data: {}, meta: {}, receivedAt: 1 })

    expect(a.startCount).toBe(1)
  })

  it("start returns FlumeRunning and starts every source", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })

    const result = await flume.open()

    expect(result).toBeInstanceOf(FlumeRunning)
    expect(a.startCount).toBe(1)
    expect(b.startCount).toBe(1)
  })

  it("merges events from every source into one stream", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const events: FlumeEvent[] = []
    const flume = new Flume({
      sources: [a, b],
      onEvent: (item) => {
        if (item.kind === "event") events.push(item.event)
      },
    })
    await flume.open()

    a.pushEvent!({ source: "discord", type: "x", data: {}, meta: {}, receivedAt: 1 })
    b.pushEvent!({ source: "slack", type: "y", data: {}, meta: {}, receivedAt: 2 })

    await waitFor(() => expect(events).toHaveLength(2))
  })

  it("rolls back started sources when one throws", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ failOnConnect: new Error("boom"), name: "slack" })
    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })

    const result = await flume.open()

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
    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })

    const result = await flume.open()

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) expect(result.message).toContain("slack: connect refused")
    expect(a.stopCount).toBe(1)
  })

  it("refuses double-start", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a], onEvent: vi.fn() })
    await flume.open()

    const second = await flume.open()

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
    const flume = new Flume({ sources: [a, b, c], onEvent: vi.fn() })

    const result = await flume.open()

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
    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })

    const result = await flume.open()

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) {
      expect(result.message).toContain("discord: a-failed")
      expect(result.message).toContain("slack: b-failed")
    }
  })

  it("surfaces status transitions through the firehose (no dedicated status callback)", async () => {
    const a = new MockSource({ name: "discord" })
    const logs: FlumeLog[] = []
    const flume = new Flume({
      sources: [a],
      onEvent: (item) => {
        if (item.kind === "log") logs.push(item.log)
      },
    })

    await flume.open()

    const statusLog = logs.find((log) => log.action === "status" && log.source === "discord")
    expect(statusLog?.message).toContain("connected")
  })

  it("logs rollback failures into the firehose when a source's stop() rejects", async () => {
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
    const captured: Array<{ action: string; level: string }> = []
    const flume = new Flume({
      sources: [failingA, b],
      onEvent: (item) => {
        if (item.kind === "log") captured.push({ action: item.log.action, level: item.log.level })
      },
    })

    await flume.open()

    expect(captured.some((c) => c.action === "flume.rollback.failed")).toBe(true)
  })

  it("threads the host signal into source ctx so sources can listen for abort natively", async () => {
    const source = new MockSource({ name: "discord" })
    const controller = new AbortController()
    const flume = new Flume({ sources: [source], onEvent: vi.fn(), signal: controller.signal })

    const result = await flume.open()
    expect(result).not.toBeInstanceOf(Error)
    expect(source.capturedSignal).toBe(controller.signal)
  })

  it("passes undefined as ctx.signal when the host did not supply one", async () => {
    const source = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [source], onEvent: vi.fn() })

    const result = await flume.open()
    expect(result).not.toBeInstanceOf(Error)
    expect(source.capturedSignal).toBeUndefined()
  })

  it("refuses start if signal already aborted", async () => {
    const a = new MockSource({ name: "discord" })
    const controller = new AbortController()
    controller.abort()
    const flume = new Flume({ sources: [a], onEvent: vi.fn(), signal: controller.signal })

    const result = await flume.open()

    expect(result).toBeInstanceOf(Error)
    expect(a.startCount).toBe(0)
  })
})

describe("FlumeRunning", () => {
  it("stop returns FlumeClosed and stops every source", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const closed = await running.close()

    expect(closed).toBeInstanceOf(FlumeClosed)
    expect(a.stopCount).toBe(1)
    expect(b.stopCount).toBe(1)
  })

  it("stop is idempotent and concurrent-safe", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a], onEvent: vi.fn() })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const [first, second] = await Promise.all([running.close(), running.close()])

    expect(first).toBe(second)
    expect(a.stopCount).toBe(1)
  })

  it("aborts all sources when signal fires", async () => {
    const a = new MockSource({ name: "discord" })
    const controller = new AbortController()
    const flume = new Flume({ sources: [a], onEvent: vi.fn(), signal: controller.signal })
    await flume.open()

    controller.abort()
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(a.stopCount).toBe(1)
  })

  it("statuses reflects underlying source state", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })

    const running = await flume.open()
    if (running instanceof Error) throw running

    expect(running.statuses()).toEqual([
      { source: "discord", status: "connected" },
      { source: "slack", status: "connected" },
    ])
  })

  it("exposes the host signal via the running.signal getter", async () => {
    const source = new MockSource({ name: "discord" })
    const controller = new AbortController()

    const flume = new Flume({ sources: [source], onEvent: vi.fn(), signal: controller.signal })
    const running = await flume.open()
    if (running instanceof Error) throw running

    expect(running.signal).toBe(controller.signal)
    expect(running.signal?.aborted).toBe(false)

    controller.abort()
    expect(running.signal?.aborted).toBe(true)
  })

  it("running.signal is undefined when the host did not supply one", async () => {
    const source = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [source], onEvent: vi.fn() })
    const running = await flume.open()
    if (running instanceof Error) throw running

    expect(running.signal).toBeUndefined()
  })

  it("propagates source.disconnect throws to runStop so flume.stop.failed is logged with the failing source name", async () => {
    const source = new MockSource({
      name: "discord",
      failOnDisconnect: new Error("ws close timeout"),
    })
    const logs: { action: string; message: string; error?: Error }[] = []

    const flume = new Flume({
      sources: [source],
      onEvent: (item) => {
        if (item.kind === "log") {
          logs.push({ action: item.log.action, message: item.log.message, error: item.log.error })
        }
      },
    })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const closed = await running.close()
    expect(closed).toBeInstanceOf(FlumeClosed)

    const failures = logs.filter((l) => l.action === "flume.close.failed")
    expect(failures).toHaveLength(1)
    expect(failures[0]?.error?.message).toBe("ws close timeout")
    expect(failures[0]?.message).toContain("discord")
  })

  it("FlumeClosed.errors() lists per-source disconnect failures so hosts skip the onLog grep", async () => {
    const ok = new MockSource({ name: "discord" })
    const bad = new MockSource({ name: "slack", failOnDisconnect: new Error("boom") })

    const flume = new Flume({ sources: [ok, bad], onEvent: vi.fn() })
    const running = await flume.open()
    if (running instanceof Error) throw running

    const closed = await running.close()
    const errors = closed.errors()

    expect(errors).toHaveLength(1)
    expect(errors[0]?.source).toBe("slack")
    expect(errors[0]?.error.message).toBe("boom")
  })

  it("FlumeClosed.errors() is empty when every source stops cleanly", async () => {
    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })

    const flume = new Flume({ sources: [a, b], onEvent: vi.fn() })
    const running = await flume.open()
    if (running instanceof Error) throw running

    const closed = await running.close()
    expect(closed.errors()).toEqual([])
  })
})

describe("FlumeClosed", () => {
  it("exposes a snapshot of final statuses without raw sources", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a], onEvent: vi.fn() })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const closed = await running.close()

    expect(closed.statuses()).toEqual([{ source: "discord", status: "disconnected" }])
  })
})

describe("Flume onError", () => {
  it("receives only error-level logs", async () => {
    const failing = new MockSource({
      returnErrorOnConnect: new Error("boom"),
      name: "discord",
    })
    const errors: FlumeLog[] = []
    const flume = new Flume({
      sources: [failing],
      onEvent: vi.fn(),
      onError: (log) => errors.push(log),
    })

    await flume.open()

    expect(errors.length).toBeGreaterThan(0)
    expect(errors.every((log) => log.level === "error")).toBe(true)
  })

  it("delivers every level to the firehose while onError filters to errors", async () => {
    const failing = new MockSource({
      returnErrorOnConnect: new Error("boom"),
      name: "discord",
    })
    const logs: FlumeLog[] = []
    const errors: FlumeLog[] = []
    const flume = new Flume({
      sources: [failing],
      onEvent: (item) => {
        if (item.kind === "log") logs.push(item.log)
      },
      onError: (log) => errors.push(log),
    })

    await flume.open()

    expect(logs.some((log) => log.level !== "error")).toBe(true)
    expect(errors.length).toBeLessThan(logs.length)
    expect(errors.every((log) => log.level === "error")).toBe(true)
  })
})

describe("FlumeRunning.stream", () => {
  it("yields event items to a for-await consumer", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a] })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const received: FlumeEvent[] = []
    const consume = (async () => {
      for await (const item of running.stream()) {
        if (item.kind !== "event") continue
        received.push(item.event)
        if (received.length === 2) break
      }
    })()

    const event: FlumeEvent = {
      source: "discord",
      type: "MESSAGE_CREATE",
      data: {},
      meta: {},
      receivedAt: 0,
    }
    a.pushEvent?.(event)
    a.pushEvent?.({ ...event, type: "SECOND" })

    await consume

    expect(received.map((e) => e.type)).toEqual(["MESSAGE_CREATE", "SECOND"])
  })

  it("ends the iterator when the flume stops", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a] })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const stream = running.stream()
    const done = (async () => {
      const items: FlumeStreamItem[] = []
      for await (const item of stream) items.push(item)
      return items
    })()

    await running.close()

    // 停止で iterator は終端する (collected は stop 時の log item のみ、event は無し)
    const items = await done
    expect(items.every((item) => item.kind === "log")).toBe(true)
  })

  it("drops oldest items when the buffer overflows", async () => {
    const a = new MockSource({ name: "discord" })
    const flume = new Flume({ sources: [a] })

    const running = await flume.open()
    if (running instanceof Error) throw running

    const stream = running.stream({ buffer: 2, onOverflow: "drop-oldest" })

    const base: FlumeEvent = {
      source: "discord",
      type: "0",
      data: {},
      meta: {},
      receivedAt: 0,
    }
    a.pushEvent?.({ ...base, type: "0" })
    a.pushEvent?.({ ...base, type: "1" })
    a.pushEvent?.({ ...base, type: "2" })

    // serial queue を drain させてから読む (3 件とも buffer に積まれてから next する)
    await new Promise((resolve) => setTimeout(resolve, 10))

    const first = await stream.next()
    const second = await stream.next()
    const firstType = first.value?.kind === "event" ? first.value.event.type : null
    const secondType = second.value?.kind === "event" ? second.value.event.type : null
    expect([firstType, secondType]).toEqual(["1", "2"])
  })
})
