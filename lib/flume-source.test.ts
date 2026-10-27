import { describe, expect, it } from "vitest"
import { Flume } from "@/flume"
import { FlumeSource } from "@/flume-source"
import { FlumeLogger } from "@/logger"
import { createFlumeDefaultDeps } from "@/deps"
import { waitFor } from "@/test-utils/wait-for"
import type { FlumeLog, FlumeSourceStartContext } from "@/types"

class Source extends FlumeSource {
  readonly name = "test"
  stopped = false
  connected = false
  protected async connect() {
    this.connected = true
    this.setStatus("connected")
    return null
  }
  protected disconnect() {
    this.stopped = true
  }
  receive(order = 0) {
    this.emit({
      source: "custom",
      sourceName: this.name,
      type: "test",
      data: { order },
      meta: {},
      receivedAt: order,
    })
  }
}
function context(overrides: Partial<FlumeSourceStartContext> = {}): FlumeSourceStartContext {
  return {
    onEvent: () => {},
    reconnect: null,
    deps: createFlumeDefaultDeps(),
    log: new FlumeLogger({ source: "test", deps: { now: () => 0 } }),
    ...overrides,
  }
}

describe("Source ingress and shutdown", () => {
  it("preserves both direct startup and cleanup failures", async () => {
    const connectFailure = new Error("connect failed")
    const cleanupFailure = new Error("cleanup failed")
    class FailingSource extends Source {
      protected override async connect(): Promise<never> {
        throw connectFailure
      }
      protected override disconnect(): void {
        throw cleanupFailure
      }
    }
    const source = new FailingSource()
    const result = await source.start(context())
    expect(result).toMatchObject({ errorCode: "FLUME_START_FAILED" })
    if (!(result instanceof Error)) throw new Error("Expected startup failure")
    expect((result.cause as AggregateError).errors).toEqual([connectFailure, cleanupFailure])
    expect(await source.stop()).toBe(cleanupFailure)
  })

  it("keeps late callback failures observable after stopping", async () => {
    const source = new Source()
    const gate = Promise.withResolvers<void>()
    const logs: FlumeLog[] = []
    const failure = new Error("consumer failed")
    await source.start(
      context({
        onEvent: async () => {
          await gate.promise
          throw failure
        },
        log: new FlumeLogger({
          source: "test",
          deps: { now: () => 0 },
          handler: (log) => logs.push(log),
        }),
      }),
    )
    source.receive()
    await source.stop()
    expect(source.deliveryStatus().pendingEvents).toBe(1)
    gate.resolve()
    await source.drain()
    expect(source.deliveryStatus()).toMatchObject({ pendingEvents: 0, callbackFailures: 1 })
    expect(logs.find((log) => log.action === "onEvent.error")).toMatchObject({
      errorCode: "FLUME_CALLBACK_FAILED",
      error: failure,
    })
  })

  it("cleans up a failed direct start and preserves its cause", async () => {
    const failure = new Error("connect failed")
    class FailingSource extends Source {
      protected override async connect(): Promise<never> {
        throw failure
      }
    }
    const source = new FailingSource()
    expect(await source.start(context())).toBe(failure)
    expect(source.stopped).toBe(true)
    expect(source.status()).toBe("disconnected")
  })

  it("fails closed when the abort listener cannot be attached", async () => {
    const source = new Source()
    const failure = new Error("listener rejected")
    const controller = new AbortController()
    controller.signal.addEventListener = () => {
      throw failure
    }
    expect(await source.start(context({ signal: controller.signal }))).toMatchObject({
      errorCode: "FLUME_RUNTIME_FAILED",
      cause: failure,
    })
    expect(source.connected).toBe(false)
    expect(source.stopped).toBe(true)
  })

  it("captures the direct callback at startup", async () => {
    const source = new Source()
    let original = 0
    let replacement = 0
    const ctx = context({
      onEvent: () => {
        original++
      },
    })
    await source.start(ctx)
    ctx.onEvent = () => {
      replacement++
    }
    source.receive()
    await source.stop()
    await source.drain()
    expect([original, replacement]).toEqual([1, 0])
  })
  it("continues observing its abort signal after connecting directly", async () => {
    const source = new Source()
    const controller = new AbortController()
    await source.start(context({ signal: controller.signal }))
    controller.abort()
    await waitFor(() => expect(source.stopped).toBe(true))
    expect(source.status()).toBe("disconnected")
  })
  it("allows a direct callback to await source.stop after an asynchronous boundary", async () => {
    const source = new Source()
    let completed = false
    await source.start(
      context({
        onEvent: async () => {
          await Promise.resolve()
          await source.stop()
          completed = true
        },
      }),
    )
    source.receive()
    await waitFor(() => expect(completed).toBe(true), { timeout: 100 })
    expect(source.stopped).toBe(true)
  })

  it("passes a synchronous burst straight to Flume's bounded delivery", async () => {
    const source = new Source()
    const gate = Promise.withResolvers<void>()
    const running = await new Flume({
      sources: [source],
      callbackBuffer: 4,
      onEvent: () => gate.promise,
    }).open()
    if (running instanceof Error) throw running
    try {
      for (let order = 0; order < 10_000; order++) source.receive(order)
      expect(running.deliveryStatus().droppedEventCallbacks).toBeGreaterThan(9990)
      expect(running.deliveryStatus().pendingCallbacks).toBeLessThanOrEqual(6)
    } finally {
      gate.resolve()
      await running.close()
      await running.drain()
    }
  })

  it("bounds direct async callbacks and counts drops while continuing after overload", async () => {
    const source = new Source()
    const gate = Promise.withResolvers<void>()
    const received: number[] = []
    const logs: FlumeLog[] = []
    await source.start(
      context({
        eventBuffer: 2,
        log: new FlumeLogger({
          source: "test",
          deps: { now: () => 0 },
          handler: (log) => logs.push(log),
        }),
        onEvent: async (event) => {
          await gate.promise
          received.push(event.receivedAt)
        },
      }),
    )
    for (let order = 0; order < 100; order++) source.receive(order)
    expect(source.deliveryStatus()).toMatchObject({ pendingEvents: 2, droppedEvents: 98 })
    gate.resolve()
    await source.drain()
    source.receive(100)
    await source.stop()
    await source.drain()
    expect(received).toEqual([0, 1, 100])
    expect(logs.filter((log) => log.action === "source.event.overflow")).toHaveLength(1)
  })
})
