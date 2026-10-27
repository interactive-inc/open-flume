import { describe, it, expect } from "vitest"
import { FlumeDelivery } from "@/flume-delivery"

describe("delivery diagnostics", () => {
  it("accounts for dropped events exactly and resumes delivery after overload", async () => {
    const gate = Promise.withResolvers<void>()
    const received: number[] = []
    const delivery = new FlumeDelivery(
      {
        callbackBuffer: 2,
        onEvent: async (item) => {
          await gate.promise
          if (item.kind === "event") received.push(item.event.receivedAt)
        },
      },
      { now: () => 0 },
    )
    const publish = (index: number) =>
      delivery.publish({
        kind: "event",
        event: {
          source: "custom",
          sourceName: "load",
          type: "tick",
          data: {},
          meta: {},
          receivedAt: index,
        },
      })
    for (let index = 0; index < 100; index++) publish(index)
    expect(delivery.status()).toMatchObject({
      droppedEventCallbacks: 98,
      droppedLogCallbacks: 0,
      droppedCallbacks: 98,
    })
    expect(delivery.diagnostics().entries).toEqual([
      expect.objectContaining({ errorCode: "FLUME_BUFFER_OVERFLOW" }),
    ])
    gate.resolve()
    await delivery.drain()
    publish(100)
    delivery.close()
    await delivery.drain()
    expect(received).toEqual([0, 1, 100])
    expect(received.length + delivery.status().droppedEventCallbacks).toBe(101)
  })
  it("bounds diagnostic callbacks when a callback getter repeatedly throws", async () => {
    const gate = Promise.withResolvers<void>()
    const delivery = new FlumeDelivery(
      {
        callbackBuffer: 4,
        get onEvent(): never {
          throw new Error("callback unavailable")
        },
        onError: () => gate.promise,
      },
      { now: () => 0 },
    )
    for (let index = 0; index < 1000; index++)
      delivery.publish({
        kind: "event",
        event: {
          source: "custom",
          sourceName: "load",
          type: "tick",
          data: {},
          meta: {},
          receivedAt: index,
        },
      })
    const pending = delivery.status().pendingCallbacks
    gate.resolve()
    delivery.close()
    await delivery.drain()
    expect(pending).toBeLessThanOrEqual(6)
  })

  it("keeps the callback selected when an item was accepted", async () => {
    const calls: string[] = []
    const options = { onEvent: () => calls.push("original") }
    const delivery = new FlumeDelivery(options, { now: () => 0 })
    delivery.publish({
      kind: "log",
      log: { level: "info", source: "test", action: "frame", message: "received", timestamp: 0 },
    })
    options.onEvent = () => calls.push("replacement")
    delivery.close()
    await delivery.drain()
    expect(calls).toEqual(["original"])
  })

  it("retains bounded diagnostic snapshots while callbacks and streams are full", async () => {
    const gate = Promise.withResolvers<void>()
    const delivery = new FlumeDelivery(
      { callbackBuffer: 1, onEvent: () => gate.promise },
      { now: () => 0 },
    )
    const stream = delivery.stream({ buffer: 1 })
    const cause = new Error("original failure")
    for (let index = 0; index < 250; index++)
      delivery.publish({
        kind: "log",
        log: {
          level: "error",
          errorCode: "FLUME_HTTP_FAILED",
          error: cause,
          source: "test",
          action: "http.error",
          message: String(index),
          timestamp: index,
        },
      })
    const snapshot = delivery.diagnostics()
    expect(snapshot.entries).toHaveLength(100)
    expect(snapshot.evicted).toBe(snapshot.total - 100)
    expect(snapshot.entries.at(-1)?.message).toBe("249")
    expect(snapshot.entries.at(-1)?.error).toBe(cause)
    expect(Object.isFrozen(snapshot.entries)).toBe(true)
    expect(Object.isFrozen(snapshot.entries[0])).toBe(true)
    expect(delivery.status().pendingCallbacks).toBeLessThanOrEqual(3)
    expect(delivery.status().droppedCallbacks).toBeGreaterThan(0)
    gate.resolve()
    delivery.close()
    await delivery.drain()
    await stream.return?.()
    expect(snapshot.entries).toHaveLength(100)
  })

  it("counts every dropped pull item even without callbacks", async () => {
    const delivery = new FlumeDelivery({}, { now: () => 0 })
    const stream = delivery.stream({ buffer: 2 })
    for (let index = 0; index < 10; index++)
      delivery.publish({
        kind: "log",
        log: {
          level: "info",
          source: "test",
          action: "frame",
          message: "received",
          timestamp: index,
        },
      })
    expect(delivery.status().droppedStreamItems).toBe(8)
    delivery.close()
    await delivery.drain()
    const times: number[] = []
    for await (const item of stream) if (item.kind === "log") times.push(item.log.timestamp)
    expect(times).toEqual([8, 9])
  })
})
