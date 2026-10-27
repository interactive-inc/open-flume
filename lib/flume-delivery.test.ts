import { describe, it, expect } from "vitest"
import { FlumeDelivery } from "@/flume-delivery"

describe("delivery diagnostics", () => {
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
