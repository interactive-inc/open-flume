import { describe, it, expect } from "vitest"
import { FlumeStreamHub } from "@/flume-stream-hub"
import type { FlumeStreamItem } from "@/types"

function item(value: number): FlumeStreamItem {
  return {
    kind: "log",
    log: {
      level: "info",
      action: "sample",
      message: String(value),
      timestamp: value,
      source: "test",
    },
  }
}

describe("stream reentrancy", () => {
  it("retains diagnostics published while replaying the startup buffer", async () => {
    const hub = new FlumeStreamHub({
      onDrop: (drop) => {
        if (drop.first) hub.publish(item(3))
      },
    })
    for (let value = 0; value < 3; value++) hub.publish(item(value))
    const stream = hub.subscribe({ buffer: 2 })
    hub.close()
    const messages: string[] = []
    for await (const entry of stream) if (entry.kind === "log") messages.push(entry.log.message)
    expect(messages).toEqual(["2", "3"])
  })
  it("commits the incoming item before an overflow observer publishes another item", async () => {
    const hub = new FlumeStreamHub({
      onDrop: (drop) => {
        if (drop.first) hub.publish(item(3))
      },
    })
    const stream = hub.subscribe({ buffer: 2 })
    for (let value = 0; value < 3; value++) hub.publish(item(value))
    hub.close()
    const messages: string[] = []
    for await (const entry of stream) if (entry.kind === "log") messages.push(entry.log.message)
    expect(messages).toEqual(["2", "3"])
  })
})
