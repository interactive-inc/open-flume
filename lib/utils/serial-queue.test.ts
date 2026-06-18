import { describe, it, expect } from "vitest"
import { FlumeSerialQueue } from "@/utils/serial-queue"

describe("FlumeSerialQueue", () => {
  it("runs tasks in submission order serially", async () => {
    const queue = new FlumeSerialQueue()
    const order: number[] = []

    queue.add(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20))
      order.push(1)
    })
    queue.add(async () => {
      order.push(2)
    })
    queue.add(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5))
      order.push(3)
    })

    await queue.drain()

    expect(order).toEqual([1, 2, 3])
  })

  it("isolates task failures from subsequent tasks", async () => {
    const queue = new FlumeSerialQueue()
    const order: string[] = []

    queue.add(async () => {
      throw new Error("first failed")
    })
    queue.add(async () => {
      order.push("second")
    })

    await queue.drain()

    expect(order).toEqual(["second"])
  })

  it("drain awaits everything queued so far", async () => {
    const queue = new FlumeSerialQueue()
    let count = 0
    for (let i = 0; i < 50; i++) {
      queue.add(async () => {
        await new Promise((resolve) => setTimeout(resolve, 1))
        count += 1
      })
    }

    await queue.drain()

    expect(count).toBe(50)
  })
})
