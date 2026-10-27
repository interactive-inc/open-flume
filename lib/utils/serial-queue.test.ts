import { describe, it, expect } from "vitest"
import { FlumeSerialQueue } from "@/utils/serial-queue"

describe("FlumeSerialQueue", () => {
  it("runs synchronous ingress without retaining a promise queue", async () => {
    const queue = new FlumeSerialQueue({ immediate: true })
    let calls = 0
    for (let index = 0; index < 10000; index++)
      queue.add(() => {
        calls++
      })
    expect(calls).toBe(10000)
    expect(queue.size).toBe(0)
    await queue.drain()
  })

  it("preserves FIFO during synchronous reentry and an asynchronous pause", async () => {
    const queue = new FlumeSerialQueue({ immediate: true })
    const gate = Promise.withResolvers<void>()
    const order: number[] = []
    queue.add(() => {
      order.push(0)
      queue.add(async () => {
        await gate.promise
        order.push(1)
      })
      queue.add(() => {
        order.push(2)
      })
    })
    queue.add(() => {
      order.push(3)
    })
    expect(order).toEqual([0])
    expect(queue.size).toBe(3)
    gate.resolve()
    await Promise.all([queue.drain(), queue.drain()])
    expect(order).toEqual([0, 1, 2, 3])
    expect(queue.size).toBe(0)
  })
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

  it("drain() waits for tasks added while draining", async () => {
    const queue = new FlumeSerialQueue()
    const executed: number[] = []

    queue.add(async () => {
      executed.push(1)
      queue.add(async () => {
        executed.push(2)
      })
    })

    await queue.drain()

    expect(executed).toEqual([1, 2])
  })
})
