import { describe, it, expect, vi } from "vitest"
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

  it("drops new tasks once maxDepth is reached and reports via onOverflow", async () => {
    const onOverflow = vi.fn()
    const queue = new FlumeSerialQueue({ maxDepth: 2, onOverflow })

    let resolveBlocker: () => void = () => {}
    const blocker = new Promise<void>((r) => {
      resolveBlocker = r
    })

    queue.add(() => blocker)
    queue.add(async () => {})
    queue.add(async () => {})

    expect(onOverflow).toHaveBeenCalledTimes(1)
    expect(queue.size()).toBe(2)

    resolveBlocker()
    await queue.drain()
  })

  it("cancel() makes subsequent add() a no-op and drain() resolves immediately", async () => {
    const queue = new FlumeSerialQueue()
    let executed = false

    queue.cancel()
    queue.add(async () => {
      executed = true
    })

    await queue.drain()

    expect(executed).toBe(false)
    expect(queue.isCancelled()).toBe(true)
  })

  it("size() decreases as tasks complete", async () => {
    const queue = new FlumeSerialQueue()
    let release: () => void = () => {}
    const blocker = new Promise<void>((r) => {
      release = r
    })

    queue.add(() => blocker)
    queue.add(async () => {})

    expect(queue.size()).toBe(2)
    release()
    await queue.drain()
    expect(queue.size()).toBe(0)
  })
})
