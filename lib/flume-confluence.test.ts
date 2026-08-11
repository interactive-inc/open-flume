import { describe, it, expect } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import type { FlumeEvent, FlumeLog, FlumeStreamItem } from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeConfluence } from "@/flume-confluence"
import { FlumeSource } from "@/flume-source"

class MockSource extends FlumeSource {
  readonly name: "discord" | "slack"

  startCount = 0

  stopCount = 0

  pushEvent: ((event: FlumeEvent) => void) | null = null

  constructor(
    private readonly mockOptions: { name?: "discord" | "slack"; failConnect?: Error } = {},
  ) {
    super()
    this.name = mockOptions.name ?? "discord"
  }

  protected async connect(): Promise<Error | null> {
    this.startCount += 1
    if (this.mockOptions.failConnect) return this.mockOptions.failConnect

    this.setStatus("connected")
    this.pushEvent = (event) => this.emit(event)
    return null
  }

  protected disconnect(): void {
    this.stopCount += 1
    this.pushEvent = null
  }
}

const event = (type: string): FlumeEvent => ({
  source: "discord",
  type,
  data: {},
  meta: {},
  receivedAt: 0,
})

describe("FlumeConfluence", () => {
  it("merges firehoses from every added group into one onEvent", async () => {
    const items: FlumeStreamItem[] = []
    const confluence = new FlumeConfluence({ onEvent: (item) => items.push(item) })

    const a = new MockSource({ name: "discord" })
    const b = new MockSource({ name: "slack" })
    expect(await confluence.add("team-a", [a])).toBeNull()
    expect(await confluence.add("team-b", [b])).toBeNull()

    a.pushEvent?.(event("from-a"))
    b.pushEvent?.(event("from-b"))

    await waitFor(() => expect(items.filter((i) => i.kind === "event")).toHaveLength(2))
    const types = items
      .filter((i) => i.kind === "event")
      .map((i) => (i.kind === "event" ? i.event.type : ""))
    expect(types.sort()).toEqual(["from-a", "from-b"])
  })

  it("rejects a duplicate id without touching the existing group", async () => {
    const confluence = new FlumeConfluence()
    const a = new MockSource()

    expect(await confluence.add("dup", [a])).toBeNull()
    const second = await confluence.add("dup", [new MockSource()])

    expect(second).toBeInstanceOf(Error)
    expect(confluence.ids()).toEqual(["dup"])
  })

  it("keeps a single group when the same id is added concurrently", async () => {
    const confluence = new FlumeConfluence()
    const a = new MockSource()
    const b = new MockSource({ name: "slack" })

    const [first, second] = await Promise.all([
      confluence.add("dup", [a]),
      confluence.add("dup", [b]),
    ])

    // 片方は成功 (null)、もう片方は重複エラー。どちらが勝つかは順序依存
    const outcomes = [first, second]
    expect(outcomes.filter((r) => r === null)).toHaveLength(1)
    expect(outcomes.filter((r) => r instanceof Error)).toHaveLength(1)
    expect(confluence.ids()).toEqual(["dup"])

    // 負けた側は pending 追跡により open される前に弾かれる (start も stop も走らない)
    expect(a.startCount + b.startCount).toBe(1)
    expect(a.stopCount + b.stopCount).toBe(0)
  })

  it("returns the start error and does not store a failed group", async () => {
    const confluence = new FlumeConfluence()
    const failing = new MockSource({ failConnect: new Error("nope") })

    const result = await confluence.add("bad", [failing])

    expect(result).toBeInstanceOf(Error)
    expect(confluence.has("bad")).toBe(false)
  })

  it("removes one group without stopping the others", async () => {
    const confluence = new FlumeConfluence()
    const a = new MockSource()
    const b = new MockSource({ name: "slack" })
    await confluence.add("a", [a])
    await confluence.add("b", [b])

    await confluence.remove("a")

    expect(a.stopCount).toBe(1)
    expect(b.stopCount).toBe(0)
    expect(confluence.ids()).toEqual(["b"])
  })

  it("remove on an unknown id is a no-op", async () => {
    const confluence = new FlumeConfluence()

    await expect(confluence.remove("missing")).resolves.toBeUndefined()
  })

  it("stopAll stops every group", async () => {
    const confluence = new FlumeConfluence()
    const a = new MockSource()
    const b = new MockSource({ name: "slack" })
    await confluence.add("a", [a])
    await confluence.add("b", [b])

    await confluence.closeAll()

    expect(a.stopCount).toBe(1)
    expect(b.stopCount).toBe(1)
    expect(confluence.ids()).toEqual([])
  })

  it("stamps groupId on every item delivered to onEvent", async () => {
    const items: { kind: string; groupId: string }[] = []
    const confluence = new FlumeConfluence({
      onEvent: (item) => items.push({ kind: item.kind, groupId: item.groupId }),
    })

    const a = new MockSource({ name: "slack" })
    const b = new MockSource({ name: "discord" })
    await confluence.add("group-a", [a])
    await confluence.add("group-b", [b])

    a.pushEvent?.(event("from-a"))
    b.pushEvent?.(event("from-b"))

    await waitFor(() => expect(items.filter((i) => i.kind === "event")).toHaveLength(2))
    const groupsForEvents = items
      .filter((i) => i.kind === "event")
      .map((i) => i.groupId)
      .sort()
    expect(groupsForEvents).toEqual(["group-a", "group-b"])
  })

  it("replace swaps the group while keeping the old one until the new one is up", async () => {
    const stops: string[] = []
    const items: { groupId: string; type: string }[] = []
    const confluence = new FlumeConfluence({
      onEvent: (item) => {
        if (item.kind === "event") items.push({ groupId: item.groupId, type: item.event.type })
      },
    })

    class TrackedSource extends MockSource {
      constructor(private readonly label: string) {
        super({ name: "slack" })
      }
      protected override disconnect(): void {
        stops.push(this.label)
        super.disconnect()
      }
    }

    const oldSrc = new TrackedSource("old")
    const newSrc = new TrackedSource("new")

    expect(await confluence.add("rotating", [oldSrc])).toBeNull()
    expect(await confluence.replace("rotating", [newSrc])).toBeNull()

    expect(stops).toEqual(["old"])
    newSrc.pushEvent?.(event("after-replace"))

    await waitFor(() => expect(items).toHaveLength(1))
    expect(items[0]?.groupId).toBe("rotating")
    expect(items[0]?.type).toBe("after-replace")
  })

  it("replace returns Error and leaves the old group running when the new group fails to start", async () => {
    const items: { groupId: string }[] = []
    const confluence = new FlumeConfluence({
      onEvent: (item) => {
        if (item.kind === "event") items.push({ groupId: item.groupId })
      },
    })

    const oldSrc = new MockSource({ name: "slack" })
    const badSrc = new MockSource({ name: "slack", failConnect: new Error("boom") })

    expect(await confluence.add("rotating", [oldSrc])).toBeNull()
    const result = await confluence.replace("rotating", [badSrc])
    expect(result).toBeInstanceOf(Error)

    expect(oldSrc.stopCount).toBe(0)
    oldSrc.pushEvent?.(event("still-old"))
    await waitFor(() => expect(items).toHaveLength(1))
    expect(items[0]?.groupId).toBe("rotating")
  })

  it("replace returns Error when the id is not currently running", async () => {
    const confluence = new FlumeConfluence()
    const src = new MockSource()
    const result = await confluence.replace("missing", [src])
    expect(result).toBeInstanceOf(Error)
  })

  it("contains async onEvent rejection without an unhandled rejection", async () => {
    const source = new MockSource()
    const errors: FlumeLog[] = []
    const confluence = new FlumeConfluence({
      onEvent: async (item) => {
        if (item.kind === "event") throw new Error("sink failed")
      },
      onError: (log) => errors.push(log),
    })

    expect(await confluence.add("async", [source])).toBeNull()
    source.pushEvent?.(event("reject"))
    await confluence.closeAll()

    expect(source.stopCount).toBe(1)
    expect(errors).toContainEqual(
      expect.objectContaining({
        action: "onEvent.error",
        detail: expect.objectContaining({ groupId: "async", itemType: "reject" }),
      }),
    )
  })

  it("aborts and waits for an add that is still connecting during closeAll", async () => {
    const connectGate = Promise.withResolvers<void>()

    class PendingSource extends FlumeSource {
      readonly name = "discord"
      startCount = 0
      stopCount = 0

      protected async connect(): Promise<Error | null> {
        this.startCount += 1
        await connectGate.promise
        return null
      }

      protected disconnect(): void {
        this.stopCount += 1
        connectGate.resolve()
      }
    }

    const source = new PendingSource()
    const confluence = new FlumeConfluence()
    const addPromise = confluence.add("pending", [source])
    await waitFor(() => expect(source.startCount).toBe(1))

    await confluence.closeAll()

    expect(await addPromise).toBeInstanceOf(Error)
    expect(source.stopCount).toBe(1)
    expect(confluence.ids()).toEqual([])
  })

  it("returns an Error when a replace timeout cannot be scheduled", async () => {
    const deps = {
      ...createFlumeDefaultDeps(),
      setTimeout: () => {
        throw new Error("timer failed")
      },
    }
    const confluence = new FlumeConfluence({ deps })
    const previous = new MockSource()
    const next = new MockSource()
    expect(await confluence.add("replace", [previous])).toBeNull()

    const result = await confluence.replace("replace", [next])

    expect(result).toBeInstanceOf(Error)
    expect(previous.stopCount).toBe(0)
    expect(confluence.has("replace")).toBe(true)
    await confluence.closeAll()
  })

  it("ignores a stale replace timeout when clearTimeout throws", async () => {
    const timer = { callback: (): void => {} }
    const deps = {
      ...createFlumeDefaultDeps(),
      setTimeout: (callback: () => void) => {
        timer.callback = callback
        return 1
      },
      clearTimeout: () => {
        throw new Error("clear failed")
      },
    }
    const confluence = new FlumeConfluence({ deps })
    const previous = new MockSource()
    const next = new MockSource()
    expect(await confluence.add("replace", [previous])).toBeNull()
    expect(await confluence.replace("replace", [next])).toBeNull()

    timer.callback()

    expect(next.stopCount).toBe(0)
    expect(confluence.has("replace")).toBe(true)
    await confluence.closeAll()
  })
})
