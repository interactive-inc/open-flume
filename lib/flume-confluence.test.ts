import { describe, it, expect } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import type { FlumeEvent, FlumeStreamItem } from "@/types"
import { FlumeConfluence } from "@/flume-confluence"
import { FlumeSource } from "@/flume-source"

class MockSource extends FlumeSource {
  readonly name: "discord" | "slack"

  stopCount = 0

  pushEvent: ((event: FlumeEvent) => void) | null = null

  constructor(
    private readonly mockOptions: { name?: "discord" | "slack"; failConnect?: Error } = {},
  ) {
    super()
    this.name = mockOptions.name ?? "discord"
  }

  protected async connect(): Promise<Error | null> {
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
})
