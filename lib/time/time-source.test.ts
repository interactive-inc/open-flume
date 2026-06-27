import { describe, it, expect, vi } from "vitest"
import { FlumeLogger } from "@/logger"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeTimeSource } from "@/time/time-source"
import type { FlumeEvent, FlumeRuntimeDeps, FlumeSourceStartContext, FlumeStatus } from "@/types"

const timerHandle = globalThis.setTimeout(() => {}, 0)
globalThis.clearTimeout(timerHandle)

function createMockDeps(startMs: number) {
  let nowMs = startMs
  let lastCallback: (() => void) | null = null

  const deps: FlumeRuntimeDeps = {
    fetch: vi.fn(),
    now: () => nowMs,
    setTimeout: vi.fn((fn: () => void, _ms: number) => {
      lastCallback = fn
      return timerHandle
    }),
    clearTimeout: vi.fn(),
    setInterval: vi.fn(() => timerHandle),
    clearInterval: vi.fn(),
    random: () => 0.5,
    WebSocket: globalThis.WebSocket,
  }

  return {
    deps,
    setNow: (ms: number) => {
      nowMs = ms
    },
    fire: () => lastCallback?.(),
  }
}

type CtxProps = {
  deps: FlumeRuntimeDeps
  onEvent?: (event: FlumeEvent) => void
  onStatus?: (status: FlumeStatus, detail?: string) => void
}

const createCtx = (props: CtxProps): FlumeSourceStartContext => ({
  onEvent: props.onEvent ?? (() => {}),
  log: new FlumeLogger({ source: "time", deps: props.deps }),
  deps: props.deps,
  onStatus: props.onStatus ?? (() => {}),
  reconnect: null,
})

function flushPromises() {
  return new Promise<void>((resolve) => globalThis.setTimeout(resolve, 0))
}

describe("FlumeTimeSource", () => {
  it("connects and schedules the first tick", async () => {
    const test = createMockDeps(0)
    const statuses: FlumeStatus[] = []

    const source = new FlumeTimeSource({ cron: "* * * * *" })
    const result = await source.start(
      createCtx({ deps: test.deps, onStatus: (s) => statuses.push(s) }),
    )

    expect(result).toBeNull()
    expect(statuses).toEqual(["connecting", "connected"])
    expect(test.deps.setTimeout).toHaveBeenCalledTimes(1)
  })

  it("emits a default tick event when the timer fires", async () => {
    const test = createMockDeps(0)
    const events: FlumeEvent[] = []

    const source = new FlumeTimeSource({ cron: "* * * * *" })
    await source.start(createCtx({ deps: test.deps, onEvent: (e) => events.push(e) }))

    test.setNow(60_000)
    test.fire()
    await flushPromises()

    expect(events).toHaveLength(1)
    const event = events[0]!
    expect(event.source).toBe("time")
    expect(event.type).toBe("tick")
    expect(event.data).toEqual({ firedAt: 60_000, cron: "* * * * *" })
    expect(event.meta).toEqual({ cron: "* * * * *" })
  })

  it("lets message() customize type, data and meta", async () => {
    const test = createMockDeps(0)
    const events: FlumeEvent[] = []

    const source = new FlumeTimeSource({
      cron: "* * * * *",
      message: (tick) => ({
        type: "heartbeat",
        data: { label: "hourly", firedAt: tick.firedAt },
        meta: { channel: "ops" },
      }),
    })
    await source.start(createCtx({ deps: test.deps, onEvent: (e) => events.push(e) }))

    test.setNow(60_000)
    test.fire()
    await flushPromises()

    const event = events[0]!
    expect(event.type).toBe("heartbeat")
    expect(event.data).toEqual({ label: "hourly", firedAt: 60_000 })
    expect(event.meta).toEqual({ channel: "ops" })
  })

  it("falls back to defaults when message() throws", async () => {
    const test = createMockDeps(0)
    const events: FlumeEvent[] = []

    const source = new FlumeTimeSource({
      cron: "* * * * *",
      message: () => {
        throw new Error("boom")
      },
    })
    await source.start(createCtx({ deps: test.deps, onEvent: (e) => events.push(e) }))

    test.setNow(60_000)
    test.fire()
    await flushPromises()

    expect(events[0]!.type).toBe("tick")
  })

  it("reschedules without emitting when woken before the target (capped wake)", async () => {
    const test = createMockDeps(0)
    const events: FlumeEvent[] = []

    const source = new FlumeTimeSource({ cron: "* * * * *" })
    await source.start(createCtx({ deps: test.deps, onEvent: (e) => events.push(e) }))

    // target は 60_000 だが、まだ 10_000 の時点で起こされた (長尺キャップ相当)
    test.setNow(10_000)
    test.fire()
    await flushPromises()

    expect(events).toHaveLength(0)
    expect(test.deps.setTimeout).toHaveBeenCalledTimes(2)
  })

  it("returns a FlumeStartError for an invalid cron", async () => {
    const test = createMockDeps(0)
    const statuses: FlumeStatus[] = []

    const source = new FlumeTimeSource({ cron: "not a cron" })
    const result = await source.start(
      createCtx({ deps: test.deps, onStatus: (s) => statuses.push(s) }),
    )

    expect(result).toBeInstanceOf(FlumeStartError)
    expect(statuses).toEqual(["connecting", "disconnected"])
  })

  it("stops the scheduler and emits no further ticks", async () => {
    const test = createMockDeps(0)
    const events: FlumeEvent[] = []

    const source = new FlumeTimeSource({ cron: "* * * * *" })
    await source.start(createCtx({ deps: test.deps, onEvent: (e) => events.push(e) }))

    await source.stop()

    test.setNow(60_000)
    test.fire()
    await flushPromises()

    expect(events).toHaveLength(0)
    expect(test.deps.clearTimeout).toHaveBeenCalled()
  })
})
