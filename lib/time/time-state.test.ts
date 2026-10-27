import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeLogger } from "@/logger"
import { FlumeTimeSource } from "@/time/time-source"
import { FlumeTimeState } from "@/time/time-state"
import { Flume } from "@/flume"
import type { FlumeLog, FlumeStatePersister, FlumeTimeSourceState } from "@/types"

function setup(persister: FlumeStatePersister<FlumeTimeSourceState>, timeoutMs = 100) {
  const logs: FlumeLog[] = []
  const deps = createFlumeDefaultDeps()
  const ctx = {
    deps,
    log: new FlumeLogger({ source: "time", deps, handler: (log: FlumeLog) => logs.push(log) }),
    onEvent: () => {},
    reconnect: null,
  }
  return { state: new FlumeTimeState({ persister, ctx, timeoutMs }), logs, ctx }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(0)
})
afterEach(() => {
  vi.useRealTimers()
})

describe("bounded time state persistence", () => {
  it("coalesces a burst into one in-flight and one latest checkpoint", async () => {
    const gate = Promise.withResolvers<void>()
    const save = vi.fn(async (_state: FlumeTimeSourceState) => gate.promise)
    const { state } = setup({ load: async () => null, save })
    for (let i = 1; i <= 10_000; i++) state.save(i)
    expect(state.status()).toMatchObject({
      pendingWrites: 2,
      coalescedWrites: 9998,
      lastRequestedAt: 10_000,
    })
    expect(save).toHaveBeenCalledTimes(1)
    gate.resolve()
    expect(await state.close()).toBeNull()
    expect(save.mock.calls.map((call) => call[0])).toEqual([
      { lastFiredAt: 1 },
      { lastFiredAt: 10_000 },
    ])
    expect(state.status()).toMatchObject({
      phase: "idle",
      pendingWrites: 0,
      lastSavedAt: 10_000,
      failedWrites: 0,
    })
    expect(vi.getTimerCount()).toBe(0)
  })

  it("times out a write without starting a competing write, even after late completion", async () => {
    const gate = Promise.withResolvers<void>()
    const save = vi.fn(
      async (_state: FlumeTimeSourceState, _options?: { signal: AbortSignal }) => gate.promise,
    )
    const { state, logs } = setup({ load: async () => null, save })
    state.save(1)
    state.save(2)
    await vi.advanceTimersByTimeAsync(100)
    expect(save.mock.calls[0]?.[1]?.signal.aborted).toBe(true)
    for (let i = 3; i <= 1000; i++) state.save(i)
    expect(state.status()).toMatchObject({
      phase: "halted",
      pendingWrites: 2,
      failedWrites: 1,
      lastRequestedAt: 1000,
      lastSavedAt: null,
    })
    expect(logs.find((log) => log.action === "time.state.save.error")).toMatchObject({
      errorCode: "FLUME_TIMEOUT",
      error: { context: { operation: "time.state.save" } },
    })
    expect(await state.close()).toMatchObject({ errorCode: "FLUME_TIMEOUT" })
    gate.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(save).toHaveBeenCalledTimes(1)
    expect(state.status()).toMatchObject({ phase: "halted", pendingWrites: 1, lastSavedAt: 1 })
    expect(vi.getTimerCount()).toBe(0)
  })

  it("returns the exact final write error on shutdown", async () => {
    const failure = new Error("storage unavailable")
    const { state, logs } = setup({
      load: async () => null,
      save: async () => {
        throw failure
      },
    })
    state.save(1)
    expect(await state.close()).toBe(failure)
    expect(state.status()).toMatchObject({
      error: failure,
      failedWrites: 1,
      lastSavedAt: null,
      lastRequestedAt: 1,
    })
    expect(logs[0]).toMatchObject({ errorCode: "FLUME_STATE_FAILED", error: failure })
    expect(vi.getTimerCount()).toBe(0)
  })

  it("bounds load waiting and exposes invalid stored state", async () => {
    const never = Promise.withResolvers<FlumeTimeSourceState | null>()
    const { state, logs } = setup({ load: () => never.promise, save: async () => {} })
    const loading = state.load()
    await vi.advanceTimersByTimeAsync(100)
    expect(await loading).toBeNull()
    expect(logs[0]).toMatchObject({ errorCode: "FLUME_TIMEOUT" })
    expect(await state.close()).toBeNull()
    never.resolve({ lastFiredAt: 10 })
    const invalid = setup({
      load: async () => ({ lastFiredAt: Number.MAX_VALUE }),
      save: async () => {},
    })
    expect(await invalid.state.load()).toBeNull()
    expect(invalid.logs[0]).toMatchObject({
      errorCode: "FLUME_STATE_FAILED",
      error: { context: { phase: "validate" } },
    })
    await invalid.state.close()
    expect(vi.getTimerCount()).toBe(0)
  })

  it("cancels the host load signal and ignores late success", async () => {
    const gate = Promise.withResolvers<FlumeTimeSourceState | null>()
    const load = vi.fn((_options?: { signal: AbortSignal }) => gate.promise)
    const { state, logs } = setup({ load, save: async () => {} })
    const loading = state.load()
    await state.close()
    expect(load.mock.calls[0]?.[0]?.signal.aborted).toBe(true)
    expect(await loading).toBeNull()
    gate.resolve({ lastFiredAt: 1 })
    expect(logs).toEqual([])
    expect(vi.getTimerCount()).toBe(0)
  })

  it("continues ticking after a save timeout and includes it in close errors", async () => {
    const never = Promise.withResolvers<void>()
    const { ctx } = setup({ load: async () => null, save: async () => {} })
    const source = new FlumeTimeSource({
      cron: "* * * * *",
      stateTimeoutMs: 100,
      statePersister: { load: async () => null, save: () => never.promise },
    })
    const received: number[] = []
    const running = await new Flume({
      sources: [source],
      deps: ctx.deps,
      onEvent: (item) => {
        if (item.kind === "event") received.push(item.event.receivedAt)
      },
    }).open()
    if (running instanceof Error) throw running
    await vi.advanceTimersByTimeAsync(180_000)
    expect(received).toEqual([60_000, 120_000, 180_000])
    expect(source.stateStatus()).toMatchObject({
      phase: "halted",
      lastRequestedAt: 180_000,
      failedWrites: 1,
    })
    const closed = await running.close()
    await running.drain()
    expect(closed.errors()).toHaveLength(1)
    expect(closed.errors()[0]?.error).toMatchObject({ errorCode: "FLUME_TIMEOUT" })
    expect(vi.getTimerCount()).toBe(0)
    never.resolve()
  })
})
