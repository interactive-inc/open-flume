import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeLogger } from "@/logger"
import { FlumeTimeSource } from "@/time/time-source"
import { FlumeStateError, FlumeOperationError, FlumeConnectionError } from "@/index"
import { FlumeTimeState } from "@/time/time-state"
import { Flume } from "@/flume"
import type { FlumeLog, FlumeStatePersister, FlumeTimeSourceState } from "@/types"

function setup(
  persister: FlumeStatePersister<FlumeTimeSourceState>,
  timeoutMs = 100,
  onLog?: (log: FlumeLog) => void,
) {
  const logs: FlumeLog[] = []
  const deps = createFlumeDefaultDeps()
  const ctx = {
    deps,
    log: new FlumeLogger({
      source: "time",
      deps,
      handler: (log: FlumeLog) => {
        logs.push(log)
        onLog?.(log)
      },
    }),
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
  it("removes a retained checkpoint when its timed-out write succeeds late without resuming saves", async () => {
    const gate = Promise.withResolvers<void>()
    const save = vi.fn(async () => gate.promise)
    const { state } = setup({ load: async () => null, save })
    state.save(1)
    await vi.advanceTimersByTimeAsync(100)
    expect(state.status()).toMatchObject({ phase: "halted", pendingWrites: 2 })
    gate.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(state.status()).toMatchObject({ phase: "halted", pendingWrites: 0, lastSavedAt: 1 })
    state.save(2)
    expect(state.status()).toMatchObject({ phase: "halted", pendingWrites: 1, coalescedWrites: 0 })
    expect(save).toHaveBeenCalledTimes(1)
    await state.close()
    expect(vi.getTimerCount()).toBe(0)
  })

  it("shares shutdown completion, drains accepted state and rejects new requests", async () => {
    const gate = Promise.withResolvers<void>()
    const save = vi.fn(async (_state: FlumeTimeSourceState) => gate.promise)
    const { state } = setup({ load: async () => null, save })
    state.save(1)
    state.save(2)
    const snapshot = state.status()
    const closing = state.close()
    expect(state.close()).toBe(closing)
    state.save(3)
    expect(state.status()).toMatchObject({
      lifecycle: "closing",
      phase: "saving",
      lastRequestedAt: 2,
    })
    gate.resolve()
    expect(await closing).toBeNull()
    expect(state.close()).toBe(closing)
    expect(state.status()).toMatchObject({
      lifecycle: "closed",
      phase: "idle",
      pendingWrites: 0,
      lastSavedAt: 2,
    })
    expect(save.mock.calls.map(([saved]) => saved.lastFiredAt)).toEqual([1, 2])
    expect(snapshot).toMatchObject({ lifecycle: "open", phase: "saving", pendingWrites: 2 })
    expect(Object.isFrozen(snapshot)).toBe(true)
    expect(vi.getTimerCount()).toBe(0)
  })

  it("preserves the latest queued value when a failure observer saves and closes", async () => {
    const failure = new Error("write rejected")
    const save = vi.fn(async (saved: FlumeTimeSourceState) => {
      if (saved.lastFiredAt === 1) throw failure
    })
    let closing: Promise<FlumeStateError | null> | undefined
    const { state } = setup({ load: async () => null, save }, 100, (log) => {
      if (log.action !== "time.state.save.error") return
      expect(state.status().error?.cause).toBe(failure)
      state.save(3)
      closing = state.close()
    })
    state.save(1)
    state.save(2)
    await vi.advanceTimersByTimeAsync(0)
    expect(closing).toBeDefined()
    expect(await closing).toBeNull()
    expect(save.mock.calls.map(([saved]) => saved.lastFiredAt)).toEqual([1, 3])
    expect(state.status()).toMatchObject({
      lifecycle: "closed",
      phase: "idle",
      coalescedWrites: 1,
      failedWrites: 1,
      lastSavedAt: 3,
      error: null,
    })
    expect(vi.getTimerCount()).toBe(0)
  })

  it("serializes requests made synchronously inside the persister", async () => {
    const gate = Promise.withResolvers<void>()
    const save = vi.fn(async (saved: FlumeTimeSourceState) => {
      if (saved.lastFiredAt !== 1) return
      state.save(2)
      state.save(3)
      await gate.promise
    })
    const { state } = setup({ load: async () => null, save })
    state.save(1)
    expect(save).toHaveBeenCalledTimes(1)
    expect(state.status()).toMatchObject({ pendingWrites: 2, coalescedWrites: 1 })
    gate.resolve()
    await state.close()
    expect(save.mock.calls.map(([saved]) => saved.lastFiredAt)).toEqual([1, 3])
    expect(vi.getTimerCount()).toBe(0)
  })

  it("retains an observable failure without starting IO if the deadline timer fails", async () => {
    const failure = new Error("timer unavailable")
    const save = vi.fn(async () => {})
    const { state, ctx, logs } = setup({ load: async () => null, save })
    ctx.deps.setTimeout = () => {
      throw failure
    }
    state.save(1)
    const error = await state.close()
    expect(save).not.toHaveBeenCalled()
    expect(state.status()).toMatchObject({
      lifecycle: "closed",
      phase: "halted",
      pendingWrites: 1,
      failedWrites: 1,
    })
    expect(error).toBeInstanceOf(FlumeStateError)
    expect(error?.cause).toBeInstanceOf(FlumeOperationError)
    expect(error?.cause).toMatchObject({ errorCode: "FLUME_TIMER_FAILED", cause: failure })
    expect(logs[0]).toMatchObject({
      errorCode: "FLUME_TIMER_FAILED",
      detail: { operation: "time.state.save", phase: "save" },
    })
    expect(vi.getTimerCount()).toBe(0)
  })

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
    const error = await state.close()
    expect(error).toBeInstanceOf(FlumeStateError)
    expect(error).toMatchObject({
      errorCode: "FLUME_TIMEOUT",
      cause: { name: "FlumeOperationError", errorCode: "FLUME_TIMEOUT" },
    })
    gate.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(save).toHaveBeenCalledTimes(1)
    expect(state.status()).toMatchObject({ phase: "halted", pendingWrites: 1, lastSavedAt: 1 })
    expect(vi.getTimerCount()).toBe(0)
  })

  it("returns a storage error retaining the exact original cause on shutdown", async () => {
    const failure = new Error("storage unavailable")
    const { state, logs } = setup({
      load: async () => null,
      save: async () => {
        throw failure
      },
    })
    state.save(1)
    const error = await state.close()
    expect(error).toBeInstanceOf(FlumeStateError)
    expect(error).not.toBeInstanceOf(FlumeConnectionError)
    expect(error?.cause).toBe(failure)
    expect(state.status()).toMatchObject({
      error,
      failedWrites: 1,
      lastSavedAt: null,
      lastRequestedAt: 1,
    })
    expect(logs[0]).toMatchObject({ errorCode: "FLUME_STATE_FAILED", error })
    expect(vi.getTimerCount()).toBe(0)
  })

  it("bounds load waiting and exposes invalid stored state", async () => {
    const never = Promise.withResolvers<FlumeTimeSourceState | null>()
    const { state, logs } = setup({ load: () => never.promise, save: async () => {} })
    const loading = state.load()
    await vi.advanceTimersByTimeAsync(100)
    expect(await loading).toBeNull()
    expect(logs[0]).toMatchObject({
      errorCode: "FLUME_TIMEOUT",
      error: { name: "FlumeStateError" },
      detail: { operation: "time.state.load", phase: "load" },
    })
    expect(await state.close()).toBeNull()
    never.resolve({ lastFiredAt: 10 })
    const invalid = setup({
      load: async () => ({ lastFiredAt: Number.MAX_VALUE }),
      save: async () => {},
    })
    expect(await invalid.state.load()).toBeNull()
    expect(invalid.logs[0]).toMatchObject({
      errorCode: "FLUME_STATE_FAILED",
      error: { name: "FlumeStateError", context: { phase: "validate" } },
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
