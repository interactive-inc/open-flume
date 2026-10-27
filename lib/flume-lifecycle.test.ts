import { describe, expect, it, vi } from "vitest"
import { Flume } from "@/flume"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeConfluence } from "@/flume-confluence"
import { FlumeRunning } from "@/flume-running"
import { FlumeSource } from "@/flume-source"
import { waitFor } from "@/test-utils/wait-for"
import type { FlumeSourceStartContext } from "@/types"

class ManualSource extends FlumeSource {
  readonly name = "manual"

  stopCount = 0

  constructor(private readonly connectError: Error | null = null) {
    super()
  }

  protected async connect(_ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connected")
    return this.connectError
  }

  protected disconnect(): void {
    this.stopCount++
  }

  receive(): void {
    this.context?.log.debug({ action: "frame", message: "received frame" })
    this.emit({
      source: "custom",
      sourceName: this.name,
      type: "tick",
      data: {},
      meta: {},
      receivedAt: 0,
    })
  }
}

describe("lifecycle regressions", () => {
  it("preserves a connection failure when the host aborts in the same startup", async () => {
    const controller = new AbortController()
    const failure = new Error("connection failed during cancellation")
    class AbortingSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        controller.abort()
        return failure
      }
    }
    const source = new AbortingSource()
    const flume = new Flume({ sources: [source], signal: controller.signal })
    const result = await flume.open()
    expect(result).toMatchObject({
      errorCode: "FLUME_ABORTED",
      cause: expect.objectContaining({ errors: expect.arrayContaining([failure]) }),
    })
    expect(source.stopCount).toBe(1)
    expect(flume.diagnostics().entries).toContainEqual(expect.objectContaining({ error: result }))
  })

  it("owns the startup deadline and exposes its cause separately from host cancellation", async () => {
    const entered = Promise.withResolvers<void>()
    const timer = { fire: (): void => {} }
    class PendingSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        entered.resolve()
        return new Promise(() => {})
      }
    }
    const source = new PendingSource()
    const cleared = vi.fn()
    const flume = new Flume({
      sources: [source],
      openTimeoutMs: 10,
      deps: {
        ...createFlumeDefaultDeps(),
        setTimeout: (callback) => {
          timer.fire = callback
          return 1
        },
        clearTimeout: cleared,
      },
    })
    const opening = flume.open()
    await entered.promise
    timer.fire()
    const result = await opening
    expect(result).toMatchObject({ errorCode: "FLUME_TIMEOUT", cause: expect.any(AggregateError) })
    expect(source.stopCount).toBe(1)
    expect(source.status()).toBe("disconnected")
    expect(flume.diagnostics().entries).toContainEqual(
      expect.objectContaining({
        action: "flume.open.failed",
        errorCode: "FLUME_TIMEOUT",
        error: result,
      }),
    )
    timer.fire()
    expect(source.stopCount).toBe(1)
    expect(cleared).not.toHaveBeenCalled()
  })

  it.each([0, -1, NaN, Infinity])(
    "refuses invalid startup deadline %s before acquiring a source",
    async (timeout) => {
      const source = new ManualSource()
      const start = vi.spyOn(source, "start")
      const result = await new Flume({ sources: [source], openTimeoutMs: timeout }).open()
      expect(result).toMatchObject({
        errorCode: "FLUME_START_FAILED",
        message: "Flume.open: openTimeoutMs must be a finite positive number",
      })
      expect(start).not.toHaveBeenCalled()
    },
  )

  it("reports a failing abort-state reader as a runtime fault and rolls back", async () => {
    const entered = Promise.withResolvers<void>()
    const ready = Promise.withResolvers<void>()
    class PendingSource extends ManualSource {
      protected override async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
        entered.resolve()
        await ready.promise
        return super.connect(ctx)
      }
    }
    const controller = new AbortController()
    const source = new PendingSource()
    const flume = new Flume({ sources: [source], signal: controller.signal })
    const opening = flume.open()
    await entered.promise
    const failure = new Error("abort state unavailable")
    vi.spyOn(controller.signal, "aborted", "get").mockImplementation(() => {
      throw failure
    })
    ready.resolve()
    const result = await opening
    expect(result).toMatchObject({ errorCode: "FLUME_RUNTIME_FAILED", cause: failure })
    expect(source.stopCount).toBe(1)
    expect(flume.diagnostics().entries).toContainEqual(
      expect.objectContaining({
        action: "flume.open.failed",
        errorCode: "FLUME_RUNTIME_FAILED",
        error: result,
      }),
    )
  })

  it("keeps one host abort subscription across startup and running", async () => {
    const source = new ManualSource()
    const controller = new AbortController()
    const register = controller.signal.addEventListener.bind(controller.signal)
    const add = vi
      .spyOn(controller.signal, "addEventListener")
      .mockImplementation((type, listener, options) => {
        if (add.mock.calls.length > 1) throw new Error("second listener rejected")
        register(type, listener, options)
      })
    const remove = vi.spyOn(controller.signal, "removeEventListener")
    const flume = new Flume({ sources: [source], signal: controller.signal })
    const running = await flume.open()
    if (running instanceof Error) throw running
    try {
      controller.abort()
      await waitFor(() => expect(source.stopCount).toBe(1), { timeout: 100 })
      expect(add).toHaveBeenCalledTimes(1)
    } finally {
      await running.close()
      await running.drain()
    }
    expect(remove).toHaveBeenCalledTimes(1)
    expect(Object.isFrozen(running.statuses())).toBe(true)
    expect(Object.isFrozen(running.statuses()[0])).toBe(true)
  })

  it("keeps the host signal selected at open even if its options object changes", async () => {
    const gate = Promise.withResolvers<void>()
    const entered = Promise.withResolvers<void>()
    class PendingSource extends ManualSource {
      protected override async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
        entered.resolve()
        await gate.promise
        return super.connect(ctx)
      }
    }
    const source = new PendingSource()
    const original = new AbortController()
    const replacement = new AbortController()
    const options = { sources: [source], signal: original.signal }
    const opening = new Flume(options).open()
    await entered.promise
    options.signal = replacement.signal
    gate.resolve()
    const running = await opening
    if (running instanceof Error) throw running
    try {
      expect(running.signal).toBe(original.signal)
      original.abort()
      await waitFor(() => expect(source.stopCount).toBe(1), { timeout: 100 })
    } finally {
      await running.close()
    }
  })

  it("retains a failed host listener cleanup as a structured diagnostic", async () => {
    const controller = new AbortController()
    const failure = new Error("listener cleanup failed")
    vi.spyOn(controller.signal, "removeEventListener").mockImplementation(() => {
      throw failure
    })
    const flume = new Flume({
      sources: [new ManualSource(new Error("connect failed"))],
      signal: controller.signal,
    })
    expect(await flume.open()).toBeInstanceOf(Error)
    expect(flume.diagnostics().entries).toContainEqual(
      expect.objectContaining({
        action: "signal.removeListener.failed",
        errorCode: "FLUME_RUNTIME_FAILED",
        error: failure,
      }),
    )
  })

  it("delivers errors from the final status snapshot before sealing delivery", async () => {
    const failure = new Error("status failed")
    class BrokenStatusSource extends ManualSource {
      override status(): never {
        throw failure
      }
    }
    const source = new BrokenStatusSource()
    const running = await new Flume({ sources: [source] }).open()
    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof Error) return
    const stream = running.stream()
    await running.close()
    const actions: string[] = []
    for await (const item of stream) {
      if (item.kind === "log" && item.log.error === failure) {
        actions.push(item.log.action)
        expect(item.log.errorCode).toBe("FLUME_RUNTIME_FAILED")
      }
    }
    expect(actions).toEqual(["source.status.failed"])
  })

  it("allows onEvent to await close, including after an asynchronous boundary", async () => {
    const source = new ManualSource()
    const owner = Promise.withResolvers<FlumeRunning>()
    const completed = { value: false }
    const running = await new Flume({
      sources: [source],
      onEvent: async (item) => {
        if (item.kind !== "event") return
        const active = await owner.promise
        const closed = await active.close()
        expect(closed.statuses()).toEqual([{ source: "manual", status: "disconnected" }])
        completed.value = true
      },
    }).open()
    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof Error) return

    owner.resolve(running)
    source.receive()
    await waitFor(() => expect(completed.value).toBe(true))
    expect(source.stopCount).toBe(1)
  })

  it("allows onError to await close and retains subsequent callback diagnostics", async () => {
    const source = new ManualSource()
    const owner = Promise.withResolvers<FlumeRunning>()
    const completed = { value: false }
    const running = await new Flume({
      sources: [source],
      onEvent: (item) => {
        if (item.kind === "event") return Promise.reject(new Error("event failure"))
      },
      onError: async () => {
        const active = await owner.promise
        await active.close()
        completed.value = true
        return Promise.reject(new Error("error handler failure after close"))
      },
    }).open()
    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof Error) return

    owner.resolve(running)
    const stream = running.stream()
    source.receive()
    await waitFor(() => expect(completed.value).toBe(true))

    const actions: string[] = []
    for await (const item of stream) {
      if (item.kind === "log") actions.push(item.log.action)
    }
    expect(actions).toContain("onError.error")
  })

  it("does not wait for a callback that awaits the failed open result", async () => {
    const failedOpen = Promise.withResolvers<void>()
    const source = new ManualSource(new Error("connect failed"))
    const running = await new Flume({
      sources: [source],
      onEvent: () => failedOpen.promise,
    }).open()
    expect(running).toBeInstanceOf(Error)
    expect(source.stopCount).toBe(1)
    failedOpen.resolve()
  })

  it("opens without waiting for a blocked startup event callback", async () => {
    const callbackGate = Promise.withResolvers<void>()
    const entered = Promise.withResolvers<void>()
    const completed = { value: false }
    class StartupSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        this.receive()
        return null
      }
    }

    const source = new StartupSource()
    const opening = new Flume({
      sources: [source],
      onEvent: async (item) => {
        if (item.kind !== "event") return
        entered.resolve()
        await callbackGate.promise
      },
    })
      .open()
      .then((running) => {
        completed.value = true
        return running
      })

    await entered.promise
    try {
      source.receive()
      await waitFor(() => expect(completed.value).toBe(true))
    } finally {
      callbackGate.resolve()
      await source.stop()
    }
    const running = await opening
    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof FlumeRunning) await running.close()
  })

  it("does not stop an already running source when another Flume tries to reuse it", async () => {
    const source = new ManualSource()
    const running = await new Flume({ sources: [source] }).open()
    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof Error) return

    expect(await new Flume({ sources: [source] }).open()).toBeInstanceOf(Error)
    expect(source.status()).toBe("connected")
    expect(source.stopCount).toBe(0)
    await running.close()
  })

  it("keeps the old group alive after replace rejects a reused source", async () => {
    const source = new ManualSource()
    const confluence = new FlumeConfluence()
    expect(await confluence.add("team", [source])).toBeNull()
    expect(await confluence.replace("team", [source])).toBeInstanceOf(Error)
    expect(confluence.has("team")).toBe(true)
    expect(source.status()).toBe("connected")
    expect(source.stopCount).toBe(0)
    await confluence.closeAll()
  })

  it("rolls back a duplicate source once and still cleans up partially failed sources", async () => {
    const duplicate = new ManualSource()
    const failing = new ManualSource(new Error("partially connected"))
    expect(await new Flume({ sources: [duplicate, duplicate, failing] }).open()).toBeInstanceOf(
      Error,
    )
    expect(duplicate.stopCount).toBe(1)
    expect(failing.stopCount).toBe(1)
  })

  it("preserves a source claimed by a concurrent open", async () => {
    const source = new ManualSource()
    const outcomes = await Promise.all([
      new Flume({ sources: [source] }).open(),
      new Flume({ sources: [source] }).open(),
    ])
    expect(outcomes.filter((outcome) => outcome instanceof Error)).toHaveLength(1)
    expect(source.status()).toBe("connected")
    expect(source.stopCount).toBe(0)
    for (const outcome of outcomes) {
      if (outcome instanceof FlumeRunning) await outcome.close()
    }
  })

  it("does not stop a source started directly by a host", async () => {
    const source = new ManualSource()
    class ContextSource extends ManualSource {
      protected override async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
        return source.start(ctx)
      }
    }
    const host = await new Flume({ sources: [new ContextSource()] }).open()
    expect(host).toBeInstanceOf(FlumeRunning)
    expect(await new Flume({ sources: [source] }).open()).toBeInstanceOf(Error)
    expect(source.stopCount).toBe(0)
    await source.stop()
    if (host instanceof FlumeRunning) await host.close()
  })
})

describe("resource ownership regressions", () => {
  it("cancels a connect that ignores abort without waiting for its promise", async () => {
    const entered = Promise.withResolvers<void>()
    class HangingSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        entered.resolve()
        return new Promise(() => {})
      }
    }
    const source = new HangingSource()
    const controller = new AbortController()
    const opening = new Flume({ sources: [source], signal: controller.signal }).open()
    await entered.promise
    controller.abort()
    expect(await opening).toMatchObject({ errorCode: "FLUME_ABORTED" })
    expect(source.stopCount).toBe(1)
  })

  it("cancels pending siblings as soon as one source fails", async () => {
    const entered = Promise.withResolvers<void>()
    class HangingSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        entered.resolve()
        return new Promise(() => {})
      }
    }
    class FailingSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        await entered.promise
        return new Error("connection refused")
      }
    }
    const pending = new HangingSource()
    const failed = new FailingSource()
    const result = await new Flume({ sources: [pending, failed] }).open()
    expect(result).toBeInstanceOf(Error)
    expect(pending.stopCount).toBe(1)
    expect(failed.stopCount).toBe(1)
    if (result instanceof Error) expect(result.cause).toBeInstanceOf(AggregateError)
  })

  it("shares stop completion when disconnect re-enters stop", async () => {
    class ReentrantSource extends ManualSource {
      nested: Promise<Error | null> | null = null
      protected override disconnect(): void {
        super.disconnect()
        if (this.stopCount === 1) this.nested = this.stop()
      }
    }
    const source = new ReentrantSource()
    const running = await new Flume({ sources: [source] }).open()
    if (running instanceof Error) throw running
    await running.close()
    expect(await source.nested).toBeNull()
    expect(source.stopCount).toBe(1)
  })

  it("remove aborts a pending add and waits for cleanup", async () => {
    const entered = Promise.withResolvers<void>()
    class HangingSource extends ManualSource {
      protected override async connect(): Promise<Error | null> {
        entered.resolve()
        return new Promise(() => {})
      }
    }
    const source = new HangingSource()
    const groups = new FlumeConfluence()
    const adding = groups.add("pending", [source])
    await entered.promise
    await groups.remove("pending")
    expect(source.stopCount).toBe(1)
    expect(await adding).toBeInstanceOf(Error)
    expect(groups.ids()).toEqual([])
  })

  it("closeAll includes an already running remove", async () => {
    const gate = Promise.withResolvers<void>()
    const entered = Promise.withResolvers<void>()
    class SlowSource extends ManualSource {
      protected override async disconnect(): Promise<void> {
        entered.resolve()
        await gate.promise
        super.disconnect()
      }
    }
    const source = new SlowSource()
    const groups = new FlumeConfluence()
    await groups.add("slow", [source])
    const removing = groups.remove("slow")
    await entered.promise
    let closed = false
    const closing = groups.closeAll().then(() => {
      closed = true
    })
    await Promise.resolve()
    await Promise.resolve()
    expect(closed).toBe(false)
    gate.resolve()
    await Promise.all([removing, closing])
    expect(source.stopCount).toBe(1)
  })

  it("reports bounded callback overflow without recursive diagnostics", async () => {
    const gate = Promise.withResolvers<void>()
    const entered = Promise.withResolvers<void>()
    const errors: string[] = []
    const source = new ManualSource()
    const running = await new Flume({
      sources: [source],
      callbackBuffer: 8,
      onEvent: async (item) => {
        if (item.kind === "event") {
          entered.resolve()
          await gate.promise
        }
      },
      onError: (log) => {
        if (log.errorCode) errors.push(log.errorCode)
      },
    }).open()
    if (running instanceof Error) throw running
    await running.drain()
    const stream = running.stream({ buffer: 500 })
    source.receive()
    await entered.promise
    for (let index = 0; index < 100; index++) source.receive()
    await running.close()
    gate.resolve()
    await running.drain()
    expect(running.deliveryStatus()).toMatchObject({ pendingCallbacks: 0, callbackFailures: 0 })
    expect(running.deliveryStatus().droppedCallbacks).toBeGreaterThan(0)
    expect(Object.isFrozen(running.deliveryStatus())).toBe(true)
    expect(errors.filter((code) => code === "FLUME_BUFFER_OVERFLOW")).toHaveLength(1)
    const actions: string[] = []
    for await (const item of stream) if (item.kind === "log") actions.push(item.log.action)
    expect(actions.filter((action) => action === "callback.overflow")).toHaveLength(1)
  })
})
