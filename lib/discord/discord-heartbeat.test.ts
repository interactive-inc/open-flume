import { describe, it, expect, vi } from "vitest"
import { FlumeDiscordHeartbeat } from "@/discord/discord-heartbeat"
import { FlumeLogger } from "@/logger"

describe("FlumeDiscordHeartbeat", () => {
  const createHeartbeat = (random = 0.5) => {
    const onSend = vi.fn()
    const onZombie = vi.fn()
    let intervalCallback = (): void => {}
    let initialCallback = (): void => {}
    const timerHandle = globalThis.setTimeout(() => {}, 0)
    globalThis.clearTimeout(timerHandle)
    const mockSetInterval = vi.fn((fn: () => void, _ms: number) => {
      intervalCallback = fn
      return timerHandle
    })
    const mockClearInterval = vi.fn()
    const mockSetTimeout = vi.fn((fn: () => void, _ms: number) => {
      initialCallback = fn
      return timerHandle
    })
    const mockClearTimeout = vi.fn()
    const log = new FlumeLogger({ source: "test", deps: { now: () => 0 }, handler: () => {} })

    const heartbeat = new FlumeDiscordHeartbeat({
      onSend,
      onZombie,
      log,
      deps: {
        setInterval: mockSetInterval,
        clearInterval: mockClearInterval,
        setTimeout: mockSetTimeout,
        clearTimeout: mockClearTimeout,
        random: () => random,
      },
    })

    const fireInitial = () => initialCallback()
    const tick = () => intervalCallback()

    return {
      heartbeat,
      onSend,
      onZombie,
      mockSetInterval,
      mockClearInterval,
      mockSetTimeout,
      mockClearTimeout,
      fireInitial,
      tick,
    }
  }

  it("start() schedules the first heartbeat with jitter via setTimeout", () => {
    const ctx = createHeartbeat(0.25)

    ctx.heartbeat.start(40000)

    expect(ctx.mockSetTimeout).toHaveBeenCalledWith(expect.any(Function), 10000)
    expect(ctx.mockSetInterval).not.toHaveBeenCalled()
  })

  it("the initial fire installs the recurring interval at the requested cadence", () => {
    const ctx = createHeartbeat(0.5)

    ctx.heartbeat.start(5000)
    ctx.fireInitial()

    expect(ctx.onSend).toHaveBeenCalledTimes(1)
    expect(ctx.mockSetInterval).toHaveBeenCalledWith(expect.any(Function), 5000)
  })

  it("subsequent interval ticks invoke onSend when ack arrives in between", () => {
    const ctx = createHeartbeat(0)

    ctx.heartbeat.start(5000)
    ctx.fireInitial()
    ctx.heartbeat.ack()
    ctx.tick()

    expect(ctx.onSend).toHaveBeenCalledTimes(2)
  })

  it("if ack() is not called between ticks, onZombie is called", () => {
    const ctx = createHeartbeat(0)

    ctx.heartbeat.start(5000)
    ctx.fireInitial()
    ctx.tick()

    expect(ctx.onZombie).toHaveBeenCalledOnce()
  })

  it("ack() prevents zombie detection", () => {
    const ctx = createHeartbeat(0)

    ctx.heartbeat.start(5000)
    ctx.fireInitial()
    ctx.heartbeat.ack()
    ctx.tick()

    expect(ctx.onZombie).not.toHaveBeenCalled()
    expect(ctx.onSend).toHaveBeenCalledTimes(2)
  })

  it("stop() clears the initial setTimeout when called before the first fire", () => {
    const ctx = createHeartbeat(0.5)

    ctx.heartbeat.start(5000)
    ctx.heartbeat.stop()

    expect(ctx.mockClearTimeout).toHaveBeenCalled()
    expect(ctx.mockClearInterval).not.toHaveBeenCalled()
  })

  it("stop() clears the interval after the first fire", () => {
    const ctx = createHeartbeat(0)

    ctx.heartbeat.start(5000)
    ctx.fireInitial()
    ctx.heartbeat.stop()

    expect(ctx.mockClearInterval).toHaveBeenCalled()
  })

  it("isRunning() reflects state", () => {
    const ctx = createHeartbeat(0)

    expect(ctx.heartbeat.isRunning()).toBe(false)
    ctx.heartbeat.start(5000)
    expect(ctx.heartbeat.isRunning()).toBe(true)
    ctx.fireInitial()
    expect(ctx.heartbeat.isRunning()).toBe(true)
    ctx.heartbeat.stop()
    expect(ctx.heartbeat.isRunning()).toBe(false)
  })

  it("start() after stop() restarts cleanly", () => {
    const ctx = createHeartbeat(0)

    ctx.heartbeat.start(5000)
    ctx.heartbeat.stop()
    ctx.heartbeat.start(3000)
    ctx.fireInitial()

    expect(ctx.mockSetTimeout).toHaveBeenCalledTimes(2)
    expect(ctx.onSend).toHaveBeenCalledOnce()
  })
})
