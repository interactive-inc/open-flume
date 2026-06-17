import { describe, it, expect, vi } from "vitest"
import { FlumeDiscordHeartbeat } from "@/discord/discord-heartbeat"

describe("FlumeDiscordHeartbeat", () => {
  const createHeartbeat = () => {
    const onSend = vi.fn()
    const onZombie = vi.fn()
    let latestCallback = (): void => {}
    const timerHandle = globalThis.setTimeout(() => {}, 0)
    globalThis.clearTimeout(timerHandle)
    const mockSetInterval = vi.fn((fn: () => void, _ms: number) => {
      latestCallback = fn
      return timerHandle
    })
    const mockClearInterval = vi.fn()

    const heartbeat = new FlumeDiscordHeartbeat({
      onSend,
      onZombie,
      deps: {
        setInterval: mockSetInterval,
        clearInterval: mockClearInterval,
      },
    })

    const tick = () => latestCallback()

    return { heartbeat, onSend, onZombie, mockSetInterval, mockClearInterval, tick }
  }

  it("start() calls setInterval with given interval", () => {
    const ctx = createHeartbeat()

    ctx.heartbeat.start(5000)

    expect(ctx.mockSetInterval).toHaveBeenCalledWith(expect.any(Function), 5000)
  })

  it("after start, interval callback calls onSend", () => {
    const ctx = createHeartbeat()

    ctx.heartbeat.start(5000)
    ctx.tick()

    expect(ctx.onSend).toHaveBeenCalledOnce()
  })

  it("if ack() not called between intervals, onZombie is called", () => {
    const ctx = createHeartbeat()

    ctx.heartbeat.start(5000)
    ctx.tick()
    ctx.tick()

    expect(ctx.onZombie).toHaveBeenCalledOnce()
  })

  it("ack() prevents zombie detection", () => {
    const ctx = createHeartbeat()

    ctx.heartbeat.start(5000)
    ctx.tick()
    ctx.heartbeat.ack()
    ctx.tick()

    expect(ctx.onZombie).not.toHaveBeenCalled()
    expect(ctx.onSend).toHaveBeenCalledTimes(2)
  })

  it("stop() calls clearInterval", () => {
    const ctx = createHeartbeat()

    ctx.heartbeat.start(5000)
    ctx.heartbeat.stop()

    expect(ctx.mockClearInterval).toHaveBeenCalled()
  })

  it("isRunning() reflects state", () => {
    const ctx = createHeartbeat()

    expect(ctx.heartbeat.isRunning()).toBe(false)

    ctx.heartbeat.start(5000)

    expect(ctx.heartbeat.isRunning()).toBe(true)

    ctx.heartbeat.stop()

    expect(ctx.heartbeat.isRunning()).toBe(false)
  })

  it("start() after stop() restarts cleanly", () => {
    const ctx = createHeartbeat()

    ctx.heartbeat.start(5000)
    ctx.heartbeat.stop()
    ctx.heartbeat.start(3000)

    expect(ctx.mockSetInterval).toHaveBeenCalledTimes(2)
    expect(ctx.heartbeat.isRunning()).toBe(true)

    ctx.tick()

    expect(ctx.onSend).toHaveBeenCalledOnce()
  })
})
