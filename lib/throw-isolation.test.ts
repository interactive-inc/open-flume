import { describe, expect, it, vi } from "vitest"
import type { FlumeRuntimeDeps } from "@/types"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { FlumeDiscordSource } from "@/discord/discord-source"

class MockWebSocket {
  static latest: MockWebSocket | null = null
  static readonly OPEN = 1

  readonly url: string
  readyState = MockWebSocket.OPEN
  readonly sentMessages: string[] = []
  private readonly listeners: Record<string, Array<(ev: unknown) => void>> = {}

  constructor(url: string | URL) {
    this.url = String(url)
    MockWebSocket.latest = this
  }

  addEventListener(type: string, fn: (ev: unknown) => void): void {
    if (!this.listeners[type]) this.listeners[type] = []
    this.listeners[type].push(fn)
  }

  send(data: string): void {
    this.sentMessages.push(data)
  }

  close(): void {
    const fns = this.listeners["close"] ?? []
    for (const fn of fns) fn({ code: 1000, reason: "" })
  }

  simulateMessage(data: string): void {
    const fns = this.listeners["message"] ?? []
    for (const fn of fns) fn({ data })
  }
}

const HELLO = '{"op":10,"d":{"heartbeat_interval":45000},"s":null,"t":null}'
const READY =
  '{"op":0,"d":{"session_id":"abc","resume_gateway_url":"wss://r.example"},"s":1,"t":"READY"}'

const createDeps = (): FlumeRuntimeDeps => ({
  WebSocket: MockWebSocket as unknown as FlumeRuntimeDeps["WebSocket"],
  fetch: vi.fn(),
  now: () => 1000,
  random: () => 0.5,
  setTimeout: vi.fn((_fn, _ms) => 1),
  clearTimeout: vi.fn(),
  setInterval: vi.fn((_fn, _ms) => 2),
  clearInterval: vi.fn(),
})

describe("throw isolation: public surface never throws", () => {
  it("source.start does not throw when onLog throws", async () => {
    MockWebSocket.latest = null
    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: false,
      deps: createDeps(),
      onLog: () => {
        throw new Error("onLog boom")
      },
    })

    const startPromise = source.start(vi.fn())
    MockWebSocket.latest!.simulateMessage(HELLO)
    MockWebSocket.latest!.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
  })

  it("source does not crash when onStatus throws", async () => {
    MockWebSocket.latest = null
    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: false,
      deps: createDeps(),
      onStatus: () => {
        throw new Error("onStatus boom")
      },
    })

    const startPromise = source.start(vi.fn())
    MockWebSocket.latest!.simulateMessage(HELLO)
    MockWebSocket.latest!.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
    expect(source.status()).toBe("connected")
  })

  it("source.start returns FlumeStartError when WebSocket constructor throws synchronously", async () => {
    const ThrowingWS = class {
      constructor(_url: string | URL) {
        throw new TypeError("bad url")
      }
    } as unknown as FlumeRuntimeDeps["WebSocket"]

    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: false,
      deps: { ...createDeps(), WebSocket: ThrowingWS },
    })

    const result = await source.start(vi.fn())

    expect(result).toBeInstanceOf(Error)
  })

  it("source.start does not throw when deps.now throws", async () => {
    const deps = createDeps()
    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: false,
      deps: {
        ...deps,
        now: () => {
          throw new Error("now boom")
        },
      },
    })

    const startPromise = source.start(vi.fn())
    MockWebSocket.latest?.simulateMessage(HELLO)
    MockWebSocket.latest?.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
  })

  it("Flume.start does not throw when a source.start throws synchronously", async () => {
    const throwingSource = {
      name: "discord" as const,
      start: (): Promise<Error | null> => {
        throw new Error("sync-start-throw")
      },
      stop: async (): Promise<void> => {},
      status: () => "disconnected" as const,
    }

    const flume = new Flume({ sources: [throwingSource] })
    const result = await flume.start(vi.fn())

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) {
      expect(result.message).toContain("sync-start-throw")
    }
  })

  it("FlumeRunning.stop does not throw when a source.stop throws synchronously", async () => {
    const okStart = vi.fn().mockResolvedValue(null)
    const throwingStop = (): Promise<void> => {
      throw new Error("sync-stop-throw")
    }
    const source = {
      name: "discord" as const,
      start: okStart,
      stop: throwingStop,
      status: () => "connected" as const,
    }

    const flume = new Flume({ sources: [source] })
    const running = await flume.start(vi.fn())
    if (!(running instanceof FlumeRunning)) throw new Error("expected FlumeRunning")

    const stopped = await running.stop()
    expect(stopped.statuses()[0]?.source).toBe("discord")
  })

  it("Flume.start does not throw when source.status throws", async () => {
    const source = {
      name: "discord" as const,
      start: async (): Promise<Error | null> => null,
      stop: async (): Promise<void> => {},
      status: (): "connected" => {
        throw new Error("status-boom")
      },
    }

    const flume = new Flume({ sources: [source] })
    const running = await flume.start(vi.fn())
    if (!(running instanceof FlumeRunning)) throw new Error("expected FlumeRunning")

    const statuses = running.statuses()
    expect(statuses[0]?.status).toBe("disconnected")
  })

  it("source.start returns FlumeStartError when deps.WebSocket getter throws", async () => {
    const deps = createDeps()
    const cursedDeps = new Proxy(deps, {
      get(target, prop, receiver) {
        if (prop === "WebSocket") throw new Error("ws-getter-boom")
        return Reflect.get(target, prop, receiver)
      },
    }) as FlumeRuntimeDeps

    const source = new FlumeDiscordSource({ token: "t", reconnect: false, deps: cursedDeps })
    const result = await source.start(vi.fn())
    expect(result).toBeInstanceOf(Error)
  })

  it("source.start returns FlumeStartError when signal.aborted getter throws", async () => {
    const poisoned = {
      get aborted(): boolean {
        throw new Error("getter-boom")
      },
      addEventListener: () => {},
      removeEventListener: () => {},
    } as unknown as AbortSignal

    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: false,
      signal: poisoned,
      deps: createDeps(),
    })
    const result = await source.start(vi.fn())
    expect(result).toBeInstanceOf(Error)
  })

  it("Flume.start does not throw when deps.setTimeout throws in source", async () => {
    const deps = createDeps()
    const setTimeoutMock = vi.fn(() => {
      throw new Error("timer-denied")
    })
    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: { maxAttempts: 0 },
      deps: { ...deps, setTimeout: setTimeoutMock as unknown as FlumeRuntimeDeps["setTimeout"] },
    })

    MockWebSocket.latest = null
    const startPromise = source.start(vi.fn())
    const ws = MockWebSocket.latest as MockWebSocket | null
    ws?.simulateMessage(HELLO)
    ws?.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
  })

  it("Flume.start does not throw when third-party source.name getter throws", async () => {
    const hostileSource = {
      get name(): "discord" {
        throw new Error("name-getter-boom")
      },
      start: async (): Promise<Error | null> => null,
      stop: async (): Promise<void> => {},
      status: () => "connected" as const,
    }

    const flume = new Flume({ sources: [hostileSource] })
    const running = await flume.start(vi.fn())

    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof FlumeRunning) {
      const stopped = await running.stop()
      expect(stopped).toBeDefined()
    }
  })

  it("source.start does not throw when handler rejects with poisoned object", async () => {
    MockWebSocket.latest = null
    const cursedThrower = {
      [Symbol.toPrimitive]() {
        throw new Error("toPrim-boom")
      },
    }
    const handler = vi.fn(() => {
      throw cursedThrower
    })

    const source = new FlumeDiscordSource({
      token: "t",
      reconnect: false,
      deps: createDeps(),
    })

    const startPromise = source.start(handler as unknown as Parameters<typeof source.start>[0])
    MockWebSocket.latest!.simulateMessage(HELLO)
    MockWebSocket.latest!.simulateMessage(READY)
    const result = await startPromise
    expect(result).toBeNull()
  })
})
