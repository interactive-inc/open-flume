import { describe, expect, it, vi } from "vitest"
import type { FlumeEvent, FlumeRuntimeDeps, FlumeSourceStartContext } from "@/types"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { FlumeSource } from "@/flume-source"
import { FlumeLogger } from "@/logger"

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

type CtxProps = {
  deps: FlumeRuntimeDeps
  onEvent?: (event: FlumeEvent) => void
  onLog?: (log: import("@/types").FlumeLog) => void
  onStatus?: (status: import("@/types").FlumeStatus, detail?: string) => void
}

const createCtx = (props: CtxProps): FlumeSourceStartContext => ({
  onEvent: props.onEvent ?? (() => {}),
  log: new FlumeLogger({ source: "test", deps: props.deps, handler: props.onLog }),
  deps: props.deps,
  onStatus: props.onStatus ?? (() => {}),
  reconnect: null,
})

describe("throw isolation: public surface never throws", () => {
  it("source.start does not throw when onLog throws", async () => {
    MockWebSocket.latest = null
    const source = new FlumeDiscordSource({ token: "t" })

    const startPromise = source.start(
      createCtx({
        deps: createDeps(),
        onLog: () => {
          throw new Error("onLog boom")
        },
      }),
    )
    MockWebSocket.latest!.simulateMessage(HELLO)
    MockWebSocket.latest!.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
  })

  it("source does not crash when onStatus throws", async () => {
    MockWebSocket.latest = null
    const source = new FlumeDiscordSource({ token: "t" })

    const startPromise = source.start(
      createCtx({
        deps: createDeps(),
        onStatus: () => {
          throw new Error("onStatus boom")
        },
      }),
    )
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

    const source = new FlumeDiscordSource({ token: "t" })
    const result = await source.start(
      createCtx({ deps: { ...createDeps(), WebSocket: ThrowingWS } }),
    )

    expect(result).toBeInstanceOf(Error)
  })

  it("source.start does not throw when deps.now throws", async () => {
    const source = new FlumeDiscordSource({ token: "t" })

    const startPromise = source.start(
      createCtx({
        deps: {
          ...createDeps(),
          now: () => {
            throw new Error("now boom")
          },
        },
      }),
    )
    MockWebSocket.latest?.simulateMessage(HELLO)
    MockWebSocket.latest?.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
  })

  class SyncThrowOnConnect extends FlumeSource {
    readonly name = "discord" as const
    protected async connect(_ctx: FlumeSourceStartContext): Promise<Error | null> {
      throw new Error("sync-start-throw")
    }
    protected disconnect(): void {}
  }

  it("Flume.start does not throw when a source.connect throws synchronously", async () => {
    const flume = new Flume([new SyncThrowOnConnect()], { onEvent: vi.fn() })
    const result = await flume.start()

    expect(result).toBeInstanceOf(Error)
    if (result instanceof Error) {
      expect(result.message).toContain("sync-start-throw")
    }
  })

  class ThrowOnDisconnect extends FlumeSource {
    readonly name = "discord" as const
    protected async connect(_ctx: FlumeSourceStartContext): Promise<Error | null> {
      this.setStatus("connected")
      return null
    }
    protected disconnect(): void {
      throw new Error("sync-stop-throw")
    }
  }

  it("FlumeRunning.stop does not throw when a source.disconnect throws synchronously", async () => {
    const flume = new Flume([new ThrowOnDisconnect()], { onEvent: vi.fn() })
    const running = await flume.start()
    if (!(running instanceof FlumeRunning)) throw new Error("expected FlumeRunning")

    const stopped = await running.stop()
    expect(stopped.statuses()[0]?.source).toBe("discord")
  })

  class CursedNameSource extends FlumeSource {
    get name(): "discord" {
      throw new Error("name-getter-boom")
    }
    protected async connect(_ctx: FlumeSourceStartContext): Promise<Error | null> {
      this.setStatus("connected")
      return null
    }
    protected disconnect(): void {}
  }

  it("Flume.start does not throw when third-party source.name getter throws", async () => {
    const flume = new Flume([new CursedNameSource()], { onEvent: vi.fn() })
    const running = await flume.start()

    expect(running).toBeInstanceOf(FlumeRunning)
    if (running instanceof FlumeRunning) {
      const stopped = await running.stop()
      expect(stopped).toBeDefined()
    }
  })

  it("source.start returns FlumeStartError when deps.WebSocket getter throws", async () => {
    const deps = createDeps()
    const cursedDeps = new Proxy(deps, {
      get(target, prop, receiver) {
        if (prop === "WebSocket") throw new Error("ws-getter-boom")
        return Reflect.get(target, prop, receiver)
      },
    }) as FlumeRuntimeDeps

    const source = new FlumeDiscordSource({ token: "t" })
    const result = await source.start(createCtx({ deps: cursedDeps }))
    expect(result).toBeInstanceOf(Error)
  })

  it("Flume.start returns FlumeStartError when signal.aborted getter throws", async () => {
    const poisoned = {
      get aborted(): boolean {
        throw new Error("getter-boom")
      },
      addEventListener: () => {},
      removeEventListener: () => {},
    } as unknown as AbortSignal

    const source = new FlumeDiscordSource({ token: "t" })
    const flume = new Flume([source], { onEvent: vi.fn(), signal: poisoned })
    const result = await flume.start()
    expect(result).toBeInstanceOf(Error)
  })

  it("Flume.start does not throw when deps.setTimeout throws in source", async () => {
    const setTimeoutMock = vi.fn(() => {
      throw new Error("timer-denied")
    })
    const source = new FlumeDiscordSource({ token: "t" })

    MockWebSocket.latest = null
    const startPromise = source.start(
      createCtx({
        deps: {
          ...createDeps(),
          setTimeout: setTimeoutMock as unknown as FlumeRuntimeDeps["setTimeout"],
        },
      }),
    )
    const ws = MockWebSocket.latest as MockWebSocket | null
    ws?.simulateMessage(HELLO)
    ws?.simulateMessage(READY)
    const result = await startPromise

    expect(result).toBeNull()
  })

  it("source.start does not throw when onEvent rejects with poisoned object", async () => {
    MockWebSocket.latest = null
    const cursedThrower = {
      [Symbol.toPrimitive]() {
        throw new Error("toPrim-boom")
      },
    }
    const onEvent = vi.fn(() => {
      throw cursedThrower
    })

    const source = new FlumeDiscordSource({ token: "t" })

    const startPromise = source.start(
      createCtx({
        deps: createDeps(),
        onEvent: onEvent as unknown as (event: FlumeEvent) => void,
      }),
    )
    MockWebSocket.latest!.simulateMessage(HELLO)
    MockWebSocket.latest!.simulateMessage(READY)
    const result = await startPromise
    expect(result).toBeNull()
  })
})
