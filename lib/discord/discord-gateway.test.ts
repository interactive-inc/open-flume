import { describe, it, expect, vi } from "vitest"
import type { FlumeRuntimeDeps } from "@/types"
import { FlumeDiscordGateway } from "@/discord/discord-gateway"
import { FlumeConnectionError } from "@/errors/connection-error"

type Listener = (ev: unknown) => void

class MockWebSocket {
  static readonly OPEN = 1
  static readonly CLOSED = 3
  static latest: MockWebSocket | null = null

  readonly url: string

  readyState = MockWebSocket.OPEN

  readonly sentMessages: Array<string> = []

  private readonly listeners: Record<string, Array<Listener>> = {}

  constructor(url: string | URL) {
    this.url = String(url)
    MockWebSocket.latest = this
  }

  addEventListener(type: string, fn: Listener): void {
    if (!this.listeners[type]) {
      this.listeners[type] = []
    }
    this.listeners[type].push(fn)
  }

  send(data: string): void {
    this.sentMessages.push(data)
  }

  close(code?: number, reason?: string): void {
    this.readyState = MockWebSocket.CLOSED
    const listeners = this.listeners["close"] ?? []
    for (const fn of listeners) {
      fn({ code: code ?? 1000, reason: reason ?? "" })
    }
  }

  simulateMessage(data: string): void {
    const listeners = this.listeners["message"] ?? []
    for (const fn of listeners) {
      fn({ data })
    }
  }

  simulateClose(code: number, reason: string): void {
    this.readyState = MockWebSocket.CLOSED
    const listeners = this.listeners["close"] ?? []
    for (const fn of listeners) {
      fn({ code, reason })
    }
  }

  simulateError(): void {
    const listeners = this.listeners["error"] ?? []
    for (const fn of listeners) {
      fn({})
    }
  }
}

type Deps = Pick<
  FlumeRuntimeDeps,
  "WebSocket" | "setInterval" | "clearInterval" | "setTimeout" | "random" | "now"
>

const createMockDeps = (): Deps => {
  const timerHandle = globalThis.setTimeout(() => {}, 0)
  globalThis.clearTimeout(timerHandle)

  return {
    WebSocket: MockWebSocket as unknown as Deps["WebSocket"],
    setInterval: vi.fn((_fn: () => void, _ms: number) => timerHandle),
    clearInterval: vi.fn(),
    setTimeout: vi.fn((_fn: () => void, _ms: number) => timerHandle),
    random: () => 0.5,
    now: () => 1000,
  }
}

const HELLO_MSG = '{"op":10,"d":{"heartbeat_interval":45000},"s":null,"t":null}'
const READY_MSG = '{"op":0,"d":{"session_id":"abc","resume_gateway_url":"wss://resume.example.com"},"s":1,"t":"READY"}'
const HEARTBEAT_ACK_MSG = '{"op":11,"d":null,"s":null,"t":null}'
const RECONNECT_MSG = '{"op":7,"d":null,"s":null,"t":null}'
const INVALID_SESSION_NULL_MSG = '{"op":9,"d":null,"s":null,"t":null}'
const INVALID_SESSION_RESUMABLE_MSG = '{"op":9,"d":{"resumable":true},"s":null,"t":null}'

const createGateway = () => {
  const deps = createMockDeps()
  const onDispatch = vi.fn()
  const onStatus = vi.fn()

  MockWebSocket.latest = null

  const gateway = new FlumeDiscordGateway({
    token: "test-token",
    intents: 513,
    onDispatch,
    onStatus,
    deps,
  })

  return { gateway, deps, onDispatch, onStatus }
}

describe("FlumeDiscordGateway", () => {
  it("connect creates WebSocket with gateway URL", () => {
    const ctx = createGateway()

    ctx.gateway.connect()

    expect(MockWebSocket.latest).not.toBeNull()
    expect(MockWebSocket.latest!.url).toBe("wss://gateway.discord.gg/?v=10&encoding=json")
  })

  it("HELLO triggers heartbeat and sends IDENTIFY", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    expect(ctx.deps.setInterval).toHaveBeenCalled()

    const identifyMessages = MockWebSocket.latest!.sentMessages.filter((msg) => {
      const parsed = JSON.parse(msg)
      return parsed.op === 2
    })

    expect(identifyMessages.length).toBe(1)

    const identify = JSON.parse(identifyMessages[0]!)

    expect(identify.d.token).toBe("test-token")
    expect(identify.d.intents).toBe(513)
  })

  it("READY dispatch resolves connect with null and calls onStatus connected", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)

    const connectResult = await connectPromise

    expect(connectResult).toBeNull()
    expect(ctx.onStatus).toHaveBeenCalledWith("connected")
  })

  it("dispatch events call onDispatch", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)

    await connectPromise

    const messageCreate = '{"op":0,"d":{"content":"hello","channel_id":"123"},"s":2,"t":"MESSAGE_CREATE"}'

    MockWebSocket.latest!.simulateMessage(messageCreate)

    expect(ctx.onDispatch).toHaveBeenCalledWith("MESSAGE_CREATE", { content: "hello", channel_id: "123" })
  })

  it("HEARTBEAT_ACK calls heartbeat ack", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(HEARTBEAT_ACK_MSG)

    expect(ctx.gateway.isConnected()).toBe(true)
  })

  it("server RECONNECT request closes socket", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    const closeSpy = vi.spyOn(ws, "close")

    ws.simulateMessage(RECONNECT_MSG)

    expect(closeSpy).toHaveBeenCalledWith(4000, "reconnect requested")
  })

  it("INVALID_SESSION with d=false closes socket", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    const closeSpy = vi.spyOn(ws, "close")

    ws.simulateMessage(HELLO_MSG)
    ws.simulateMessage(INVALID_SESSION_NULL_MSG)

    expect(closeSpy).toHaveBeenCalledWith(4000, "invalid session")
  })

  it("INVALID_SESSION with d=true schedules re-identify", () => {
    const ctx = createGateway()

    ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(INVALID_SESSION_RESUMABLE_MSG)

    expect(ctx.deps.setTimeout).toHaveBeenCalled()
  })

  it("disconnect() stops heartbeat and closes WebSocket", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)

    await connectPromise

    ctx.gateway.disconnect()

    expect(ctx.gateway.stopped).toBe(true)
    expect(ctx.deps.clearInterval).toHaveBeenCalled()
  })

  it("WebSocket close event resolves connect with error", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateClose(1006, "abnormal closure")

    const connectResult = await connectPromise

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
  })
})
