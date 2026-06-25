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
  "WebSocket" | "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout" | "random" | "now"
>

const createMockDeps = (): Deps => {
  return {
    WebSocket: MockWebSocket as unknown as Deps["WebSocket"],
    setInterval: vi.fn((_fn: () => void, _ms: number) => 1),
    clearInterval: vi.fn(),
    setTimeout: vi.fn((_fn: () => void, _ms: number) => 2),
    clearTimeout: vi.fn(),
    random: () => 0.5,
    now: () => 1000,
  }
}

const HELLO_MSG = '{"op":10,"d":{"heartbeat_interval":45000},"s":null,"t":null}'
const READY_MSG =
  '{"op":0,"d":{"session_id":"abc","resume_gateway_url":"wss://resume.example.com"},"s":1,"t":"READY"}'
const RESUMED_MSG = '{"op":0,"d":{},"s":2,"t":"RESUMED"}'
const HEARTBEAT_ACK_MSG = '{"op":11,"d":null,"s":null,"t":null}'
const SERVER_HEARTBEAT_MSG = '{"op":1,"d":null,"s":null,"t":null}'
const RECONNECT_MSG = '{"op":7,"d":null,"s":null,"t":null}'
const INVALID_SESSION_NULL_MSG = '{"op":9,"d":false,"s":null,"t":null}'
const INVALID_SESSION_RESUMABLE_MSG = '{"op":9,"d":true,"s":null,"t":null}'

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

  it("HELLO triggers heartbeat scheduling and sends IDENTIFY", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    expect(ctx.deps.setTimeout).toHaveBeenCalled()

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

  it("RESUMED dispatch (resumed session) resolves connect with null", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(RESUMED_MSG)

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

    const messageCreate =
      '{"op":0,"d":{"content":"hello","channel_id":"123"},"s":2,"t":"MESSAGE_CREATE"}'

    MockWebSocket.latest!.simulateMessage(messageCreate)

    expect(ctx.onDispatch).toHaveBeenCalledWith("MESSAGE_CREATE", {
      content: "hello",
      channel_id: "123",
    })
  })

  it("HEARTBEAT_ACK calls heartbeat ack", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(HEARTBEAT_ACK_MSG)

    expect(ctx.gateway.isConnected()).toBe(true)
  })

  it("server HEARTBEAT (op 1) triggers an outbound heartbeat", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    const sentBefore = MockWebSocket.latest!.sentMessages.length
    MockWebSocket.latest!.simulateMessage(SERVER_HEARTBEAT_MSG)

    const heartbeatsSent = MockWebSocket.latest!.sentMessages.slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.op === 1)

    expect(heartbeatsSent.length).toBe(1)
  })

  it("server RECONNECT request closes socket", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    const closeSpy = vi.spyOn(ws, "close")

    ws.simulateMessage(RECONNECT_MSG)

    expect(closeSpy).toHaveBeenCalledWith(4000, "reconnect requested")
  })

  it("INVALID_SESSION with d=false schedules timed close", () => {
    const ctx = createGateway()

    ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    ws.simulateMessage(HELLO_MSG)
    ws.simulateMessage(INVALID_SESSION_NULL_MSG)

    expect(ctx.deps.setTimeout).toHaveBeenCalled()
  })

  it("INVALID_SESSION with d=true schedules timed close without identifying", () => {
    const ctx = createGateway()

    ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    const sentBeforeInvalid = MockWebSocket.latest!.sentMessages.length

    MockWebSocket.latest!.simulateMessage(INVALID_SESSION_RESUMABLE_MSG)

    const sentAfter = MockWebSocket.latest!.sentMessages.slice(sentBeforeInvalid)
    const newIdentifies = sentAfter.map((s) => JSON.parse(s)).filter((m) => m.op === 2)

    expect(newIdentifies.length).toBe(0)
    expect(ctx.deps.setTimeout).toHaveBeenCalled()
  })

  it("disconnect() stops heartbeat, closes WebSocket, and sets isStopped", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)

    await connectPromise

    ctx.gateway.disconnect()

    expect(ctx.gateway.isStopped).toBe(true)
    expect(ctx.deps.clearTimeout).toHaveBeenCalled()
  })

  it("WebSocket close before READY resolves connect with FlumeConnectionError carrying code", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateClose(1006, "abnormal closure")

    const connectResult = await connectPromise

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
    if (connectResult instanceof FlumeConnectionError) {
      expect(connectResult.code).toBe(1006)
    }
  })

  it("terminal close code (4004) sets isStopped to suppress reconnect", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateClose(4004, "authentication failed")

    const connectResult = await connectPromise

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
    if (connectResult instanceof FlumeConnectionError) {
      expect(connectResult.code).toBe(4004)
    }
    expect(ctx.gateway.isStopped).toBe(true)
  })

  it("does NOT emit onStatus('disconnected') when initial close happens before READY", () => {
    const ctx = createGateway()

    ctx.gateway.connect()

    MockWebSocket.latest!.simulateClose(1006, "abnormal")

    const disconnectedCalls = ctx.onStatus.mock.calls.filter((c) => c[0] === "disconnected")
    expect(disconnectedCalls.length).toBe(0)
  })

  it("emits onStatus('disconnected') when the socket closes AFTER READY", async () => {
    const ctx = createGateway()

    const connectPromise = ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)
    await connectPromise

    MockWebSocket.latest!.simulateClose(1006, "later")

    const disconnectedCalls = ctx.onStatus.mock.calls.filter((c) => c[0] === "disconnected")
    expect(disconnectedCalls.length).toBe(1)
  })

  it("does NOT log raw token in IDENTIFY frame", () => {
    const ctx = createGateway()
    const logged: string[] = []
    const gateway = new FlumeDiscordGateway({
      token: "SECRET",
      intents: 513,
      onDispatch: vi.fn(),
      onStatus: vi.fn(),
      onLog: (log) => {
        logged.push(`${log.message} ${JSON.stringify(log.detail ?? {})}`)
      },
      deps: ctx.deps,
    })

    gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    const leaked = logged.some((m) => m.includes("SECRET"))
    expect(leaked).toBe(false)
  })
})
