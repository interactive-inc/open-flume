import { describe, it, expect, vi } from "vitest"
import type { FlumeRuntimeDeps } from "@/types"
import { FlumeDiscordGateway } from "@/discord/discord-gateway"
import { FlumeDiscordGatewaySession } from "@/discord/discord-gateway-session"
import { FlumeConnectionError } from "@/errors/connection-error"

type Listener = (ev: unknown) => void

class MockWebSocket {
  static readonly OPEN = 1
  static readonly CLOSED = 3
  static latest: MockWebSocket | null = null

  readonly url: string

  readyState = MockWebSocket.OPEN

  // false にすると close() しても close event を配送しない (死んだ TCP 経路の再現)
  emitCloseEvents = true

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
    if (!this.emitCloseEvents) return
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

type FakeTimer = {
  fn: () => void
  ms: number
  kind: "timeout" | "interval"
  cleared: boolean
}

const createTimerDeps = () => {
  const timers: Array<FakeTimer> = []

  const clearByHandle = (handle: unknown) => {
    if (typeof handle === "number" && timers[handle] !== undefined) {
      timers[handle]!.cleared = true
    }
  }

  const deps: Deps = {
    WebSocket: MockWebSocket as unknown as Deps["WebSocket"],
    setTimeout: (fn: () => void, ms: number) => {
      timers.push({ fn, ms, kind: "timeout", cleared: false })
      return timers.length - 1
    },
    clearTimeout: clearByHandle,
    setInterval: (fn: () => void, ms: number) => {
      timers.push({ fn, ms, kind: "interval", cleared: false })
      return timers.length - 1
    },
    clearInterval: clearByHandle,
    random: () => 0.5,
    now: () => 1000,
  }

  const fireTimer = (predicate: (timer: FakeTimer) => boolean): boolean => {
    const target = timers.find((timer) => !timer.cleared && predicate(timer))
    if (!target) return false

    if (target.kind === "timeout") {
      target.cleared = true
    }
    target.fn()
    return true
  }

  return { deps, timers, fireTimer }
}

const createGatewayWithTimers = (options?: {
  session?: FlumeDiscordGatewaySession
  handshakeTimeoutMs?: number
}) => {
  const harness = createTimerDeps()
  const onDispatch = vi.fn()
  const onStatus = vi.fn()

  MockWebSocket.latest = null

  const gateway = new FlumeDiscordGateway({
    token: "test-token",
    intents: 513,
    handshakeTimeoutMs: options?.handshakeTimeoutMs,
    session: options?.session,
    onDispatch,
    onStatus,
    deps: harness.deps,
  })

  return { gateway, harness, onDispatch, onStatus }
}

const createResumableSession = () =>
  new FlumeDiscordGatewaySession({
    sessionId: "sid-1",
    resumeUrl: "wss://resume.example.com",
    seq: 7,
  })

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

describe("FlumeDiscordGateway session and resume", () => {
  it("connect(resumeUrl) appends v=10&encoding=json to the resume url", () => {
    const ctx = createGatewayWithTimers({ session: createResumableSession() })

    ctx.gateway.connect("wss://resume.example.com")

    expect(MockWebSocket.latest!.url).toBe("wss://resume.example.com/?v=10&encoding=json")
  })

  it("injected session sends RESUME instead of IDENTIFY after HELLO", () => {
    const ctx = createGatewayWithTimers({ session: createResumableSession() })

    ctx.gateway.connect("wss://resume.example.com")
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    const frames = MockWebSocket.latest!.sentMessages.map((raw) => JSON.parse(raw))
    const resumes = frames.filter((frame) => frame.op === 6)

    expect(resumes.length).toBe(1)
    expect(resumes[0]!.d.session_id).toBe("sid-1")
    expect(resumes[0]!.d.seq).toBe(7)
    expect(frames.filter((frame) => frame.op === 2).length).toBe(0)
  })

  it("unparseable resume url falls back to gateway url and identifies fresh", () => {
    const ctx = createGatewayWithTimers({ session: createResumableSession() })

    ctx.gateway.connect("::::not-a-url")

    expect(MockWebSocket.latest!.url).toBe("wss://gateway.discord.gg/?v=10&encoding=json")
    expect(ctx.gateway.session.canResume()).toBe(false)

    MockWebSocket.latest!.simulateMessage(HELLO_MSG)

    const frames = MockWebSocket.latest!.sentMessages.map((raw) => JSON.parse(raw))

    expect(frames.filter((frame) => frame.op === 2).length).toBe(1)
    expect(frames.filter((frame) => frame.op === 6).length).toBe(0)
  })

  for (const code of [4007, 4009]) {
    it(`close code ${code} clears the stored session`, async () => {
      const ctx = createGatewayWithTimers()

      const connectPromise = ctx.gateway.connect()
      MockWebSocket.latest!.simulateMessage(HELLO_MSG)
      MockWebSocket.latest!.simulateMessage(READY_MSG)
      await connectPromise

      expect(ctx.gateway.session.canResume()).toBe(true)

      MockWebSocket.latest!.simulateClose(code, "session gone")

      expect(ctx.gateway.session.canResume()).toBe(false)
    })
  }

  it("INVALID_SESSION d=false clears the stored session", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)
    await connectPromise

    MockWebSocket.latest!.simulateMessage(INVALID_SESSION_NULL_MSG)

    expect(ctx.gateway.session.canResume()).toBe(false)
  })
})

describe("FlumeDiscordGateway handshake timeout", () => {
  it("resolves connect with FlumeConnectionError when READY never arrives", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    const fired = ctx.harness.fireTimer((timer) => timer.kind === "timeout" && timer.ms === 30000)

    expect(fired).toBe(true)

    const connectResult = await connectPromise

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
    if (connectResult instanceof FlumeConnectionError) {
      expect(connectResult.message).toContain("handshake timeout")
    }
    expect(ws.readyState).toBe(MockWebSocket.CLOSED)

    const disconnectedCalls = ctx.onStatus.mock.calls.filter((call) => call[0] === "disconnected")
    expect(disconnectedCalls.length).toBe(0)
  })

  it("custom handshakeTimeoutMs arms the timer with that delay", () => {
    const ctx = createGatewayWithTimers({ handshakeTimeoutMs: 1234 })

    ctx.gateway.connect()

    const armed = ctx.harness.timers.some((timer) => timer.kind === "timeout" && timer.ms === 1234)
    expect(armed).toBe(true)
  })

  it("non-positive handshakeTimeoutMs falls back to default 30000", () => {
    const ctx = createGatewayWithTimers({ handshakeTimeoutMs: -1 })

    ctx.gateway.connect()

    const armed = ctx.harness.timers.some((timer) => timer.kind === "timeout" && timer.ms === 30000)
    expect(armed).toBe(true)
  })

  it("READY clears the handshake timer", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()
    MockWebSocket.latest!.simulateMessage(HELLO_MSG)
    MockWebSocket.latest!.simulateMessage(READY_MSG)
    await connectPromise

    const handshakeTimer = ctx.harness.timers.find(
      (timer) => timer.kind === "timeout" && timer.ms === 30000,
    )
    expect(handshakeTimer!.cleared).toBe(true)
  })
})

describe("FlumeDiscordGateway malformed HELLO", () => {
  it("non-numeric heartbeat_interval resolves connect with error and closes the socket", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    ws.simulateMessage('{"op":10,"d":{"heartbeat_interval":"soon"},"s":null,"t":null}')

    const connectResult = await connectPromise

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
    if (connectResult instanceof FlumeConnectionError) {
      expect(connectResult.message).toContain("HELLO")
    }
    expect(ws.readyState).toBe(MockWebSocket.CLOSED)
    expect(ctx.harness.timers.filter((timer) => timer.kind === "interval").length).toBe(0)
  })

  it("missing heartbeat_interval resolves connect with error (no interval-0 flood)", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()

    MockWebSocket.latest!.simulateMessage('{"op":10,"d":{},"s":null,"t":null}')

    const connectResult = await connectPromise

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
    // heartbeat の初回 setTimeout (0.5 * interval) が予約されていないこと
    const heartbeatArmed = ctx.harness.timers.some(
      (timer) => timer.kind === "timeout" && timer.ms === 0,
    )
    expect(heartbeatArmed).toBe(false)
  })
})

describe("FlumeDiscordGateway zombie fallback", () => {
  it("zombie stops heartbeat and synthesizes teardown when the close event never arrives", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    ws.simulateMessage(HELLO_MSG)
    ws.simulateMessage(READY_MSG)
    await connectPromise

    // 死んだ TCP 経路を再現: close() しても close event が届かない
    ws.emitCloseEvents = false

    // 初回 heartbeat (0.5 * 45000) → ACK なしで interval tick → zombie
    expect(ctx.harness.fireTimer((timer) => timer.kind === "timeout" && timer.ms === 22500)).toBe(
      true,
    )
    expect(ctx.harness.fireTimer((timer) => timer.kind === "interval" && timer.ms === 45000)).toBe(
      true,
    )

    // heartbeat は zombie 検知時点で停止 (onZombie が interval ごとに再発火しない)
    const heartbeatInterval = ctx.harness.timers.find(
      (timer) => timer.kind === "interval" && timer.ms === 45000,
    )
    expect(heartbeatInterval!.cleared).toBe(true)

    // fallback timer が teardown を合成し disconnected を通知する
    expect(ctx.harness.fireTimer((timer) => timer.kind === "timeout" && timer.ms === 5000)).toBe(
      true,
    )

    const disconnectedCalls = ctx.onStatus.mock.calls.filter((call) => call[0] === "disconnected")
    expect(disconnectedCalls.length).toBe(1)
    expect(ctx.gateway.isConnected()).toBe(false)

    // 遅れて届いた close は二重通知しない
    ws.emitCloseEvents = true
    ws.simulateClose(4000, "late close")

    const afterLateClose = ctx.onStatus.mock.calls.filter((call) => call[0] === "disconnected")
    expect(afterLateClose.length).toBe(1)
  })

  it("real close event after zombie forced-close cancels the fallback and notifies once", async () => {
    const ctx = createGatewayWithTimers()

    const connectPromise = ctx.gateway.connect()
    const ws = MockWebSocket.latest!

    ws.simulateMessage(HELLO_MSG)
    ws.simulateMessage(READY_MSG)
    await connectPromise

    // close event は正常に届く経路
    expect(ctx.harness.fireTimer((timer) => timer.kind === "timeout" && timer.ms === 22500)).toBe(
      true,
    )
    expect(ctx.harness.fireTimer((timer) => timer.kind === "interval" && timer.ms === 45000)).toBe(
      true,
    )

    const disconnectedCalls = ctx.onStatus.mock.calls.filter((call) => call[0] === "disconnected")
    expect(disconnectedCalls.length).toBe(1)

    // fallback timer は close 到着時に解除済みで、後から発火しても二重通知しない
    const fallbackFired = ctx.harness.fireTimer(
      (timer) => timer.kind === "timeout" && timer.ms === 5000,
    )
    expect(fallbackFired).toBe(false)

    // zombie 後は resume 可能な session を保持している (close code 4000)
    expect(ctx.gateway.session.canResume()).toBe(true)
  })
})

describe("FlumeDiscordGateway listener registration failure", () => {
  class FailingListenerWebSocket {
    static latest: FailingListenerWebSocket | null = null

    readonly closeCalls: Array<number | undefined> = []

    readyState = 1

    constructor(_url: string | URL) {
      FailingListenerWebSocket.latest = this
    }

    addEventListener(): void {
      throw new Error("listener registration boom")
    }

    send(_data: string): void {}

    close(code?: number, _reason?: string): void {
      this.closeCalls.push(code)
    }
  }

  it("closes the socket when addEventListener throws", async () => {
    const harness = createTimerDeps()
    const deps = {
      ...harness.deps,
      WebSocket: FailingListenerWebSocket as unknown as Deps["WebSocket"],
    }

    FailingListenerWebSocket.latest = null

    const gateway = new FlumeDiscordGateway({
      token: "test-token",
      intents: 513,
      onDispatch: vi.fn(),
      onStatus: vi.fn(),
      deps,
    })

    const connectResult = await gateway.connect()

    expect(connectResult).toBeInstanceOf(FlumeConnectionError)
    expect(FailingListenerWebSocket.latest!.closeCalls.length).toBe(1)
  })
})
