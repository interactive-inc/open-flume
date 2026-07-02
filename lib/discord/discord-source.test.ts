import { describe, it, expect, vi } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import type {
  FlumeEvent,
  FlumeReconnectConfig,
  FlumeRuntimeDeps,
  FlumeSourceStartContext,
} from "@/types"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { flumeExtractDiscordMeta } from "@/discord/extract-discord-meta"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeLogger } from "@/logger"

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
}

const HELLO_MSG = '{"op":10,"d":{"heartbeat_interval":45000},"s":null,"t":null}'
const READY_MSG =
  '{"op":0,"d":{"session_id":"abc","resume_gateway_url":"wss://resume.example.com"},"s":1,"t":"READY"}'

const createMockDeps = (): FlumeRuntimeDeps => {
  const timerHandle = globalThis.setTimeout(() => {}, 0)
  globalThis.clearTimeout(timerHandle)

  return {
    WebSocket: MockWebSocket as unknown as FlumeRuntimeDeps["WebSocket"],
    setInterval: vi.fn((_fn: () => void, _ms: number) => timerHandle),
    clearInterval: vi.fn(),
    setTimeout: vi.fn((_fn: () => void, _ms: number) => timerHandle),
    clearTimeout: vi.fn(),
    random: () => 0.5,
    now: () => 1000,
    fetch: vi.fn(),
  }
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

  const deps: FlumeRuntimeDeps = {
    WebSocket: MockWebSocket as unknown as FlumeRuntimeDeps["WebSocket"],
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
    fetch: vi.fn(),
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

type CtxProps = {
  deps: FlumeRuntimeDeps
  onEvent?: (event: FlumeEvent) => void
  onStatus?: (status: string, detail?: string) => void
  reconnect?: FlumeReconnectConfig | null
}

const createCtx = (props: CtxProps): FlumeSourceStartContext => ({
  onEvent: props.onEvent ?? (() => {}),
  log: new FlumeLogger({ source: "discord", deps: props.deps }),
  deps: props.deps,
  onStatus: props.onStatus ?? (() => {}),
  reconnect: props.reconnect ?? null,
})

const simulateReadySequence = () => {
  const ws = MockWebSocket.latest!

  ws.simulateMessage(HELLO_MSG)
  ws.simulateMessage(READY_MSG)
}

describe("FlumeDiscordSource", () => {
  it("start() creates gateway and connects", async () => {
    const onStatus = vi.fn()
    const deps = createMockDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })
    const startPromise = source.start(createCtx({ deps, onStatus }))

    simulateReadySequence()

    await startPromise

    expect(MockWebSocket.latest).not.toBeNull()
    expect(onStatus).toHaveBeenCalledWith("connected")
  })

  it("stop() disconnects and sets status to disconnected", async () => {
    const onStatus = vi.fn()
    const deps = createMockDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })
    const startPromise = source.start(createCtx({ deps, onStatus }))

    simulateReadySequence()

    await startPromise

    await source.stop()

    expect(onStatus).toHaveBeenCalledWith("disconnected")
  })

  it("dispatched events are forwarded to onEvent", async () => {
    const deps = createMockDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })

    const receivedEvents: Array<FlumeEvent> = []
    const onEvent = (event: FlumeEvent) => {
      receivedEvents.push(event)
    }

    const startPromise = source.start(createCtx({ deps, onEvent }))

    simulateReadySequence()

    await startPromise

    const messageCreate =
      '{"op":0,"d":{"content":"hello","channel_id":"123"},"s":2,"t":"MESSAGE_CREATE"}'

    MockWebSocket.latest!.simulateMessage(messageCreate)

    await waitFor(() => {
      expect(receivedEvents.filter((ev) => ev.type !== "READY").length).toBe(1)
    })

    const nonReadyEvents = receivedEvents.filter((ev) => ev.type !== "READY")

    expect(nonReadyEvents.length).toBe(1)
    expect(nonReadyEvents[0]!.source).toBe("discord")
    expect(nonReadyEvents[0]!.type).toBe("MESSAGE_CREATE")
  })

  it("status() returns current status", () => {
    const source = new FlumeDiscordSource({ token: "test-token" })

    expect(source.status()).toBe("disconnected")
  })

  it("second start() returns FlumeStartError (consumed guard)", async () => {
    const deps = createMockDeps()
    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })
    const startPromise = source.start(createCtx({ deps }))
    simulateReadySequence()
    await startPromise

    const second = await source.start(createCtx({ deps }))

    expect(second).toBeInstanceOf(Error)
  })
})

describe("FlumeDiscordSource reconnect", () => {
  const RECONNECT: FlumeReconnectConfig = { maxAttempts: 3, baseDelay: 100, maxDelay: 1000 }

  it("threads the session across reconnect attempts and sends RESUME", async () => {
    const harness = createTimerDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })
    const startPromise = source.start(createCtx({ deps: harness.deps, reconnect: RECONNECT }))

    simulateReadySequence()
    await startPromise

    const firstSocket = MockWebSocket.latest!

    firstSocket.simulateClose(1006, "network drop")

    // resume 可能 → 通常 backoff (100 * (0.5 + 0.5 * 0.5) = 75ms)。identify 下限 5000ms は適用されない
    expect(harness.fireTimer((timer) => timer.kind === "timeout" && timer.ms === 75)).toBe(true)

    const secondSocket = MockWebSocket.latest!

    expect(secondSocket).not.toBe(firstSocket)
    expect(secondSocket.url).toBe("wss://resume.example.com/?v=10&encoding=json")

    secondSocket.simulateMessage(HELLO_MSG)

    const frames = secondSocket.sentMessages.map((raw) => JSON.parse(raw))
    const resumes = frames.filter((frame) => frame.op === 6)

    expect(resumes.length).toBe(1)
    expect(resumes[0]!.d.session_id).toBe("abc")
    expect(resumes[0]!.d.seq).toBe(1)
    expect(frames.filter((frame) => frame.op === 2).length).toBe(0)

    await source.stop()
  })

  it("applies the 5s identify floor when the session was invalidated by close 4009", async () => {
    const harness = createTimerDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })
    const startPromise = source.start(createCtx({ deps: harness.deps, reconnect: RECONNECT }))

    simulateReadySequence()
    await startPromise

    MockWebSocket.latest!.simulateClose(4009, "session timed out")

    // session 破棄 → IDENTIFY し直し → backoff 75ms が 5000ms へ底上げされる
    expect(harness.fireTimer((timer) => timer.kind === "timeout" && timer.ms === 5000)).toBe(true)

    const secondSocket = MockWebSocket.latest!

    expect(secondSocket.url).toBe("wss://gateway.discord.gg/?v=10&encoding=json")

    secondSocket.simulateMessage(HELLO_MSG)

    const frames = secondSocket.sentMessages.map((raw) => JSON.parse(raw))

    expect(frames.filter((frame) => frame.op === 2).length).toBe(1)
    expect(frames.filter((frame) => frame.op === 6).length).toBe(0)

    await source.stop()
  })

  it("stop() during a pending connect resolves start() with a connection error", async () => {
    const deps = createMockDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token" })
    const startPromise = source.start(createCtx({ deps }))

    // READY 前に stop: this.gateway が null 化されても local 参照で TypeError を出さず終了する
    await source.stop()

    const startResult = await startPromise

    expect(startResult).toBeInstanceOf(FlumeConnectionError)
  })

  it("threads handshakeTimeoutMs into the gateway handshake timer", async () => {
    const harness = createTimerDeps()

    MockWebSocket.latest = null

    const source = new FlumeDiscordSource({ token: "test-token", handshakeTimeoutMs: 1234 })
    const startPromise = source.start(createCtx({ deps: harness.deps }))

    const armed = harness.timers.some((timer) => timer.kind === "timeout" && timer.ms === 1234)
    expect(armed).toBe(true)

    simulateReadySequence()
    await startPromise
    await source.stop()
  })
})

describe("flumeExtractDiscordMeta", () => {
  it("extracts event_type", () => {
    const meta = flumeExtractDiscordMeta("MESSAGE_CREATE", {})
    expect(meta.event_type).toBe("MESSAGE_CREATE")
  })

  it("extracts channel_id and guild_id", () => {
    const meta = flumeExtractDiscordMeta("MESSAGE_CREATE", {
      channel_id: "ch-1",
      guild_id: "g-1",
    })
    expect(meta.channel_id).toBe("ch-1")
    expect(meta.guild_id).toBe("g-1")
  })

  it("extracts user_id from author", () => {
    const meta = flumeExtractDiscordMeta("MESSAGE_CREATE", {
      author: { id: "u-1" },
    })
    expect(meta.user_id).toBe("u-1")
  })

  it("ignores non-string fields", () => {
    const meta = flumeExtractDiscordMeta("MESSAGE_CREATE", {
      channel_id: 123,
      author: "not-an-object",
    })
    expect(meta.channel_id).toBeUndefined()
    expect(meta.user_id).toBeUndefined()
  })
})
