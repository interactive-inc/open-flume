import { describe, it, expect, vi } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import type { FlumeEvent, FlumeRuntimeDeps, FlumeSourceStartContext } from "@/types"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { flumeExtractDiscordMeta } from "@/discord/extract-discord-meta"
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

type CtxProps = {
  deps: FlumeRuntimeDeps
  onEvent?: (event: FlumeEvent) => void
  onStatus?: (status: string, detail?: string) => void
}

const createCtx = (props: CtxProps): FlumeSourceStartContext => ({
  onEvent: props.onEvent ?? (() => {}),
  log: new FlumeLogger({ source: "discord", deps: props.deps }),
  deps: props.deps,
  onStatus: props.onStatus ?? (() => {}),
  reconnect: null,
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
