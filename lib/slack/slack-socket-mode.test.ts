import { describe, it, expect, vi } from "vitest"
import { FlumeSlackSocketMode } from "@/slack/slack-socket-mode"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeHttpError } from "@/errors/http-error"

type Listener = (ev: unknown) => void

class MockWebSocket {

  readonly url: string

  readyState = 1

  private listeners: Map<string, Array<Listener>> = new Map()

  sent: Array<string> = []

  constructor(url: string | URL) {
    this.url = String(url)
  }

  addEventListener(type: string, fn: Listener): void {
    const existing = this.listeners.get(type) ?? []
    existing.push(fn)
    this.listeners.set(type, existing)
  }

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {
    this.simulateClose(1000, "")
  }

  simulateMessage(data: string): void {
    const fns = this.listeners.get("message") ?? []
    for (const fn of fns) {
      fn({ data })
    }
  }

  simulateClose(code: number, reason: string): void {
    const fns = this.listeners.get("close") ?? []
    for (const fn of fns) {
      fn({ code, reason })
    }
  }

  simulateError(): void {
    const fns = this.listeners.get("error") ?? []
    for (const fn of fns) {
      fn({})
    }
  }
}

const createMockFetch = () => {
  return vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    json: () => Promise.resolve({ ok: true, url: "wss://slack.example.com/ws" }),
  })
}

const createDeps = (overrides?: { fetch?: (url: string | URL, init?: RequestInit) => Promise<Response> }) => {
  const instances: MockWebSocket[] = []

  const WS = class extends MockWebSocket {
    constructor(url: string | URL) {
      super(url)
      instances.push(this)
    }
  }

  const deps = {
    WebSocket: WS as unknown as new (url: string | URL) => WebSocket,
    fetch: overrides?.fetch ?? createMockFetch(),
    now: () => 1000,
  }

  const getSocket = () => instances[instances.length - 1] ?? null

  return { deps, getSocket }
}

describe("FlumeSlackSocketMode", () => {

  it("connect() obtains URL and opens WebSocket", async () => {
    const mockFetch = createMockFetch()
    const { deps, getSocket } = createDeps({ fetch: mockFetch })
    const onMessage = vi.fn()
    const onConnected = vi.fn()
    const onDisconnected = vi.fn()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage,
      onConnected,
      onDisconnected,
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    expect(mockFetch).toHaveBeenCalledWith(
      "https://slack.com/api/apps.connections.open",
      expect.objectContaining({ method: "POST" }),
    )

    expect(getSocket()!.url).toBe("wss://slack.example.com/ws")

    getSocket()!.simulateMessage(JSON.stringify({ type: "hello" }))

    const result = await connectPromise

    expect(result).toBeNull()
  })

  it("hello message resolves connect with null and calls onConnected", async () => {
    const { deps, getSocket } = createDeps()
    const onConnected = vi.fn()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage: vi.fn(),
      onConnected,
      onDisconnected: vi.fn(),
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    getSocket()!.simulateMessage(JSON.stringify({ type: "hello" }))

    const result = await connectPromise

    expect(result).toBeNull()
    expect(onConnected).toHaveBeenCalledOnce()
  })

  it("envelope messages are acknowledged and forwarded", async () => {
    const { deps, getSocket } = createDeps()
    const onMessage = vi.fn()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage,
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    const ws = getSocket()!

    ws.simulateMessage(JSON.stringify({ type: "hello" }))
    await connectPromise

    const envelope = {
      envelope_id: "e1",
      type: "events_api",
      payload: { event: { type: "message" } },
    }

    ws.simulateMessage(JSON.stringify(envelope))

    expect(ws.sent).toContainEqual(JSON.stringify({ envelope_id: "e1" }))
    expect(onMessage).toHaveBeenCalledOnce()

    const received = onMessage.mock.calls[0]![0]!

    expect(received.envelope_id).toBe("e1")
    expect(received.type).toBe("events_api")
  })

  it("disconnect type message closes socket", async () => {
    const { deps, getSocket } = createDeps()
    const onDisconnected = vi.fn()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected,
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    const ws = getSocket()!

    ws.simulateMessage(JSON.stringify({ type: "hello" }))
    await connectPromise

    ws.simulateMessage(JSON.stringify({ type: "disconnect", reason: "refresh" }))

    expect(onDisconnected).toHaveBeenCalled()
  })

  it("invalid JSON logs error and continues", async () => {
    const { deps, getSocket } = createDeps()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    const ws = getSocket()!

    ws.simulateMessage("not-json{{{{")
    ws.simulateMessage(JSON.stringify({ type: "hello" }))

    const result = await connectPromise

    expect(result).toBeNull()
  })

  it("WebSocket close before hello resolves with FlumeConnectionError", async () => {
    const { deps, getSocket } = createDeps()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    getSocket()!.simulateClose(1006, "abnormal")

    const result = await connectPromise

    expect(result).toBeInstanceOf(FlumeConnectionError)
  })

  it("disconnect() sets stopped and closes WebSocket", async () => {
    const { deps, getSocket } = createDeps()

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps,
    })

    const connectPromise = mode.connect()

    await vi.waitFor(() => {
      expect(getSocket()).not.toBeNull()
    })

    getSocket()!.simulateMessage(JSON.stringify({ type: "hello" }))
    await connectPromise

    mode.disconnect()

    expect(mode.stopped).toBe(true)
  })

  it("HTTP error from obtainSlackUrl returns FlumeHttpError", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ ok: false, error: "invalid_auth" }),
    })

    const { deps } = createDeps({ fetch: mockFetch })

    const mode = new FlumeSlackSocketMode({
      appToken: "xapp-test",
      onMessage: vi.fn(),
      onConnected: vi.fn(),
      onDisconnected: vi.fn(),
      deps,
    })

    const result = await mode.connect()

    expect(result).toBeInstanceOf(FlumeHttpError)
  })
})
