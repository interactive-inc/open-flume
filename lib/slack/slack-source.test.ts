import { describe, it, expect, vi } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import { FlumeSlackSource } from "@/slack/slack-source"
import { flumeExtractSlackMeta } from "@/slack/extract-slack-meta"
import { FlumeLogger } from "@/logger"
import type { FlumeEvent, FlumeRuntimeDeps, FlumeSourceStartContext, FlumeStatus } from "@/types"

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

class TrackingMockWebSocket extends MockWebSocket {
  static latest: TrackingMockWebSocket | null = null

  constructor(url: string | URL) {
    super(url)
    TrackingMockWebSocket.latest = this
  }
}

const createMockFetch = () => {
  return vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    text: () => Promise.resolve(JSON.stringify({ ok: true, url: "wss://slack.example.com/ws" })),
  })
}

const createDeps = (): FlumeRuntimeDeps => {
  return {
    WebSocket: TrackingMockWebSocket as unknown as new (url: string | URL) => WebSocket,
    fetch: createMockFetch(),
    now: () => 1000,
    random: () => 0.5,
    setTimeout: vi.fn((fn: () => void, _ms: number) => {
      return globalThis.setTimeout(fn, 0)
    }),
    clearTimeout: vi.fn((id) => {
      globalThis.clearTimeout(id)
    }),
    setInterval: vi.fn((fn: () => void, ms: number) => {
      return globalThis.setInterval(fn, ms)
    }),
    clearInterval: vi.fn((id) => {
      globalThis.clearInterval(id)
    }),
  }
}

type CtxProps = {
  deps: FlumeRuntimeDeps
  onEvent?: (event: FlumeEvent) => void
  onStatus?: (status: FlumeStatus, detail?: string) => void
  reconnect?: FlumeSourceStartContext["reconnect"]
}

const createCtx = (props: CtxProps): FlumeSourceStartContext => ({
  onEvent: props.onEvent ?? (() => {}),
  log: new FlumeLogger({ source: "slack", deps: props.deps }),
  deps: props.deps,
  onStatus: props.onStatus ?? (() => {}),
  reconnect: props.reconnect ?? null,
})

describe("FlumeSlackSource", () => {
  it("start() connects and forwards events to onEvent", async () => {
    TrackingMockWebSocket.latest = null
    const receivedEvents: Array<FlumeEvent> = []
    const deps = createDeps()

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })

    const startPromise = source.start(
      createCtx({ deps, onEvent: (event) => receivedEvents.push(event) }),
    )

    await waitFor(() => {
      expect(TrackingMockWebSocket.latest).not.toBeNull()
    })

    TrackingMockWebSocket.latest!.simulateMessage(JSON.stringify({ type: "hello" }))

    await startPromise

    const envelope = {
      envelope_id: "e1",
      type: "events_api",
      payload: { event: { type: "message" } },
    }

    TrackingMockWebSocket.latest!.simulateMessage(JSON.stringify(envelope))

    await waitFor(() => {
      expect(receivedEvents.length).toBe(1)
    })

    expect(receivedEvents.length).toBe(1)
    expect(receivedEvents[0]!.source).toBe("slack")
    expect(receivedEvents[0]!.type).toBe("events_api")
  })

  it("stop() disconnects and sets status to disconnected", async () => {
    TrackingMockWebSocket.latest = null
    const statuses: Array<FlumeStatus> = []
    const deps = createDeps()

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })
    const startPromise = source.start(createCtx({ deps, onStatus: (s) => statuses.push(s) }))

    await waitFor(() => {
      expect(TrackingMockWebSocket.latest).not.toBeNull()
    })

    TrackingMockWebSocket.latest!.simulateMessage(JSON.stringify({ type: "hello" }))

    await startPromise

    await source.stop()

    expect(source.status()).toBe("disconnected")
    expect(statuses).toContain("disconnected")
  })

  it("status() returns current status", () => {
    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })

    expect(source.status()).toBe("disconnected")
  })

  it("passes idleTimeoutMs through and reconnects after socket silence", async () => {
    TrackingMockWebSocket.latest = null
    let nowMs = 1_000_000
    const intervalCallbacks: Array<() => void> = []
    const statuses: Array<FlumeStatus> = []
    const deps = createDeps()
    deps.now = () => nowMs
    deps.setInterval = ((fn: () => void) => {
      intervalCallbacks.push(fn)
      return intervalCallbacks.length
    }) as unknown as typeof deps.setInterval
    deps.clearInterval = vi.fn()

    const source = new FlumeSlackSource({
      appToken: "xapp-test",
      botToken: "xoxb-test",
      idleTimeoutMs: 1_000,
    })
    const startPromise = source.start(
      createCtx({
        deps,
        onStatus: (status) => statuses.push(status),
        reconnect: { maxAttempts: 1, baseDelay: 1_000, maxDelay: 1_000 },
      }),
    )

    await waitFor(() => {
      expect(TrackingMockWebSocket.latest).not.toBeNull()
    })

    TrackingMockWebSocket.latest!.simulateMessage(JSON.stringify({ type: "hello" }))
    await startPromise
    expect(intervalCallbacks).toHaveLength(1)

    nowMs += 5_000
    intervalCallbacks[0]!()

    expect(statuses).toContain("reconnecting")
  })
})

describe("flumeExtractSlackMeta", () => {
  it("extracts event_type from envelope", () => {
    const meta = flumeExtractSlackMeta({
      envelope_id: "e1",
      type: "events_api",
      payload: {},
    })
    expect(meta.event_type).toBe("events_api")
  })

  it("extracts channel, user, thread_ts from payload.event", () => {
    const meta = flumeExtractSlackMeta({
      envelope_id: "e1",
      type: "events_api",
      payload: {
        event: { type: "message", channel: "C123", user: "U456", thread_ts: "1234.5678" },
      },
    })
    expect(meta.channel_id).toBe("C123")
    expect(meta.user_id).toBe("U456")
    expect(meta.thread_ts).toBe("1234.5678")
    expect(meta.slack_event_type).toBe("message")
  })

  it("handles missing event payload", () => {
    const meta = flumeExtractSlackMeta({
      envelope_id: "e1",
      type: "slash_commands",
      payload: {},
    })
    expect(meta.event_type).toBe("slash_commands")
    expect(meta.channel_id).toBeUndefined()
  })
})
