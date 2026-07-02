import { describe, it, expect, vi } from "vitest"
import { waitFor } from "@/test-utils/wait-for"
import { FlumeSlackSource } from "@/slack/slack-source"
import { flumeExtractSlackMeta } from "@/slack/extract-slack-meta"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeLogger } from "@/logger"
import type {
  FlumeEvent,
  FlumeLog,
  FlumeLogHandler,
  FlumeRuntimeDeps,
  FlumeSourceStartContext,
  FlumeStatus,
} from "@/types"

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

type RecordedTimeout = { fn: () => void; ms: number; cleared: boolean }

// deps.setTimeout は自動発火しない記録式にする。handshake timer / reconnect backoff が
// 勝手に発火するとテストの制御が効かないため、必要なテストだけ entry.fn() で明示的に発火する
const createDeps = (): { deps: FlumeRuntimeDeps; timeouts: Array<RecordedTimeout> } => {
  const timeouts: Array<RecordedTimeout> = []

  const deps: FlumeRuntimeDeps = {
    WebSocket: TrackingMockWebSocket as unknown as new (url: string | URL) => WebSocket,
    fetch: createMockFetch(),
    now: () => 1000,
    random: () => 0.5,
    setTimeout: (fn: () => void, ms: number) => {
      const entry: RecordedTimeout = { fn, ms, cleared: false }
      timeouts.push(entry)
      return entry
    },
    clearTimeout: (id) => {
      for (const entry of timeouts) {
        if (entry === id) entry.cleared = true
      }
    },
    setInterval: vi.fn((fn: () => void, ms: number) => {
      return globalThis.setInterval(fn, ms)
    }),
    clearInterval: vi.fn((id) => {
      globalThis.clearInterval(id)
    }),
  }

  return { deps, timeouts }
}

type CtxProps = {
  deps: FlumeRuntimeDeps
  onEvent?: (event: FlumeEvent) => void
  onStatus?: (status: FlumeStatus, detail?: string) => void
  onLog?: FlumeLogHandler
  reconnect?: FlumeSourceStartContext["reconnect"]
}

const createCtx = (props: CtxProps): FlumeSourceStartContext => ({
  onEvent: props.onEvent ?? (() => {}),
  log: new FlumeLogger({ source: "slack", deps: props.deps, handler: props.onLog }),
  deps: props.deps,
  onStatus: props.onStatus ?? (() => {}),
  reconnect: props.reconnect ?? null,
})

describe("FlumeSlackSource", () => {
  it("start() connects and forwards events to onEvent", async () => {
    TrackingMockWebSocket.latest = null
    const receivedEvents: Array<FlumeEvent> = []
    const bundle = createDeps()

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })

    const startPromise = source.start(
      createCtx({ deps: bundle.deps, onEvent: (event) => receivedEvents.push(event) }),
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
    const bundle = createDeps()

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })
    const startPromise = source.start(
      createCtx({ deps: bundle.deps, onStatus: (s) => statuses.push(s) }),
    )

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
    const bundle = createDeps()
    bundle.deps.now = () => nowMs
    bundle.deps.setInterval = ((fn: () => void) => {
      intervalCallbacks.push(fn)
      return intervalCallbacks.length
    }) as unknown as typeof bundle.deps.setInterval
    bundle.deps.clearInterval = vi.fn()

    const source = new FlumeSlackSource({
      appToken: "xapp-test",
      botToken: "xoxb-test",
      idleTimeoutMs: 1_000,
    })
    const startPromise = source.start(
      createCtx({
        deps: bundle.deps,
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

  it("treats invalid_auth as terminal: start() fails and no reconnect is scheduled", async () => {
    TrackingMockWebSocket.latest = null
    const statuses: Array<FlumeStatus> = []
    const logs: Array<FlumeLog> = []
    const bundle = createDeps()
    bundle.deps.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: () => Promise.resolve(JSON.stringify({ ok: false, error: "invalid_auth" })),
    })

    const source = new FlumeSlackSource({ appToken: "xapp-bad", botToken: "xoxb-test" })
    const result = await source.start(
      createCtx({
        deps: bundle.deps,
        onStatus: (status) => statuses.push(status),
        onLog: (log) => logs.push(log),
        reconnect: { maxAttempts: 5, baseDelay: 10, maxDelay: 100 },
      }),
    )

    expect(result).toBeInstanceOf(FlumeHttpError)
    if (result instanceof FlumeHttpError) {
      expect(result.code).toBe("invalid_auth")
    }
    expect(statuses).toContain("disconnected")
    expect(statuses).not.toContain("reconnecting")
    expect(logs.some((log) => log.action === "reconnect.terminal")).toBe(true)
    expect(logs.some((log) => log.action === "reconnect.scheduled")).toBe(false)
    expect(bundle.timeouts).toHaveLength(0)
  })

  it("respects 429 Retry-After as the reconnect backoff floor", async () => {
    TrackingMockWebSocket.latest = null
    const logs: Array<FlumeLog> = []
    const bundle = createDeps()
    bundle.deps.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 429,
      headers: {
        get: (name: string) => (name.toLowerCase() === "retry-after" ? "7" : null),
      },
      text: () => Promise.resolve(JSON.stringify({ ok: false, error: "ratelimited" })),
    })

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })
    const result = await source.start(
      createCtx({
        deps: bundle.deps,
        onLog: (log) => logs.push(log),
        reconnect: { maxAttempts: 3, baseDelay: 10, maxDelay: 50 },
      }),
    )

    expect(result).toBeNull()

    const scheduled = logs.find((log) => log.action === "reconnect.scheduled")
    expect(scheduled).toBeDefined()
    expect(Number(scheduled!.detail!.delayMs)).toBeGreaterThanOrEqual(7_000)

    const pendingTimer = bundle.timeouts.find((entry) => !entry.cleared)
    expect(pendingTimer).toBeDefined()
    expect(pendingTimer!.ms).toBeGreaterThanOrEqual(7_000)
  })

  it("dedups Events API redeliveries by payload.event_id across envelope_ids", async () => {
    TrackingMockWebSocket.latest = null
    const receivedEvents: Array<FlumeEvent> = []
    const bundle = createDeps()

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })
    const startPromise = source.start(
      createCtx({ deps: bundle.deps, onEvent: (event) => receivedEvents.push(event) }),
    )

    await waitFor(() => {
      expect(TrackingMockWebSocket.latest).not.toBeNull()
    })

    const ws = TrackingMockWebSocket.latest!
    ws.simulateMessage(JSON.stringify({ type: "hello" }))
    await startPromise

    ws.simulateMessage(
      JSON.stringify({
        envelope_id: "env-1",
        type: "events_api",
        payload: { event_id: "Ev123", event: { type: "message" } },
      }),
    )
    // Slack の再配送: envelope_id は変わるが event_id は同一
    ws.simulateMessage(
      JSON.stringify({
        envelope_id: "env-2",
        type: "events_api",
        payload: { event_id: "Ev123", event: { type: "message" } },
        retry_attempt: 1,
      }),
    )
    ws.simulateMessage(
      JSON.stringify({
        envelope_id: "env-3",
        type: "events_api",
        payload: { event_id: "Ev456", event: { type: "message" } },
      }),
    )

    await waitFor(() => {
      expect(receivedEvents.length).toBe(2)
    })

    expect(receivedEvents.length).toBe(2)
  })

  it("falls back to envelope_id dedup when payload has no event_id", async () => {
    TrackingMockWebSocket.latest = null
    const receivedEvents: Array<FlumeEvent> = []
    const bundle = createDeps()

    const source = new FlumeSlackSource({ appToken: "xapp-test", botToken: "xoxb-test" })
    const startPromise = source.start(
      createCtx({ deps: bundle.deps, onEvent: (event) => receivedEvents.push(event) }),
    )

    await waitFor(() => {
      expect(TrackingMockWebSocket.latest).not.toBeNull()
    })

    const ws = TrackingMockWebSocket.latest!
    ws.simulateMessage(JSON.stringify({ type: "hello" }))
    await startPromise

    const envelope = { envelope_id: "env-9", type: "slash_commands", payload: {} }
    ws.simulateMessage(JSON.stringify(envelope))
    ws.simulateMessage(JSON.stringify(envelope))
    ws.simulateMessage(
      JSON.stringify({ envelope_id: "env-10", type: "slash_commands", payload: {} }),
    )

    await waitFor(() => {
      expect(receivedEvents.length).toBe(2)
    })

    expect(receivedEvents.length).toBe(2)
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
