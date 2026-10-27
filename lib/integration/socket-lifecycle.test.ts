import { describe, it, expect, vi } from "vitest"
import { WebSocketServer, type WebSocket as ServerSocket } from "ws"
import { createServer } from "node:http"
import { once } from "node:events"
import type { AddressInfo } from "node:net"
import { Flume } from "@/flume"
import { FlumeRunning } from "@/flume-running"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { FlumeSlackSource } from "@/slack/slack-source"
import { trackedRuntime } from "@/test-utils/tracked-runtime"

const cycles = Number(process.env.FLUME_STRESS_CYCLES ?? 10)
const batch = Number(process.env.FLUME_STRESS_EVENTS ?? 100)

async function websocketServer(onConnection: (socket: ServerSocket) => void) {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 })
  server.on("connection", onConnection)
  await once(server, "listening")
  const endpoint = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`
  class LocalWebSocket extends globalThis.WebSocket {
    constructor(_url: string | URL) {
      super(endpoint)
    }
  }
  return {
    server,
    endpoint,
    WebSocket: LocalWebSocket,
    dispose: () =>
      new Promise<void>((resolve) => {
        for (const socket of server.clients) socket.terminate()
        server.close(() => resolve())
      }),
  }
}

describe("real socket lifecycle", () => {
  it.each(["timeout", "abort"])(
    "releases a real socket stuck before hello on %s",
    async (reason) => {
      const runtime = trackedRuntime()
      const controller = new AbortController()
      const connected = Promise.withResolvers<void>()
      const wire = await websocketServer(() => connected.resolve())
      runtime.deps.fetch = async () =>
        new Response(JSON.stringify({ ok: true, url: wire.endpoint }))
      try {
        const flume = new Flume({
          deps: runtime.deps,
          signal: controller.signal,
          sources: [
            new FlumeSlackSource({
              appToken: "test-token",
              botToken: "test-token",
              handshakeTimeoutMs: 100,
            }),
          ],
        })
        const opening = flume.open()
        await connected.promise
        if (reason === "abort") controller.abort()
        expect(await opening).toBeInstanceOf(Error)
        expect(flume.diagnostics().entries).toContainEqual(
          expect.objectContaining({
            errorCode: reason === "abort" ? "FLUME_ABORTED" : "FLUME_TIMEOUT",
          }),
        )
        await vi.waitFor(() => expect(wire.server.clients.size).toBe(0))
        expect(runtime.pending()).toBe(0)
      } finally {
        controller.abort()
        runtime.dispose()
        await wire.dispose()
      }
    },
  )

  it("reconnects Discord repeatedly without losing ordered dispatches or leaking timers", async () => {
    const runtime = trackedRuntime()
    let current: ServerSocket | undefined
    let connections = 0
    let sequence = 0
    let resumed = 0
    const wire = await websocketServer((socket) => {
      connections++
      current = socket
      socket.send(JSON.stringify({ op: 10, d: { heartbeat_interval: 100 } }))
      socket.on("message", (frame) => {
        const message = JSON.parse(frame.toString())
        if (message.op === 1) socket.send('{"op":11}')
        if (message.op !== 2 && message.op !== 6) return
        if (message.op === 6) resumed++
        socket.send(
          JSON.stringify({
            op: 0,
            t: message.op === 6 ? "RESUMED" : "READY",
            s: sequence++,
            d: { session_id: "test-session", resume_gateway_url: "wss://gateway-test.discord.gg" },
          }),
        )
      })
    })
    runtime.deps.WebSocket = wire.WebSocket
    const received: number[] = []
    let active: FlumeRunning | undefined
    try {
      const result = await new Flume({
        sources: [new FlumeDiscordSource({ token: "test-token" })],
        deps: runtime.deps,
        callbackBuffer: batch * 4 + 100,
        reconnect: { baseDelay: 1, maxDelay: 2 },
        onEvent: (item) => {
          if (item.kind === "event" && item.event.type === "MESSAGE_CREATE")
            received.push(Number(item.event.data.order))
        },
      }).open()
      expect(result).toBeInstanceOf(FlumeRunning)
      if (result instanceof Error) throw result
      active = result
      for (let cycle = 0; cycle < cycles; cycle++) {
        for (let index = 0; index < batch; index++)
          current!.send(
            JSON.stringify({
              op: 0,
              t: "MESSAGE_CREATE",
              s: sequence++,
              d: { order: cycle * batch + index },
            }),
          )
        await vi.waitFor(() => expect(received).toHaveLength((cycle + 1) * batch), {
          timeout: 10_000,
          interval: 5,
        })
        if (cycle + 1 === cycles) break
        current!.terminate()
        await vi.waitFor(
          () => {
            expect(connections).toBe(cycle + 2)
            expect(active!.statuses()[0]?.status).toBe("connected")
          },
          { timeout: 10_000, interval: 5 },
        )
      }
      const [a, b] = await Promise.all([active.close(), active.close()])
      expect(a).toBe(b)
      await active.drain()
      expect(received).toEqual(Array.from({ length: cycles * batch }, (_, index) => index))
      expect(resumed).toBe(cycles - 1)
      expect(active.deliveryStatus().droppedCallbacks).toBe(0)
      expect(runtime.pending()).toBe(0)
      await vi.waitFor(() => expect(wire.server.clients.size).toBe(0))
    } finally {
      await active?.close()
      runtime.dispose()
      await wire.dispose()
    }
  }, 120_000)

  it("receives and acknowledges Slack envelopes across real HTTP and socket refreshes", async () => {
    const runtime = trackedRuntime()
    let current: ServerSocket | undefined
    let connections = 0
    const acknowledgements = new Set<string>()
    const wire = await websocketServer((socket) => {
      current = socket
      connections++
      socket.send('{"type":"hello"}')
      socket.on("message", (frame) =>
        acknowledgements.add(JSON.parse(frame.toString()).envelope_id),
      )
    })
    const api = createServer((_request, response) => {
      response.setHeader("Content-Type", "application/json")
      response.end(JSON.stringify({ ok: true, url: wire.endpoint }))
    })
    api.listen(0, "127.0.0.1")
    await once(api, "listening")
    const endpoint = `http://127.0.0.1:${(api.address() as AddressInfo).port}`
    runtime.deps.fetch = (_url, init) => fetch(endpoint, init)
    const received: number[] = []
    let active: FlumeRunning | undefined
    try {
      const result = await new Flume({
        sources: [new FlumeSlackSource({ appToken: "test-token", botToken: "test-token" })],
        deps: runtime.deps,
        callbackBuffer: batch * 4 + 100,
        reconnect: { baseDelay: 1, maxDelay: 2 },
        onEvent: (item) => {
          if (item.kind === "event" && item.event.source === "slack")
            received.push(Number(item.event.data.order))
        },
      }).open()
      expect(result).toBeInstanceOf(FlumeRunning)
      if (result instanceof Error) throw result
      active = result
      for (let cycle = 0; cycle < cycles; cycle++) {
        for (let index = 0; index < batch; index++)
          current!.send(
            JSON.stringify({
              type: "events_api",
              envelope_id: `envelope-${cycle}-${index}`,
              payload: { event_id: `event-${cycle}-${index}`, order: cycle * batch + index },
            }),
          )
        await vi.waitFor(
          () => {
            expect(received).toHaveLength((cycle + 1) * batch)
            expect(acknowledgements.size).toBe((cycle + 1) * batch)
          },
          { timeout: 10_000, interval: 5 },
        )
        if (cycle + 1 === cycles) break
        current!.send('{"type":"disconnect","reason":"refresh_requested"}')
        await vi.waitFor(
          () => {
            expect(connections).toBe(cycle + 2)
            expect(active!.statuses()[0]?.status).toBe("connected")
          },
          { timeout: 10_000, interval: 5 },
        )
      }
      await active.close()
      await active.drain()
      expect(received).toEqual(Array.from({ length: cycles * batch }, (_, index) => index))
      expect(active.deliveryStatus().droppedCallbacks).toBe(0)
      expect(runtime.pending()).toBe(0)
      await vi.waitFor(() => expect(wire.server.clients.size).toBe(0))
    } finally {
      await active?.close()
      runtime.dispose()
      await wire.dispose()
      await new Promise<void>((resolve) => api.close(() => resolve()))
    }
  }, 120_000)
})
