import { describe, it, expect, vi } from "vitest"
import { createServer } from "node:http"
import { once } from "node:events"
import { execFile } from "node:child_process"
import { promisify } from "node:util"
import type { AddressInfo } from "node:net"
import { WebSocketServer } from "ws"

async function client(
  source: string,
  endpoint: string,
  mode: string,
): Promise<{
  bun: string
  failed: boolean
  received: number[]
  pendingTimers: number
  delivery: { droppedEventCallbacks: number }
  codes: string[]
}> {
  const { stdout } = await promisify(execFile)(
    "bun",
    ["lib/test-utils/runtime-wire-client.ts", source, endpoint, mode],
    { timeout: 10_000 },
  )
  return JSON.parse(stdout)
}

describe("Bun runtime against Node wire servers", () => {
  it.each([
    ["slack", "body", 200],
    ["github", "body", 200],
    ["slack", "rate-limit", 429],
    ["github", "rate-limit", 429],
  ] as const)(
    "releases the %s HTTP connection after %s",
    async (source, mode, status) => {
      let closed = false
      const server = createServer((_request, response) => {
        response.writeHead(status, { "Retry-After": "60" })
        response.write("{")
        response.on("close", () => {
          closed = true
        })
      })
      server.listen(0, "127.0.0.1")
      await once(server, "listening")
      try {
        const result = await client(
          source,
          `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
          mode,
        )
        expect(result.bun).toBeTruthy()
        expect(result.codes).toContain(mode === "body" ? "FLUME_TIMEOUT" : "FLUME_RATE_LIMITED")
        expect(result.pendingTimers).toBe(0)
        await vi.waitFor(() => expect(closed).toBe(true))
      } finally {
        server.closeAllConnections()
        await new Promise<void>((resolve) => server.close(() => resolve()))
      }
    },
    15_000,
  )

  it.each(["slack", "discord"])(
    "reconnects %s with native Bun WebSocket and preserves event order",
    async (source) => {
      const wire = new WebSocketServer({ host: "127.0.0.1", port: 0 })
      let connections = 0
      let sequence = 0
      wire.on("connection", (socket) => {
        const cycle = connections++
        let acknowledgements = 0
        let batchEnd = -1
        const sendBatch = () => {
          for (let index = 0; index < 100; index++) {
            const order = cycle * 100 + index
            socket.send(
              JSON.stringify(
                source === "slack"
                  ? {
                      type: "events_api",
                      envelope_id: `test-${order}`,
                      payload: { event_id: `test-${order}`, order },
                    }
                  : { op: 0, t: "MESSAGE_CREATE", s: sequence++, d: { order } },
              ),
            )
          }
          batchEnd = sequence - 1
        }
        socket.on("message", (frame) => {
          const message = JSON.parse(frame.toString())
          if (source === "slack") {
            if (++acknowledgements === 100 && cycle < 2)
              socket.send('{"type":"disconnect","reason":"refresh_requested"}')
          } else if (message.op === 1) {
            if (cycle < 2 && batchEnd >= 0 && message.d === batchEnd) socket.terminate()
            else socket.send('{"op":11}')
          } else if (message.op === 2 || message.op === 6) {
            socket.send(
              JSON.stringify({
                op: 0,
                t: message.op === 2 ? "READY" : "RESUMED",
                s: sequence++,
                d: { session_id: "test-session", resume_gateway_url: "wss://gateway.example.com" },
              }),
            )
            sendBatch()
          }
        })
        socket.send(
          source === "slack" ? '{"type":"hello"}' : '{"op":10,"d":{"heartbeat_interval":100}}',
        )
        if (source === "slack") sendBatch()
      })
      await once(wire, "listening")
      const socketUrl = `ws://127.0.0.1:${(wire.address() as AddressInfo).port}`
      const api = createServer((_request, response) =>
        response.end(JSON.stringify({ ok: true, url: socketUrl })),
      )
      api.listen(0, "127.0.0.1")
      await once(api, "listening")
      try {
        const result = await client(
          source,
          source === "slack"
            ? `http://127.0.0.1:${(api.address() as AddressInfo).port}`
            : socketUrl,
          "socket",
        )
        expect(result.bun).toBeTruthy()
        expect(result.failed).toBe(false)
        expect(result.received).toEqual(Array.from({ length: 300 }, (_, index) => index))
        expect(result.delivery.droppedEventCallbacks).toBe(0)
        expect(result.pendingTimers).toBe(0)
        expect(connections).toBe(3)
        await vi.waitFor(() => expect(wire.clients.size).toBe(0))
      } finally {
        for (const socket of wire.clients) socket.terminate()
        await new Promise<void>((resolve) => wire.close(() => resolve()))
        api.closeAllConnections()
        await new Promise<void>((resolve) => api.close(() => resolve()))
      }
    },
    15_000,
  )
})
