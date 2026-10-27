import { describe, it, expect, vi } from "vitest"
import { createServer } from "node:http"
import { once } from "node:events"
import type { AddressInfo } from "node:net"
import { Flume } from "@/flume"
import { FlumeSlackSource } from "@/slack/slack-source"
import { FlumeGitHubSource } from "@/github/github-source"
import { trackedRuntime } from "@/test-utils/tracked-runtime"

// ヘッダだけ返して本文を終えない実 HTTP サーバー。abort が TCP 側にも届くことを確認する。
describe("real HTTP lifecycle", () => {
  it.each(["slack", "github"] as const)(
    "aborts a stalled %s response body and releases timers",
    async (source) => {
      const runtime = trackedRuntime()
      let requestClosed = false
      const server = createServer((_request, response) => {
        response.setHeader("Content-Type", "application/json")
        response.write("{")
        response.on("close", () => {
          requestClosed = true
        })
      })
      server.listen(0, "127.0.0.1")
      await once(server, "listening")
      const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
      runtime.deps.fetch = (_url, init) => fetch(endpoint, init)
      try {
        const flume = new Flume({
          deps: runtime.deps,
          sources: [
            source === "slack"
              ? new FlumeSlackSource({
                  appToken: "test-token",
                  botToken: "test-token",
                  handshakeTimeoutMs: 100,
                })
              : new FlumeGitHubSource({ token: "test-token", requestTimeoutMs: 100 }),
          ],
        })
        expect(await flume.open()).toBeInstanceOf(Error)
        expect(flume.diagnostics().entries).toContainEqual(
          expect.objectContaining({ errorCode: "FLUME_TIMEOUT" }),
        )
        await vi.waitFor(() => expect(requestClosed).toBe(true))
        expect(runtime.pending()).toBe(0)
      } finally {
        runtime.dispose()
        server.closeAllConnections()
        await new Promise<void>((resolve) => server.close(() => resolve()))
      }
    },
  )

  it.each(["slack", "github"] as const)(
    "cancels an unread %s rate-limit body after reading its status",
    async (source) => {
      const runtime = trackedRuntime()
      let requestClosed = false
      const server = createServer((_request, response) => {
        response.writeHead(429, { "Retry-After": "60" })
        response.write("{")
        response.on("close", () => {
          requestClosed = true
        })
      })
      server.listen(0, "127.0.0.1")
      await once(server, "listening")
      const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
      runtime.deps.fetch = (_url, init) => fetch(endpoint, init)
      const flume = new Flume({
        deps: runtime.deps,
        sources: [
          source === "slack"
            ? new FlumeSlackSource({ appToken: "test-token", botToken: "test-token" })
            : new FlumeGitHubSource({ token: "test-token" }),
        ],
      })
      let running: Awaited<ReturnType<typeof flume.open>> | undefined
      try {
        running = await flume.open()
        expect(flume.diagnostics().entries).toContainEqual(
          expect.objectContaining({ errorCode: "FLUME_RATE_LIMITED" }),
        )
        await vi.waitFor(() => expect(requestClosed).toBe(true))
        if (!(running instanceof Error)) await running.close()
        expect(runtime.pending()).toBe(0)
      } finally {
        if (running && !(running instanceof Error)) await running.close()
        runtime.dispose()
        server.closeAllConnections()
        await new Promise<void>((resolve) => server.close(() => resolve()))
      }
    },
  )
})
