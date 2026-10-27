// 別 runtime でライブラリだけを動かす。HTTP / WS サーバーとテストランナーは親プロセスが所有する。
import { Flume } from "@/flume"
import { FlumeSlackSource } from "@/slack/slack-source"
import { FlumeGitHubSource } from "@/github/github-source"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { trackedRuntime } from "@/test-utils/tracked-runtime"

const [source, endpoint, mode] = process.argv.slice(2)
if (!endpoint) throw new Error("missing local endpoint")
const runtime = trackedRuntime()
runtime.deps.fetch = (_url, init) => fetch(endpoint, init)
if (source === "discord")
  runtime.deps.WebSocket = class extends globalThis.WebSocket {
    constructor(_url: string | URL) {
      super(endpoint!)
    }
  }
const received: number[] = []
const complete = Promise.withResolvers<void>()
const flume = new Flume({
  deps: runtime.deps,
  reconnect: mode === "socket" ? { baseDelay: 1, maxDelay: 2 } : false,
  sources: [
    source === "slack"
      ? new FlumeSlackSource({
          appToken: "test-token",
          botToken: "test-token",
          handshakeTimeoutMs: 100,
        })
      : source === "discord"
        ? new FlumeDiscordSource({ token: "test-token" })
        : new FlumeGitHubSource({ token: "test-token", requestTimeoutMs: 100 }),
  ],
  onEvent: (item) => {
    if (item.kind !== "event" || !("order" in item.event.data)) return
    received.push(Number(item.event.data.order))
    if (received.length === 300) complete.resolve()
  },
})
const running = await flume.open()
if (mode === "socket") await complete.promise
if (!(running instanceof Error)) {
  await running.close()
  await running.drain()
}
console.log(
  JSON.stringify({
    bun: process.versions.bun,
    failed: running instanceof Error,
    received,
    pendingTimers: runtime.pending(),
    delivery: flume.deliveryStatus(),
    codes: flume.diagnostics().entries.map((entry) => entry.errorCode),
  }),
)
runtime.dispose()
