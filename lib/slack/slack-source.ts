import type { FlumeEvent, FlumeHandler, FlumeRuntimeDeps, FlumeSlackEnvelope, FlumeSlackSourceOptions, FlumeStatus } from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { FlumeSerialQueue } from "@/utils/serial-queue"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"
import { extractSlackMeta } from "@/slack/extract-slack-meta"
import { FlumeSlackSeenCache } from "@/slack/slack-seen-cache"
import { FlumeSlackSocketMode } from "@/slack/slack-socket-mode"

const SEEN_CACHE_MAX = 1024

export class FlumeSlackSource {

  readonly name = "slack" as const

  private socket: FlumeSlackSocketMode | null = null

  private reconnector: FlumeReconnector | null = null

  private handler: FlumeHandler | null = null

  private currentStatus: FlumeStatus = "disconnected"

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly queue = new FlumeSerialQueue()

  private readonly seen = new FlumeSlackSeenCache({ maxSize: SEEN_CACHE_MAX })

  constructor(private readonly options: FlumeSlackSourceOptions) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "slack", handler: options.onLog, deps: this.deps })

    const rc = resolveFlumeReconnectConfig(options.reconnect)

    if (rc) {
      this.reconnector = new FlumeReconnector({ ...rc, deps: this.deps })
    }
  }

  async start(handler: FlumeHandler): Promise<void | Error> {
    if (this.options.signal?.aborted) return new Error("Slack source: signal already aborted")

    this.options.signal?.addEventListener("abort", () => this.stop(), { once: true })

    this.handler = handler
    this.log.info({ action: "start", message: "starting Slack source" })

    return this.connectInternal()
  }

  async stop(): Promise<void> {
    this.log.info({ action: "stop", message: "stopping Slack source" })
    if (this.reconnector && !this.reconnector.aborted) {
      this.log.debug({ action: "reconnect.cancel", message: "aborting reconnector" })
    }
    this.reconnector?.cancel()
    this.socket?.disconnect()
    this.socket = null
    this.handler = null
    await this.queue.drain()
    this.setStatus("disconnected")
  }

  status(): FlumeStatus {
    return this.currentStatus
  }

  private async connectInternal(): Promise<void | Error> {
    this.setStatus("connecting")

    this.socket = new FlumeSlackSocketMode({
      appToken: this.options.appToken,
      onLog: this.options.onLog,
      deps: this.deps,
      onMessage: (envelope) => this.handleMessage(envelope),
      onConnected: () => {
        if (this.reconnector && this.reconnector.attempt > 0) {
          this.log.info({ action: "reconnect.reset", message: `cleared ${this.reconnector.attempt} attempts` })
        }
        this.reconnector?.reset()
        this.setStatus("connected")
      },
      onDisconnected: () => { if (!this.socket?.stopped) this.scheduleReconnect() },
    })

    const error = await this.socket.connect()

    if (error instanceof Error) {
      this.log.error({ action: "connect.failed", message: error.message, error })

      if (!this.reconnector || this.reconnector.aborted) {
        this.setStatus("disconnected")
        return error
      }

      this.scheduleReconnect()
    }
  }

  private handleMessage(envelope: FlumeSlackEnvelope): void {
    if (this.seen.has(envelope.envelope_id)) {
      this.log.debug({
        action: "dedup.skip",
        message: `duplicate envelope_id=${envelope.envelope_id}`,
        detail: { envelope_id: envelope.envelope_id, retry_attempt: envelope.retry_attempt },
      })
      return
    }

    this.seen.add(envelope.envelope_id)
    this.seen.trim()

    const event: FlumeEvent = {
      source: "slack",
      type: envelope.type,
      data: envelope.payload,
      meta: extractSlackMeta(envelope),
      receivedAt: this.deps.now(),
    }

    this.queue.add(async () => {
      try {
        await this.handler?.(event)
      } catch (err) {
        this.log.error({
          action: "handler.error",
          message: "user handler threw",
          error: err instanceof Error ? err : new Error(String(err)),
        })
      }
    })
  }

  private scheduleReconnect(): void {
    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: this.log,
      setStatus: (status) => this.setStatus(status),
      retry: () => { this.connectInternal() },
    })
  }

  private setStatus(next: FlumeStatus): void {
    if (this.currentStatus === next) return

    this.log.info({ action: "status", message: `${this.currentStatus} → ${next}` })
    this.currentStatus = next
    this.options.onStatus?.(next)
  }

}
