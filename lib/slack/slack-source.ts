import type { FlumeEvent, FlumeHandler, FlumeRuntimeDeps, FlumeSlackEnvelope, FlumeSlackSourceOptions, FlumeStatus } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { isRecord } from "@/utils/is-record"
import { FlumeSlackSocketMode } from "@/slack/slack-socket-mode"

export class FlumeSlackSource {

  private socket: FlumeSlackSocketMode | null = null

  private reconnector: FlumeReconnector | null = null

  private handler: FlumeHandler | null = null

  private currentStatus: FlumeStatus = "disconnected"

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  constructor(private readonly options: FlumeSlackSourceOptions) {
    this.deps = options.deps
    this.log = new FlumeLogger({ source: "slack", handler: options.onLog, deps: this.deps })

    const rc = resolveFlumeReconnectConfig(options.reconnect)

    if (rc) {
      this.reconnector = new FlumeReconnector({ ...rc, deps: this.deps })
    }
  }

  async start(handler: FlumeHandler): Promise<void> {
    if (this.options.signal?.aborted) return

    this.options.signal?.addEventListener("abort", () => this.stop(), { once: true })

    this.handler = handler
    this.log.info({ action: "start", message: "starting Slack source" })
    await this.connectInternal()
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
    this.setStatus("disconnected")
  }

  status(): FlumeStatus {
    return this.currentStatus
  }

  private async connectInternal(): Promise<void> {
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
      this.scheduleReconnect()
    }
  }

  private handleMessage(envelope: FlumeSlackEnvelope): void {
    const event: FlumeEvent = {
      source: "slack",
      type: envelope.type,
      data: envelope.payload,
      meta: FlumeSlackSource.extractMeta(envelope),
      receivedAt: this.deps.now(),
    }

    try {
      this.handler?.(event)
    } catch (err) {
      this.log.error({
        action: "handler.error",
        message: "user handler threw",
        error: err instanceof Error ? err : new Error(String(err)),
      })
    }
  }

  private scheduleReconnect(): void {
    if (!this.reconnector || this.reconnector.aborted) {
      this.setStatus("disconnected")
      return
    }

    this.setStatus("reconnecting")

    const delay = this.reconnector.schedule(() => this.connectInternal())

    if (delay === -1) {
      this.log.error({ action: "reconnect.exhausted", message: `gave up after ${this.reconnector.attempt} attempts` })
      this.setStatus("disconnected")
    } else {
      this.log.info({ action: "reconnect.scheduled", message: `next attempt in ${Math.round(delay)}ms` })
    }
  }

  private setStatus(next: FlumeStatus): void {
    if (this.currentStatus === next) return

    this.log.info({ action: "status", message: `${this.currentStatus} → ${next}` })
    this.currentStatus = next
    this.options.onStatus?.(next)
  }

  static extractMeta(envelope: FlumeSlackEnvelope): Record<string, string> {
    const meta: Record<string, string> = { event_type: envelope.type }
    const eventPayload = isRecord(envelope.payload.event) ? envelope.payload.event : null

    if (!eventPayload) return meta

    if (typeof eventPayload.channel === "string") meta.channel_id = eventPayload.channel
    if (typeof eventPayload.user === "string") meta.user_id = eventPayload.user
    if (typeof eventPayload.thread_ts === "string") meta.thread_ts = eventPayload.thread_ts
    if (typeof eventPayload.type === "string") meta.slack_event_type = eventPayload.type

    return meta
  }
}
