import type { FlumeDiscordSourceOptions, FlumeEvent, FlumeHandler, FlumeRuntimeDeps, FlumeStatus } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { FlumeConnectionError } from "@/errors/connection-error"
import { isRecord } from "@/utils/is-record"
import { FlumeDiscordGateway } from "@/discord/discord-gateway"
import { FlumeDiscordGatewayIntents } from "@/discord/discord-gateway-intents"

const DEFAULT_INTENTS =
  FlumeDiscordGatewayIntents.Guilds |
  FlumeDiscordGatewayIntents.GuildMessages |
  FlumeDiscordGatewayIntents.DirectMessages |
  FlumeDiscordGatewayIntents.MessageContent

export class FlumeDiscordSource {

  private gateway: FlumeDiscordGateway | null = null

  private reconnector: FlumeReconnector | null = null

  private handler: FlumeHandler | null = null

  private currentStatus: FlumeStatus = "disconnected"

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  constructor(private readonly options: FlumeDiscordSourceOptions) {
    this.deps = options.deps
    this.log = new FlumeLogger({ source: "discord", handler: options.onLog, deps: this.deps })

    const rc = resolveFlumeReconnectConfig(options.reconnect)

    if (rc) {
      this.reconnector = new FlumeReconnector({ ...rc, deps: this.deps })
    }
  }

  async start(handler: FlumeHandler): Promise<void> {
    if (this.options.signal?.aborted) return

    this.options.signal?.addEventListener("abort", () => this.stop(), { once: true })

    this.handler = handler
    this.log.info({ action: "start", message: "starting Discord source" })
    await this.connectInternal()
  }

  async stop(): Promise<void> {
    this.log.info({ action: "stop", message: "stopping Discord source" })
    if (this.reconnector && !this.reconnector.aborted) {
      this.log.debug({ action: "reconnect.cancel", message: "aborting reconnector" })
    }
    this.reconnector?.cancel()
    this.gateway?.disconnect()
    this.gateway = null
    this.handler = null
    this.setStatus("disconnected")
  }

  status(): FlumeStatus {
    return this.currentStatus
  }

  private async connectInternal(resumeUrl?: string): Promise<void> {
    this.setStatus("connecting")

    this.gateway = new FlumeDiscordGateway({
      token: this.options.token,
      intents: this.options.intents ?? DEFAULT_INTENTS,
      onLog: this.options.onLog,
      deps: this.deps,
      onDispatch: (eventName, eventData) => this.handleDispatch(eventName, eventData),
      onStatus: (status) => this.handleGatewayStatus(status),
    })

    const error = await this.gateway.connect(resumeUrl)

    if (error instanceof FlumeConnectionError) {
      this.log.error({ action: "connect.failed", message: error.message, error })
      this.scheduleReconnect()
    }
  }

  private handleDispatch(eventName: string, eventData: Record<string, unknown>): void {
    const event: FlumeEvent = {
      source: "discord",
      type: eventName,
      data: eventData,
      meta: FlumeDiscordSource.extractMeta(eventName, eventData),
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

  private handleGatewayStatus(status: "connected" | "disconnected"): void {
    if (status === "connected") {
      if (this.reconnector && this.reconnector.attempt > 0) {
        this.log.info({ action: "reconnect.reset", message: `cleared ${this.reconnector.attempt} attempts` })
      }
      this.reconnector?.reset()
      this.setStatus("connected")
    }

    if (status === "disconnected" && !this.gateway?.stopped) {
      this.scheduleReconnect()
    }
  }

  private scheduleReconnect(): void {
    if (!this.reconnector || this.reconnector.aborted) {
      this.setStatus("disconnected")
      return
    }

    const url = this.gateway?.session.resumeUrl ?? undefined

    this.setStatus("reconnecting")

    const delay = this.reconnector.schedule(() => {
      this.connectInternal(url)
    })

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

  static extractMeta(eventName: string, eventData: Record<string, unknown>): Record<string, string> {
    const meta: Record<string, string> = { event_type: eventName }

    if (typeof eventData.channel_id === "string") meta.channel_id = eventData.channel_id
    if (typeof eventData.guild_id === "string") meta.guild_id = eventData.guild_id
    if (isRecord(eventData.author) && typeof eventData.author.id === "string") meta.user_id = eventData.author.id

    return meta
  }
}
