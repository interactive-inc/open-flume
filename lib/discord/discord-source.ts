import type { FlumeDiscordSourceOptions, FlumeEvent, FlumeHandler, FlumeRuntimeDeps, FlumeStatus } from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeSerialQueue } from "@/utils/serial-queue"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"
import { extractDiscordMeta } from "@/discord/extract-discord-meta"
import { FlumeDiscordGateway } from "@/discord/discord-gateway"
import { FlumeDiscordGatewayIntents } from "@/discord/discord-gateway-intents"

const DEFAULT_INTENTS =
  FlumeDiscordGatewayIntents.Guilds |
  FlumeDiscordGatewayIntents.GuildMessages |
  FlumeDiscordGatewayIntents.DirectMessages |
  FlumeDiscordGatewayIntents.MessageContent

export class FlumeDiscordSource {

  readonly name = "discord" as const

  private gateway: FlumeDiscordGateway | null = null

  private reconnector: FlumeReconnector | null = null

  private handler: FlumeHandler | null = null

  private currentStatus: FlumeStatus = "disconnected"

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly queue = new FlumeSerialQueue()

  constructor(private readonly options: FlumeDiscordSourceOptions) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "discord", handler: options.onLog, deps: this.deps })

    const rc = resolveFlumeReconnectConfig(options.reconnect)

    if (rc) {
      this.reconnector = new FlumeReconnector({ ...rc, deps: this.deps })
    }
  }

  async start(handler: FlumeHandler): Promise<Error | null> {
    if (this.options.signal?.aborted) {
      return new FlumeStartError("Discord source: signal already aborted")
    }

    this.options.signal?.addEventListener("abort", () => this.stop(), { once: true })

    this.handler = handler
    this.log.info({ action: "start", message: "starting Discord source" })

    return await this.connectInternal()
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
    await this.queue.drain()
    this.setStatus("disconnected")
  }

  status(): FlumeStatus {
    return this.currentStatus
  }

  private async connectInternal(resumeUrl?: string): Promise<Error | null> {
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

      if (!this.reconnector || this.reconnector.aborted) {
        this.setStatus("disconnected")
        return error
      }

      this.scheduleReconnect()
    }

    return null
  }

  private handleDispatch(eventName: string, eventData: Record<string, unknown>): void {
    const event: FlumeEvent = {
      source: "discord",
      type: eventName,
      data: eventData,
      meta: extractDiscordMeta(eventName, eventData),
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
    const url = this.gateway?.session.resumeUrl ?? undefined

    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: this.log,
      setStatus: (status) => this.setStatus(status),
      retry: () => { this.connectInternal(url) },
    })
  }

  private setStatus(next: FlumeStatus): void {
    if (this.currentStatus === next) return

    this.log.info({ action: "status", message: `${this.currentStatus} → ${next}` })
    this.currentStatus = next
    this.options.onStatus?.(next)
  }

}
