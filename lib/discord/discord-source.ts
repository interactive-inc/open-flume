import type {
  FlumeDiscordSourceOptions,
  FlumeEvent,
  FlumeHandler,
  FlumeRuntimeDeps,
  FlumeSourceStartOptions,
  FlumeStatus,
} from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"
import { FlumeDiscordGateway } from "@/discord/discord-gateway"
import { FlumeDiscordGatewayIntents } from "@/discord/discord-gateway-intents"
import { flumeExtractDiscordMeta } from "@/discord/extract-discord-meta"
import { FlumeSignalRegistry } from "@/source-helpers/flume-signal-registry"
import { FlumeStatusEmitter } from "@/source-helpers/flume-status-emitter"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"
import { FlumeSerialQueue } from "@/utils/serial-queue"

const DEFAULT_INTENTS =
  FlumeDiscordGatewayIntents.Guilds |
  FlumeDiscordGatewayIntents.GuildMessages |
  FlumeDiscordGatewayIntents.DirectMessages

export class FlumeDiscordSource {
  readonly name = "discord" as const

  private gateway: FlumeDiscordGateway | null = null

  private reconnector: FlumeReconnector | null = null

  private handler: FlumeHandler | null = null

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly queue = new FlumeSerialQueue()

  private readonly signals: FlumeSignalRegistry

  private readonly statusEmitter: FlumeStatusEmitter

  private readonly onSignalAbort = (): void => {
    safeInvokeCallback({
      fn: () => this.stop(),
      onError: (error) => {
        this.log.error({
          action: "signal.abort.stop.failed",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }

  constructor(private readonly options: FlumeDiscordSourceOptions) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "discord", handler: options.onLog, deps: this.deps })
    this.signals = new FlumeSignalRegistry({ log: this.log, onAbort: this.onSignalAbort })
    this.statusEmitter = new FlumeStatusEmitter({ log: this.log, onStatus: options.onStatus })

    const rc = resolveFlumeReconnectConfig(options.reconnect)
    if (rc) {
      this.reconnector = new FlumeReconnector({ ...rc, log: this.log, deps: this.deps })
    }
  }

  async start(handler: FlumeHandler, options?: FlumeSourceStartOptions): Promise<Error | null> {
    if (
      this.signals.isAnyAborted(this.options.signal) ||
      this.signals.isAnyAborted(options?.signal)
    ) {
      return new FlumeStartError("Discord source: signal already aborted")
    }

    if (!this.hasWebSocket()) {
      return new FlumeStartError(
        "Discord source: deps.WebSocket is null (no WebSocket runtime available)",
      )
    }

    this.signals.register(this.options.signal)
    this.signals.register(options?.signal)

    this.handler = handler
    this.log.info({ action: "source.start", message: "starting Discord source" })

    return await this.connectInternal()
  }

  async stop(): Promise<void> {
    this.signals.unregisterAll()
    this.log.info({ action: "source.stop", message: "stopping Discord source" })

    if (this.reconnector && !this.reconnector.aborted) {
      this.log.debug({ action: "reconnect.cancel", message: "aborting reconnector" })
    }

    this.reconnector?.cancel()
    this.gateway?.disconnect()
    await this.queue.drain()
    this.gateway = null
    this.handler = null
    this.statusEmitter.set("disconnected")
  }

  status(): FlumeStatus {
    return this.statusEmitter.value
  }

  private hasWebSocket(): boolean {
    const result = attempt(() => Boolean(this.deps.WebSocket))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      this.log.error({
        action: "deps.web-socket.read.error",
        message: safeErrorMessage({ error }),
        error,
      })
      return false
    }
    return result
  }

  private async connectInternal(resumeUrl?: string): Promise<Error | null> {
    this.statusEmitter.set("connecting")

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
      this.log.error({ action: "connect.failed", message: safeErrorMessage({ error }), error })

      if (this.gateway.isStopped || !this.reconnector || this.reconnector.aborted) {
        this.statusEmitter.set("disconnected")
        return error
      }

      this.scheduleReconnect()
    }

    return null
  }

  private handleDispatch(eventName: string, eventData: Record<string, unknown>): void {
    const handler = this.handler
    if (!handler) return

    this.queue.add(async () => {
      const event: FlumeEvent = {
        source: "discord",
        type: eventName,
        data: eventData,
        meta: this.safeExtractMeta(eventName, eventData),
        receivedAt: safeNow({ deps: this.deps }),
      }
      const r = await attempt(() => Promise.resolve(handler(event)))
      if (r instanceof Error) {
        this.log.error({
          action: "handler.error",
          message: safeErrorMessage({ error: r }),
          error: r,
        })
      }
    })
  }

  private safeExtractMeta(
    eventName: string,
    eventData: Record<string, unknown>,
  ): Record<string, string> {
    const result = attempt(() => flumeExtractDiscordMeta(eventName, eventData))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      this.log.warn({
        action: "meta.extract.error",
        message: safeErrorMessage({ error }),
        error,
        detail: { eventName },
      })
      return { event_type: eventName }
    }
    return result
  }

  private handleGatewayStatus(status: "connected" | "disconnected"): void {
    if (status === "connected") {
      if (this.reconnector && this.reconnector.attempt > 0) {
        this.log.info({
          action: "reconnect.reset",
          message: `cleared ${this.reconnector.attempt} attempts`,
        })
      }
      this.reconnector?.reset()
      this.statusEmitter.set("connected")
      return
    }

    if (this.gateway?.isStopped) {
      this.statusEmitter.set("disconnected")
      return
    }

    this.scheduleReconnect()
  }

  private scheduleReconnect(): void {
    const url = this.gateway?.session.resumeUrl ?? undefined

    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: this.log,
      setStatus: (status) => this.statusEmitter.set(status),
      retry: () => {
        this.connectInternal(url).catch((err: unknown) => {
          const error = safeNormalizeError({ value: err })
          this.log.error({
            action: "reconnect.unhandled",
            message: safeErrorMessage({ error }),
            error,
          })
          this.statusEmitter.set("disconnected")
        })
      },
    })
  }
}
