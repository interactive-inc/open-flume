import type { FlumeDiscordSourceOptions, FlumeSourceStartContext } from "@/types"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeReconnector } from "@/reconnector"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"
import { FlumeDiscordGateway } from "@/discord/discord-gateway"
import { FlumeDiscordGatewayIntents } from "@/discord/discord-gateway-intents"
import { flumeExtractDiscordMeta } from "@/discord/extract-discord-meta"
import { FlumeSource } from "@/flume-source"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"

const DEFAULT_INTENTS =
  FlumeDiscordGatewayIntents.Guilds |
  FlumeDiscordGatewayIntents.GuildMessages |
  FlumeDiscordGatewayIntents.DirectMessages

export class FlumeDiscordSource extends FlumeSource {
  readonly name = "discord" as const

  private gateway: FlumeDiscordGateway | null = null

  private reconnector: FlumeReconnector | null = null

  constructor(private readonly options: FlumeDiscordSourceOptions) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    if (!this.hasWebSocket(ctx)) {
      return new FlumeStartError(
        "Discord source: deps.WebSocket is null (no WebSocket runtime available)",
      )
    }

    if (ctx.reconnect && !this.reconnector) {
      this.reconnector = new FlumeReconnector({ ...ctx.reconnect, log: ctx.log, deps: ctx.deps })
    }

    return await this.connectInternal(ctx)
  }

  protected disconnect(): void {
    if (this.reconnector && !this.reconnector.aborted) {
      this.context?.log.debug({ action: "reconnect.cancel", message: "aborting reconnector" })
    }
    this.reconnector?.cancel()
    this.gateway?.disconnect()
    this.gateway = null
  }

  private hasWebSocket(ctx: FlumeSourceStartContext): boolean {
    const result = attempt(() => Boolean(ctx.deps.WebSocket))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      ctx.log.error({
        action: "deps.web-socket.read.error",
        message: safeErrorMessage({ error }),
        error,
      })
      return false
    }
    return result
  }

  private async connectInternal(
    ctx: FlumeSourceStartContext,
    resumeUrl?: string,
  ): Promise<Error | null> {
    this.setStatus("connecting")

    this.gateway = new FlumeDiscordGateway({
      token: this.options.token,
      intents: this.options.intents ?? DEFAULT_INTENTS,
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onDispatch: (eventName, eventData) => this.dispatch(ctx, eventName, eventData),
      onStatus: (status) => this.handleGatewayStatus(ctx, status),
    })

    const error = await this.gateway.connect(resumeUrl)

    if (error instanceof FlumeConnectionError) {
      ctx.log.error({ action: "connect.failed", message: safeErrorMessage({ error }), error })

      if (this.gateway.isStopped || !this.reconnector || this.reconnector.aborted) {
        this.setStatus("disconnected")
        return error
      }

      this.scheduleReconnect(ctx)
    }

    return null
  }

  private dispatch(
    ctx: FlumeSourceStartContext,
    eventName: string,
    eventData: Record<string, unknown>,
  ): void {
    this.emit({
      source: "discord",
      type: eventName,
      data: eventData,
      meta: this.safeExtractMeta(ctx, eventName, eventData),
      receivedAt: safeNow({ deps: ctx.deps }),
    })
  }

  private safeExtractMeta(
    ctx: FlumeSourceStartContext,
    eventName: string,
    eventData: Record<string, unknown>,
  ): Record<string, string> {
    const result = attempt(() => flumeExtractDiscordMeta(eventName, eventData))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      ctx.log.warn({
        action: "meta.extract.error",
        message: safeErrorMessage({ error }),
        error,
        detail: { eventName },
      })
      return { event_type: eventName }
    }
    return result
  }

  private handleGatewayStatus(
    ctx: FlumeSourceStartContext,
    status: "connected" | "disconnected",
  ): void {
    if (status === "connected") {
      if (this.reconnector && this.reconnector.attempt > 0) {
        ctx.log.info({
          action: "reconnect.reset",
          message: `cleared ${this.reconnector.attempt} attempts`,
        })
      }
      this.reconnector?.reset()
      this.setStatus("connected")
      return
    }

    if (this.gateway?.isStopped) {
      this.setStatus("disconnected")
      return
    }

    this.scheduleReconnect(ctx)
  }

  private scheduleReconnect(ctx: FlumeSourceStartContext): void {
    const url = this.gateway?.session.resumeUrl ?? undefined

    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: ctx.log,
      setStatus: (status) => this.setStatus(status),
      retry: () => {
        this.connectInternal(ctx, url).catch((err: unknown) => {
          const error = safeNormalizeError({ value: err })
          ctx.log.error({
            action: "reconnect.unhandled",
            message: safeErrorMessage({ error }),
            error,
          })
          this.setStatus("disconnected")
        })
      },
    })
  }
}
