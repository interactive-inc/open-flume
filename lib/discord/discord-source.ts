import type { FlumeDiscordSourceOptions, FlumeSourceStartContext } from "@/types"
import type { FlumeDiscordGatewaySession } from "@/discord/discord-gateway-session"
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

// Discord の IDENTIFY は 1 回 / 5 秒。RESUME できない再接続は backoff にこの下限を敷く
const IDENTIFY_MIN_RECONNECT_DELAY_MS = 5_000

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

  /**
   * gateway を 1 接続 = 1 インスタンスで作り直す。`session` は前回接続から引き継いだ
   * resume 可能な session (無ければ IDENTIFY)。await 後は `this.gateway` でなく local な
   * `gateway` を参照する (並行する close() が `this.gateway` を null 化しても壊れない)
   */
  private async connectInternal(
    ctx: FlumeSourceStartContext,
    session?: FlumeDiscordGatewaySession,
  ): Promise<Error | null> {
    this.setStatus("connecting")

    const gateway = new FlumeDiscordGateway({
      token: this.options.token,
      intents: this.options.intents ?? DEFAULT_INTENTS,
      handshakeTimeoutMs: this.options.handshakeTimeoutMs,
      session,
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onDispatch: (eventName, eventData) => this.dispatch(ctx, eventName, eventData),
      onStatus: (status) => this.handleGatewayStatus(ctx, gateway, status),
    })
    this.gateway = gateway

    const resumeUrl =
      session !== undefined && session.canResume() && session.resumeUrl !== null
        ? session.resumeUrl
        : undefined
    const error = await gateway.connect(resumeUrl)

    if (error instanceof FlumeConnectionError) {
      ctx.log.error({ action: "connect.failed", message: safeErrorMessage({ error }), error })

      if (gateway.isStopped || !this.reconnector || this.reconnector.aborted) {
        this.setStatus("disconnected")
        return error
      }

      this.scheduleReconnect(ctx, gateway)
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

  /**
   * status は発火元 gateway に束縛して受ける。交換済み (stale) な gateway からの通知は無視し、
   * 現行 gateway の状態を誤って上書きしない
   */
  private handleGatewayStatus(
    ctx: FlumeSourceStartContext,
    gateway: FlumeDiscordGateway,
    status: "connected" | "disconnected",
  ): void {
    if (gateway !== this.gateway) {
      ctx.log.debug({
        action: "gateway.status.stale",
        message: `ignored ${status} from replaced gateway`,
      })
      return
    }

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

    if (gateway.isStopped) {
      this.setStatus("disconnected")
      return
    }

    this.scheduleReconnect(ctx, gateway)
  }

  /**
   * resume 可能な session はこの時点で捕捉して次の gateway へ引き継ぐ (gateway インスタンスは
   * 接続ごとに破棄されるため)。resume できない = IDENTIFY し直す再接続には identify rate limit
   * (1 回 / 5 秒) を守る下限 delay を敷く
   */
  private scheduleReconnect(ctx: FlumeSourceStartContext, gateway: FlumeDiscordGateway): void {
    const capturedSession = gateway.session.canResume() ? gateway.session : undefined

    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: ctx.log,
      setStatus: (status) => this.setStatus(status),
      minDelayMs: capturedSession === undefined ? IDENTIFY_MIN_RECONNECT_DELAY_MS : undefined,
      retry: () => {
        this.connectInternal(ctx, capturedSession).catch((err: unknown) => {
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
