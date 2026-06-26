import type { FlumeSlackEnvelope, FlumeSlackSourceOptions, FlumeSourceStartContext } from "@/types"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeReconnector } from "@/reconnector"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"
import { flumeExtractSlackMeta } from "@/slack/extract-slack-meta"
import { FlumeSlackSeenCache } from "@/slack/slack-seen-cache"
import { FlumeSlackSocketMode } from "@/slack/slack-socket-mode"
import { FlumeSource } from "@/flume-source"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"

const SEEN_CACHE_MAX = 1024
const SEEN_CACHE_TTL_MS = 5 * 60 * 1000

export class FlumeSlackSource extends FlumeSource {
  readonly name = "slack" as const

  private socket: FlumeSlackSocketMode | null = null

  private reconnector: FlumeReconnector | null = null

  private internalController: AbortController | null = null

  private seen: FlumeSlackSeenCache | null = null

  constructor(private readonly options: FlumeSlackSourceOptions) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    if (!this.hasWebSocket(ctx)) {
      return new FlumeStartError(
        "Slack source: deps.WebSocket is null (no WebSocket runtime available)",
      )
    }

    this.seen = new FlumeSlackSeenCache({
      maxSize: SEEN_CACHE_MAX,
      ttlMs: SEEN_CACHE_TTL_MS,
      deps: ctx.deps,
    })

    if (ctx.reconnect && !this.reconnector) {
      this.reconnector = new FlumeReconnector({
        ...ctx.reconnect,
        log: ctx.log,
        deps: ctx.deps,
      })
    }

    const controllerResult = attempt(() => new AbortController())
    if (controllerResult instanceof Error) {
      const error = safeNormalizeError({ value: controllerResult })
      ctx.log.error({
        action: "slack.abort-controller.new.error",
        message: safeErrorMessage({ error }),
        error,
      })
      this.internalController = null
    } else {
      this.internalController = controllerResult
    }

    return await this.connectInternal(ctx)
  }

  protected disconnect(): void {
    const ctx = this.context
    if (ctx && this.reconnector && !this.reconnector.aborted) {
      ctx.log.debug({ action: "reconnect.cancel", message: "aborting reconnector" })
    }
    this.reconnector?.cancel()
    this.internalController?.abort()
    this.socket?.disconnect()
    this.socket = null
    this.internalController = null
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

  private async connectInternal(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")

    this.socket = new FlumeSlackSocketMode({
      appToken: this.options.appToken,
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onMessage: (envelope) => this.handleMessage(ctx, envelope),
      onConnected: () => {
        if (this.reconnector && this.reconnector.attempt > 0) {
          ctx.log.info({
            action: "reconnect.reset",
            message: `cleared ${this.reconnector.attempt} attempts`,
          })
        }
        this.reconnector?.reset()
        this.setStatus("connected")
      },
      onDisconnected: () => {
        if (this.socket?.isStopped) {
          this.setStatus("disconnected")
          return
        }
        this.scheduleReconnect(ctx)
      },
    })

    const error = await this.socket.connect({ signal: this.internalController?.signal })

    if (error instanceof Error) {
      ctx.log.error({ action: "connect.failed", message: safeErrorMessage({ error }), error })

      if (this.socket.isStopped || !this.reconnector || this.reconnector.aborted) {
        this.setStatus("disconnected")
        return error
      }

      this.scheduleReconnect(ctx)
    }

    return null
  }

  private handleMessage(ctx: FlumeSourceStartContext, envelope: FlumeSlackEnvelope): void {
    const seen = this.seen
    if (!seen) return

    if (seen.has(envelope.envelope_id)) {
      ctx.log.debug({
        action: "dedup.skip",
        message: `duplicate envelope_id=${envelope.envelope_id}`,
        detail: { envelope_id: envelope.envelope_id, retry_attempt: envelope.retry_attempt },
      })
      return
    }

    seen.add(envelope.envelope_id)
    seen.trim()

    this.emit({
      source: "slack",
      type: envelope.type,
      data: envelope.payload,
      meta: this.safeExtractMeta(ctx, envelope),
      receivedAt: safeNow({ deps: ctx.deps }),
    })
  }

  private safeExtractMeta(
    ctx: FlumeSourceStartContext,
    envelope: FlumeSlackEnvelope,
  ): Record<string, string> {
    const result = attempt(() => flumeExtractSlackMeta(envelope))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      ctx.log.warn({
        action: "meta.extract.error",
        message: safeErrorMessage({ error }),
        error,
        detail: { envelopeType: envelope.type },
      })
      return { event_type: envelope.type }
    }
    return result
  }

  private scheduleReconnect(ctx: FlumeSourceStartContext): void {
    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: ctx.log,
      setStatus: (status) => this.setStatus(status),
      retry: () => {
        this.connectInternal(ctx).catch((err: unknown) => {
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
