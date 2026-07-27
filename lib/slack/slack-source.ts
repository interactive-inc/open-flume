import type { FlumeSlackEnvelope, FlumeSlackSourceOptions, FlumeSourceStartContext } from "@/types"
import { FlumeHttpError } from "@/errors/http-error"
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

/**
 * `apps.connections.open` が返す恒久エラー。トークンが無効な状態で再接続しても
 * 回復しないため、reconnect を打ち切って呼び出し側へエラーを返す
 */
const TERMINAL_SLACK_ERROR_CODES = new Set([
  "invalid_auth",
  "account_inactive",
  "token_revoked",
  "not_authed",
  "not_allowed_token_type",
  "token_expired",
  "missing_scope",
])

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

    // close()/disconnect() は this.socket を null 化するため、await 以降と callback 内の
    // 参照は closure に捕捉したローカル参照で行う (mutable field の null-deref race を避ける)
    const socket = new FlumeSlackSocketMode({
      appToken: this.options.appToken,
      idleTimeoutMs: this.options.idleTimeoutMs,
      handshakeTimeoutMs: this.options.handshakeTimeoutMs,
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
        if (socket.isStopped) {
          this.setStatus("disconnected")
          return
        }
        this.scheduleReconnect(ctx)
      },
    })

    this.socket = socket

    const error = await socket.connect({ signal: this.internalController?.signal })

    if (error instanceof Error) {
      ctx.log.error({ action: "connect.failed", message: safeErrorMessage({ error }), error })

      if (this.isTerminalSlackError(error)) {
        ctx.log.error({
          action: "reconnect.terminal",
          message: `permanent Slack API error, not reconnecting: ${safeErrorMessage({ error })}`,
          error,
        })
        this.setStatus("disconnected")
        return error
      }

      if (socket.isStopped || !this.reconnector || this.reconnector.aborted) {
        this.setStatus("disconnected")
        return error
      }

      this.scheduleReconnect(ctx, this.minRetryDelayMs(error))
    }

    return null
  }

  private isTerminalSlackError(error: Error): boolean {
    if (!(error instanceof FlumeHttpError)) return false
    if (error.code === null) return false
    return TERMINAL_SLACK_ERROR_CODES.has(error.code)
  }

  private minRetryDelayMs(error: Error): number | undefined {
    if (!(error instanceof FlumeHttpError)) return undefined
    return error.retryAfterMs ?? undefined
  }

  private handleMessage(ctx: FlumeSourceStartContext, envelope: FlumeSlackEnvelope): void {
    const seen = this.seen
    if (!seen) return

    const dedupKey = this.toDedupKey(envelope)

    if (seen.has(dedupKey)) {
      ctx.log.debug({
        action: "dedup.skip",
        message: `duplicate key=${dedupKey}`,
        detail: {
          dedup_key: dedupKey,
          envelope_id: envelope.envelope_id,
          retry_attempt: envelope.retry_attempt,
        },
      })
      return
    }

    seen.add(dedupKey)
    seen.trim()

    this.emit({
      source: "slack",
      type: envelope.type,
      data: envelope.payload,
      meta: this.safeExtractMeta(ctx, envelope),
      receivedAt: safeNow({ deps: ctx.deps }),
    })
  }

  /**
   * Events API の再配送は envelope_id が変わり得るため、payload.event_id があれば
   * そちらを重複判定キーとして優先する (再配送をまたいで安定な識別子)
   */
  private toDedupKey(envelope: FlumeSlackEnvelope): string {
    const eventId = envelope.payload.event_id
    if (typeof eventId === "string" && eventId.length > 0) return eventId
    return envelope.envelope_id
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

  private scheduleReconnect(ctx: FlumeSourceStartContext, minDelayMs?: number): void {
    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: ctx.log,
      setStatus: (status) => this.setStatus(status),
      minDelayMs,
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
