import type {
  FlumeEvent,
  FlumeHandler,
  FlumeRuntimeDeps,
  FlumeSlackEnvelope,
  FlumeSlackSourceOptions,
  FlumeSourceStartOptions,
  FlumeStatus,
} from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { resolveFlumeReconnectConfig } from "@/reconnect-config"
import { scheduleFlumeReconnect } from "@/schedule-reconnect"
import { flumeExtractSlackMeta } from "@/slack/extract-slack-meta"
import { FlumeSlackSeenCache } from "@/slack/slack-seen-cache"
import { FlumeSlackSocketMode } from "@/slack/slack-socket-mode"
import { FlumeSignalRegistry } from "@/source-helpers/flume-signal-registry"
import { FlumeStatusEmitter } from "@/source-helpers/flume-status-emitter"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"
import { FlumeSerialQueue } from "@/utils/serial-queue"

const SEEN_CACHE_MAX = 1024
const SEEN_CACHE_TTL_MS = 5 * 60 * 1000

export class FlumeSlackSource {
  readonly name = "slack" as const

  private socket: FlumeSlackSocketMode | null = null

  private reconnector: FlumeReconnector | null = null

  private handler: FlumeHandler | null = null

  private internalController: AbortController | null = null

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly queue = new FlumeSerialQueue()

  private readonly seen: FlumeSlackSeenCache

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

  constructor(private readonly options: FlumeSlackSourceOptions) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "slack", handler: options.onLog, deps: this.deps })
    this.signals = new FlumeSignalRegistry({ log: this.log, onAbort: this.onSignalAbort })
    this.statusEmitter = new FlumeStatusEmitter({ log: this.log, onStatus: options.onStatus })

    this.seen = new FlumeSlackSeenCache({
      maxSize: SEEN_CACHE_MAX,
      ttlMs: SEEN_CACHE_TTL_MS,
      deps: this.deps,
    })

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
      return new FlumeStartError("Slack source: signal already aborted")
    }

    if (!this.hasWebSocket()) {
      return new FlumeStartError(
        "Slack source: deps.WebSocket is null (no WebSocket runtime available)",
      )
    }

    this.signals.register(this.options.signal)
    this.signals.register(options?.signal)

    this.handler = handler

    const controllerResult = attempt(() => new AbortController())
    if (controllerResult instanceof Error) {
      const error = safeNormalizeError({ value: controllerResult })
      this.log.error({
        action: "slack.abort-controller.new.error",
        message: safeErrorMessage({ error }),
        error,
      })
      this.internalController = null
    } else {
      this.internalController = controllerResult
    }

    this.log.info({ action: "source.start", message: "starting Slack source" })

    return await this.connectInternal()
  }

  async stop(): Promise<void> {
    this.signals.unregisterAll()
    this.log.info({ action: "source.stop", message: "stopping Slack source" })

    if (this.reconnector && !this.reconnector.aborted) {
      this.log.debug({ action: "reconnect.cancel", message: "aborting reconnector" })
    }

    this.reconnector?.cancel()
    this.internalController?.abort()
    this.socket?.disconnect()
    await this.queue.drain()
    this.socket = null
    this.handler = null
    this.internalController = null
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

  private async connectInternal(): Promise<Error | null> {
    this.statusEmitter.set("connecting")

    this.socket = new FlumeSlackSocketMode({
      appToken: this.options.appToken,
      onLog: this.options.onLog,
      deps: this.deps,
      onMessage: (envelope) => this.handleMessage(envelope),
      onConnected: () => {
        if (this.reconnector && this.reconnector.attempt > 0) {
          this.log.info({
            action: "reconnect.reset",
            message: `cleared ${this.reconnector.attempt} attempts`,
          })
        }
        this.reconnector?.reset()
        this.statusEmitter.set("connected")
      },
      onDisconnected: () => {
        if (this.socket?.isStopped) {
          this.statusEmitter.set("disconnected")
          return
        }
        this.scheduleReconnect()
      },
    })

    const error = await this.socket.connect({ signal: this.internalController?.signal })

    if (error instanceof Error) {
      this.log.error({ action: "connect.failed", message: safeErrorMessage({ error }), error })

      if (this.socket.isStopped || !this.reconnector || this.reconnector.aborted) {
        this.statusEmitter.set("disconnected")
        return error
      }

      this.scheduleReconnect()
    }

    return null
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

    const handler = this.handler
    if (!handler) return

    this.queue.add(async () => {
      const event: FlumeEvent = {
        source: "slack",
        type: envelope.type,
        data: envelope.payload,
        meta: this.safeExtractMeta(envelope),
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

  private safeExtractMeta(envelope: FlumeSlackEnvelope): Record<string, string> {
    const result = attempt(() => flumeExtractSlackMeta(envelope))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      this.log.warn({
        action: "meta.extract.error",
        message: safeErrorMessage({ error }),
        error,
        detail: { envelopeType: envelope.type },
      })
      return { event_type: envelope.type }
    }
    return result
  }

  private scheduleReconnect(): void {
    scheduleFlumeReconnect({
      reconnector: this.reconnector,
      log: this.log,
      setStatus: (status) => this.statusEmitter.set(status),
      retry: () => {
        this.connectInternal().catch((err: unknown) => {
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
