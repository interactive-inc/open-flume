import type {
  FlumeEvent,
  FlumeGitHubNotification,
  FlumeGitHubSourceOptions,
  FlumeHandler,
  FlumeRuntimeDeps,
  FlumeSourceStartOptions,
  FlumeStatus,
} from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeLogger } from "@/logger"
import { flumeExtractGitHubMeta } from "@/github/extract-github-meta"
import { FlumeGitHubPoller } from "@/github/github-poller"
import { FlumeSignalRegistry } from "@/source-helpers/flume-signal-registry"
import { FlumeStatusEmitter } from "@/source-helpers/flume-status-emitter"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"
import { FlumeSerialQueue } from "@/utils/serial-queue"

export class FlumeGitHubSource {
  readonly name = "github" as const

  private poller: FlumeGitHubPoller | null = null

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

  constructor(private readonly options: FlumeGitHubSourceOptions) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "github", handler: options.onLog, deps: this.deps })
    this.signals = new FlumeSignalRegistry({ log: this.log, onAbort: this.onSignalAbort })
    this.statusEmitter = new FlumeStatusEmitter({ log: this.log, onStatus: options.onStatus })
  }

  async start(handler: FlumeHandler, options?: FlumeSourceStartOptions): Promise<Error | null> {
    if (
      this.signals.isAnyAborted(this.options.signal) ||
      this.signals.isAnyAborted(options?.signal)
    ) {
      return new FlumeStartError("GitHub source: signal already aborted")
    }

    this.signals.register(this.options.signal)
    this.signals.register(options?.signal)

    this.handler = handler
    this.log.info({ action: "source.start", message: "starting GitHub source" })
    this.statusEmitter.set("connecting")

    this.poller = new FlumeGitHubPoller({
      token: this.options.token,
      interval: this.options.pollInterval ?? 60,
      onLog: this.options.onLog,
      deps: this.deps,
      onNotifications: (notifications) => this.handleNotifications(notifications),
      onConnected: () => this.statusEmitter.set("connected"),
      onDisconnected: (detail) => this.statusEmitter.set("disconnected", detail),
    })

    const result = await this.poller.start()

    if (result instanceof Error) {
      this.log.error({
        action: "source.start.failed",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      this.statusEmitter.set("disconnected", result.message)
      return result
    }

    return null
  }

  async stop(): Promise<void> {
    this.signals.unregisterAll()
    this.log.info({ action: "source.stop", message: "stopping GitHub source" })
    this.poller?.stop()
    await this.queue.drain()
    this.poller = null
    this.handler = null
    this.statusEmitter.set("disconnected")
  }

  status(): FlumeStatus {
    return this.statusEmitter.value
  }

  private handleNotifications(notifications: FlumeGitHubNotification[]): void {
    const handler = this.handler
    if (!handler) return

    for (const notification of notifications) {
      this.queue.add(async () => {
        const event: FlumeEvent = {
          source: "github",
          type: "notification",
          data: notification,
          meta: this.safeExtractMeta(notification),
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
  }

  private safeExtractMeta(notification: FlumeGitHubNotification): Record<string, string> {
    const result = attempt(() => flumeExtractGitHubMeta(notification))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      this.log.warn({
        action: "meta.extract.error",
        message: safeErrorMessage({ error }),
        error,
        detail: { notificationId: notification.id },
      })
      return { event_type: "notification" }
    }
    return result
  }
}
