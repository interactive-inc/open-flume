import type { FlumeEvent, FlumeGitHubNotification, FlumeGitHubSourceOptions, FlumeHandler, FlumeRuntimeDeps, FlumeStatus } from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeStartError } from "@/errors/start-error"
import { FlumeLogger } from "@/logger"
import { FlumeSerialQueue } from "@/utils/serial-queue"
import { extractGitHubMeta } from "@/github/extract-github-meta"
import { FlumeGitHubPoller } from "@/github/github-poller"

export class FlumeGitHubSource {

  readonly name = "github" as const

  private poller: FlumeGitHubPoller | null = null

  private currentStatus: FlumeStatus = "disconnected"

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  private readonly queue = new FlumeSerialQueue()

  constructor(private readonly options: FlumeGitHubSourceOptions) {
    this.deps = options.deps ?? createFlumeDefaultDeps()
    this.log = new FlumeLogger({ source: "github", handler: options.onLog, deps: this.deps })
  }

  async start(handler: FlumeHandler): Promise<Error | null> {
    if (this.options.signal?.aborted) {
      return new FlumeStartError("GitHub source: signal already aborted")
    }

    this.options.signal?.addEventListener("abort", () => this.stop(), { once: true })

    this.log.info({ action: "start", message: "starting GitHub source" })
    this.setStatus("connecting")

    this.poller = new FlumeGitHubPoller({
      token: this.options.token,
      interval: this.options.pollInterval ?? 60,
      onLog: this.options.onLog,
      deps: this.deps,
      onNotifications: (notifications) => this.handleNotifications(handler, notifications),
      onConnected: () => this.setStatus("connected"),
      onDisconnected: (detail) => this.setStatus("disconnected", detail),
    })

    try {
      await this.poller.start()
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      this.log.error({ action: "start.failed", message: err.message, error: err })
      this.setStatus("disconnected")
      return err
    }

    return null
  }

  async stop(): Promise<void> {
    this.log.info({ action: "stop", message: "stopping GitHub source" })
    this.poller?.stop()
    this.poller = null
    await this.queue.drain()
    this.setStatus("disconnected")
  }

  status(): FlumeStatus {
    return this.currentStatus
  }

  private handleNotifications(handler: FlumeHandler, notifications: FlumeGitHubNotification[]): void {
    for (const notification of notifications) {
      const event: FlumeEvent = {
        source: "github",
        type: "notification",
        data: notification,
        meta: extractGitHubMeta(notification),
        receivedAt: this.deps.now(),
      }
      this.queue.add(async () => {
        try {
          await handler(event)
        } catch (err) {
          this.log.error({
            action: "handler.error",
            message: "user handler threw",
            error: err instanceof Error ? err : new Error(String(err)),
          })
        }
      })
    }
  }

  private setStatus(next: FlumeStatus, detail?: string): void {
    if (this.currentStatus === next) return

    this.log.info({ action: "status", message: `${this.currentStatus} → ${next}${detail ? ` (${detail})` : ""}` })
    this.currentStatus = next
    this.options.onStatus?.(next, detail)
  }

}
