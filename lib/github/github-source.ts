import type { FlumeEvent, FlumeGitHubNotification, FlumeGitHubSourceOptions, FlumeHandler, FlumeRuntimeDeps, FlumeStatus } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeGitHubPoller } from "@/github/github-poller"

export class FlumeGitHubSource {

  private poller: FlumeGitHubPoller | null = null

  private currentStatus: FlumeStatus = "disconnected"

  private readonly log: FlumeLogger

  private readonly deps: FlumeRuntimeDeps

  constructor(private readonly options: FlumeGitHubSourceOptions) {
    this.deps = options.deps
    this.log = new FlumeLogger({ source: "github", handler: options.onLog, deps: this.deps })
  }

  async start(handler: FlumeHandler): Promise<void> {
    if (this.options.signal?.aborted) return

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
    }
  }

  async stop(): Promise<void> {
    this.log.info({ action: "stop", message: "stopping GitHub source" })
    this.poller?.stop()
    this.poller = null
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
        meta: FlumeGitHubSource.extractMeta(notification),
        receivedAt: this.deps.now(),
      }
      try {
        handler(event)
      } catch (err) {
        this.log.error({
          action: "handler.error",
          message: "user handler threw",
          error: err instanceof Error ? err : new Error(String(err)),
        })
      }
    }
  }

  private setStatus(next: FlumeStatus, detail?: string): void {
    if (this.currentStatus === next) return

    this.log.info({ action: "status", message: `${this.currentStatus} → ${next}${detail ? ` (${detail})` : ""}` })
    this.currentStatus = next
    this.options.onStatus?.(next, detail)
  }

  static extractMeta(notification: FlumeGitHubNotification): Record<string, string> {
    return {
      event_type: "notification",
      reason: notification.reason,
      subject_type: notification.subject.type,
      repository: notification.repository.full_name,
      thread_id: notification.id,
    }
  }
}
