import type {
  FlumeGitHubNotification,
  FlumeGitHubSourceOptions,
  FlumeSourceStartContext,
} from "@/types"
import { flumeExtractGitHubMeta } from "@/github/extract-github-meta"
import { FlumeGitHubPoller } from "@/github/github-poller"
import { FlumeSource } from "@/flume-source"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"

const DEFAULT_POLL_INTERVAL_SEC = 60
const MAX_POLL_INTERVAL_SEC = Math.floor(2_147_483_647 / 1000)

export class FlumeGitHubSource extends FlumeSource {
  readonly name = "github" as const

  private poller: FlumeGitHubPoller | null = null

  constructor(private readonly options: FlumeGitHubSourceOptions) {
    super()
  }

  protected async connect(ctx: FlumeSourceStartContext): Promise<Error | null> {
    this.setStatus("connecting")

    this.poller = new FlumeGitHubPoller({
      token: this.options.token,
      interval: this.getPollIntervalSec(),
      onLog: ctx.log.handler,
      deps: ctx.deps,
      onNotifications: (notifications) => this.handleNotifications(ctx, notifications),
      onConnected: () => this.setStatus("connected"),
      onDisconnected: (detail) => this.setStatus("disconnected", detail),
    })

    const result = await this.poller.start()

    if (result instanceof Error) {
      ctx.log.error({
        action: "source.start.failed",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      this.setStatus("disconnected", result.message)
      return result
    }

    return null
  }

  protected disconnect(): void {
    this.poller?.stop()
    this.poller = null
  }

  /**
   * pollInterval が非数値・非有限・0 以下の場合は既定値へフォールバックする
   */
  private getPollIntervalSec(): number {
    const requested = this.options.pollInterval
    if (typeof requested !== "number") return DEFAULT_POLL_INTERVAL_SEC
    if (!Number.isFinite(requested)) return DEFAULT_POLL_INTERVAL_SEC
    if (requested <= 0) return DEFAULT_POLL_INTERVAL_SEC
    return Math.min(requested, MAX_POLL_INTERVAL_SEC)
  }

  private handleNotifications(
    ctx: FlumeSourceStartContext,
    notifications: FlumeGitHubNotification[],
  ): void {
    for (const notification of notifications) {
      this.emit({
        source: "github",
        type: "notification",
        data: notification,
        meta: this.safeExtractMeta(ctx, notification),
        receivedAt: safeNow({ deps: ctx.deps }),
      })
    }
  }

  private safeExtractMeta(
    ctx: FlumeSourceStartContext,
    notification: FlumeGitHubNotification,
  ): Record<string, string> {
    const result = attempt(() => flumeExtractGitHubMeta(notification))
    if (result instanceof Error) {
      const error = safeNormalizeError({ value: result })
      ctx.log.warn({
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
