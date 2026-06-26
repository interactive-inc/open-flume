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
      interval: this.options.pollInterval ?? 60,
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
