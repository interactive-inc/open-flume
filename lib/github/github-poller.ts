import type { FlumeGitHubNotification, FlumeLogHandler, FlumeRuntimeDeps, FlumeTimerHandle } from "@/types"
import { FlumeGitHubNotificationSchema } from "@/github/github-notification-schema"
import { FlumeLogger } from "@/logger"
import { safeFetch } from "@/utils/safe-fetch"
import { FlumeGitHubSeenCache } from "@/github/github-seen-cache"

type Deps = Pick<FlumeRuntimeDeps, "fetch" | "setInterval" | "clearInterval" | "now">

type Props = {
  token: string
  interval: number
  onNotifications: (notifications: FlumeGitHubNotification[]) => void
  onConnected: () => void
  onDisconnected: (detail: string) => void
  onLog?: FlumeLogHandler
  deps: Deps
}

export class FlumeGitHubPoller {

  private readonly log: FlumeLogger

  private readonly cache = new FlumeGitHubSeenCache({ maxSize: 5000 })

  private timer: FlumeTimerHandle | null = null

  private since: string | null = null

  private bootstrapped = false

  stopped = false

  private consecutiveErrors = 0

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({ source: "github.poller", handler: props.onLog, deps: props.deps })
  }

  async start(): Promise<void> {
    this.stopped = false
    this.log.info({ action: "start", message: `polling every ${this.props.interval}s` })

    await this.poll()

    this.timer = this.props.deps.setInterval(() => {
      this.poll().catch((err) => {
        this.log.error({
          action: "poll.unhandled",
          message: "unexpected error in poll loop",
          error: err instanceof Error ? err : new Error(String(err)),
        })
      })
    }, this.props.interval * 1000)
  }

  stop(): void {
    this.log.info({ action: "stop", message: "stopping poller" })
    this.stopped = true

    if (this.timer !== null) {
      this.props.deps.clearInterval(this.timer)
      this.timer = null
    }
  }

  private async poll(): Promise<void> {
    const params = new URLSearchParams({ all: "false" })
    if (this.since) params.set("since", this.since)

    const url = `https://api.github.com/notifications?${params}`
    this.log.debug({ action: "http.request", message: `GET ${url}` })

    const response = await this.safeFetch(url)
    if (response instanceof Error) return

    this.log.debug({
      action: "http.response",
      message: `GET ${response.status}`,
      detail: { status: response.status, url },
    })

    if (!response.ok) {
      this.consecutiveErrors++
      this.log.error({ action: "http.error", message: `HTTP ${response.status} (consecutive=${this.consecutiveErrors})` })
      if (this.consecutiveErrors >= 3) this.props.onDisconnected(`HTTP ${response.status}`)
      return
    }

    this.consecutiveErrors = 0

    const body: unknown = await response.json()
    if (!Array.isArray(body)) {
      this.log.warn({
        action: "http.body",
        message: "response body is not an array, dropping",
        detail: { bodyType: typeof body },
      })
      return
    }

    this.processNotifications(body)
  }

  private processNotifications(raw: unknown[]): void {
    let dropped = 0
    const notifications = raw.flatMap((item) => {
      const parsed = FlumeGitHubNotificationSchema.safeParse(item)
      if (!parsed.success) {
        dropped++
        this.log.warn({
          action: "parse.skip",
          message: "notification did not match schema",
          detail: { issues: parsed.error.issues.map((i) => ({ path: i.path, message: i.message })) },
        })
        return []
      }
      return [parsed.data]
    })

    if (dropped > 0) {
      this.log.warn({ action: "parse.summary", message: `${dropped}/${raw.length} notifications dropped by schema` })
    }

    if (!this.bootstrapped) {
      this.bootstrapped = true
      for (const notification of notifications) {
        this.cache.add(notification.id, notification.updated_at)
      }
      this.since = new Date(this.props.deps.now()).toISOString()
      this.log.info({ action: "bootstrap", message: `seeded ${notifications.length} existing notifications` })
      this.props.onConnected()
      return
    }

    const fresh: FlumeGitHubNotification[] = []

    for (const notification of notifications) {
      if (this.cache.has(notification.id, notification.updated_at)) continue
      this.cache.add(notification.id, notification.updated_at)
      fresh.push(notification)
    }

    this.cache.trim()

    this.since = new Date(this.props.deps.now()).toISOString()

    if (fresh.length > 0) {
      this.log.info({ action: "poll.fresh", message: `${fresh.length} new notifications` })
      this.props.onNotifications(fresh)
    } else {
      this.log.debug({ action: "poll.idle", message: "0 new notifications" })
    }
  }

  private async safeFetch(url: string): Promise<Response | Error> {
    const result = await safeFetch({
      fetch: this.props.deps.fetch,
      url,
      init: {
        headers: {
          Authorization: `Bearer ${this.props.token}`,
          Accept: "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
        },
      },
      log: this.log,
    })

    if (result instanceof Error) {
      this.consecutiveErrors++
      this.log.warn({ action: "http.error", message: `consecutive=${this.consecutiveErrors}` })
      if (this.consecutiveErrors >= 3) this.props.onDisconnected("network error")
    }

    return result
  }
}
