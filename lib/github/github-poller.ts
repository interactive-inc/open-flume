import type {
  FlumeGitHubNotification,
  FlumeLogHandler,
  FlumeRuntimeDeps,
  FlumeTimerHandle,
} from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeGitHubNotificationSchema } from "@/github/github-notification-schema"
import { FlumeGitHubSeenCache } from "@/github/github-seen-cache"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeJsonParse } from "@/utils/safe-json-parse"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeNow } from "@/utils/safe-now"
import { safeReadText } from "@/utils/safe-read-text"

type Deps = Pick<
  FlumeRuntimeDeps,
  "fetch" | "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout" | "now"
>

type Props = {
  token: string
  interval: number
  onNotifications: (notifications: FlumeGitHubNotification[]) => void
  onConnected: () => void
  onDisconnected: (detail: string) => void
  onLog?: FlumeLogHandler
  deps: Deps
}

const NOTIFICATIONS_URL = "https://api.github.com/notifications"
const CONSECUTIVE_ERRORS_TO_DISCONNECT = 3
const SEEN_CACHE_MAX = 5000

/**
 * GitHub /notifications を条件付きポーリングする (ETag / Last-Modified)。
 * 304 / X-Poll-Interval / レート制限を尊重し、stop() / abort() で in-flight 通信を打ち切る
 */
export class FlumeGitHubPoller {
  private readonly log: FlumeLogger

  private readonly cache = new FlumeGitHubSeenCache({ maxSize: SEEN_CACHE_MAX })

  private timer: FlumeTimerHandle | null = null

  private rateLimitTimer: FlumeTimerHandle | null = null

  private since: string | null = null

  private etag: string | null = null

  private lastModified: string | null = null

  private bootstrapped = false

  private isStoppedFlag = false

  private inFlight = false

  private consecutiveErrors = 0

  private effectiveIntervalSec: number

  private controller: AbortController | null = null

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({ source: "github.poller", handler: props.onLog, deps: props.deps })
    this.effectiveIntervalSec = props.interval
  }

  get isStopped(): boolean {
    return this.isStoppedFlag
  }

  async start(): Promise<Error | null> {
    this.isStoppedFlag = false

    const controllerResult = attempt(() => new AbortController())
    if (controllerResult instanceof Error) {
      this.log.error({
        action: "github.abort-controller.new.error",
        message: safeErrorMessage({ error: controllerResult }),
        error: controllerResult,
      })
      this.controller = null
    } else {
      this.controller = controllerResult
    }

    this.log.info({
      action: "poller.start",
      message: `polling every ${this.effectiveIntervalSec}s`,
    })

    const error = await this.poll()
    if (error) return error
    if (this.isStoppedFlag) return null

    this.scheduleInterval()
    return null
  }

  stop(): void {
    this.log.info({ action: "poller.stop", message: "stopping poller" })
    this.isStoppedFlag = true
    this.controller?.abort()
    this.controller = null

    this.clearTimer()
    this.clearRateLimitTimer()
  }

  private scheduleInterval(): void {
    this.clearTimer()

    const intervalResult = attempt(() =>
      this.props.deps.setInterval(() => {
        this.poll()
          .catch((err) => {
            const error = safeNormalizeError({ value: err })
            this.log.error({
              action: "poll.unhandled",
              message: safeErrorMessage({ error }),
              error,
            })
          })
          .catch(() => {})
      }, this.effectiveIntervalSec * 1000),
    )
    if (intervalResult instanceof Error) {
      this.log.error({
        action: "poller.interval.schedule.error",
        message: safeErrorMessage({ error: intervalResult }),
        error: intervalResult,
      })
      this.timer = null
      if (!this.isStoppedFlag) {
        this.props.onDisconnected("interval scheduling rejected by runtime")
      }
      return
    }
    this.timer = intervalResult
  }

  private async poll(): Promise<Error | null> {
    if (this.inFlight || this.isStoppedFlag) return null
    this.inFlight = true

    try {
      return await this.pollOnce()
    } catch (err) {
      const cause = safeNormalizeError({ value: err })
      const error = new FlumeHttpError({
        message: `poll loop threw: ${safeErrorMessage({ error: cause })}`,
        status: 0,
        cause,
      })
      this.log.error({
        action: "poll.unhandled",
        message: safeErrorMessage({ error }),
        error,
      })
      if (!this.bootstrapped) return error
      return null
    } finally {
      this.inFlight = false
    }
  }

  private async pollOnce(): Promise<Error | null> {
    const params = new URLSearchParams({ all: "false", per_page: "50" })
    if (this.since) params.set("since", this.since)

    const url = `${NOTIFICATIONS_URL}?${params}`
    this.log.debug({ action: "http.request", message: `GET ${url}` })

    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.props.token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    }
    if (this.etag) headers["If-None-Match"] = this.etag
    if (this.lastModified) headers["If-Modified-Since"] = this.lastModified

    const response = await attempt(() =>
      this.props.deps.fetch(url, { headers, signal: this.controller?.signal }),
    )

    if (this.isStoppedFlag) return null

    if (response instanceof Error) {
      this.log.error({
        action: "http.error",
        message: safeErrorMessage({ error: response }),
        error: response,
      })
      return this.recordFailure({ kind: "network", message: response.message, cause: response })
    }

    this.log.debug({
      action: "http.response",
      message: `GET ${response.status}`,
      detail: { status: response.status, url },
    })

    this.maybeWidenInterval(response.headers.get("X-Poll-Interval"))

    if (response.status === 304) {
      this.consecutiveErrors = 0
      this.log.debug({ action: "poll.not-modified", message: "304 Not Modified" })
      return null
    }

    if (this.isRateLimited(response)) {
      this.handleRateLimit(response)
      return null
    }

    if (!response.ok) {
      const error = new FlumeHttpError({
        message: `HTTP ${response.status}`,
        status: response.status,
      })
      return this.recordFailure({ kind: "http", message: safeErrorMessage({ error }), error })
    }

    this.consecutiveErrors = 0
    this.etag = response.headers.get("ETag")
    this.lastModified = response.headers.get("Last-Modified")

    const text = await safeReadText({ response, context: "notifications" })
    if (this.isStoppedFlag) return null
    if (text instanceof FlumeHttpError) {
      this.log.warn({
        action: "http.body.read",
        message: safeErrorMessage({ error: text }),
        error: text,
      })
      return this.recordFailure({ kind: "http", message: text.message, error: text })
    }

    const json = safeJsonParse(text)

    if (json instanceof FlumeParseError) {
      this.log.warn({ action: "http.body.parse", message: json.message, error: json })
      return null
    }

    if (!Array.isArray(json)) {
      this.log.warn({
        action: "http.body.shape",
        message: "response body is not an array, dropping",
        detail: { bodyType: typeof json },
      })
      return null
    }

    this.processNotifications(json)
    return null
  }

  private recordFailure(input: {
    kind: "network" | "http"
    message: string
    error?: Error
    cause?: unknown
  }): Error | null {
    this.consecutiveErrors++
    const error =
      input.error ?? new FlumeHttpError({ message: input.message, status: 0, cause: input.cause })
    this.log.error({
      action: "http.error",
      message: input.message,
      error,
      detail: { consecutiveErrors: this.consecutiveErrors },
    })

    if (this.consecutiveErrors >= CONSECUTIVE_ERRORS_TO_DISCONNECT && !this.isStoppedFlag) {
      this.props.onDisconnected(input.kind === "network" ? "network error" : input.message)
    }

    if (!this.bootstrapped) return error
    return null
  }

  private isRateLimited(response: Response): boolean {
    if (response.status === 429) return true
    if (response.status === 403 && response.headers.get("X-RateLimit-Remaining") === "0")
      return true
    return false
  }

  private handleRateLimit(response: Response): void {
    const retryAfter = response.headers.get("Retry-After")
    const reset = response.headers.get("X-RateLimit-Reset")
    const nowSec = Math.floor(safeNow({ deps: this.props.deps }) / 1000)

    const retryAfterSec = retryAfter !== null ? Number.parseInt(retryAfter, 10) : NaN
    const resetSec = reset !== null ? Number.parseInt(reset, 10) - nowSec : NaN

    const delaySec =
      Number.isFinite(retryAfterSec) && retryAfterSec > 0
        ? retryAfterSec
        : Number.isFinite(resetSec) && resetSec > 0
          ? resetSec
          : 60

    this.log.warn({
      action: "rate.limit",
      message: `rate limited, pausing for ${delaySec}s`,
      detail: { status: response.status, delaySec },
    })

    this.clearTimer()
    this.clearRateLimitTimer()

    const timerResult = attempt(() =>
      this.props.deps.setTimeout(() => {
        this.rateLimitTimer = null
        if (this.isStoppedFlag) return
        this.scheduleInterval()
      }, delaySec * 1000),
    )
    if (timerResult instanceof Error) {
      this.log.error({
        action: "poller.rate-limit.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.rateLimitTimer = null
    } else {
      this.rateLimitTimer = timerResult
    }
  }

  private clearTimer(): void {
    if (this.timer === null) return
    const handle = this.timer
    const result = attempt(() => this.props.deps.clearInterval(handle))
    if (result instanceof Error) {
      this.log.error({
        action: "poller.timer.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.timer = null
  }

  private clearRateLimitTimer(): void {
    if (this.rateLimitTimer === null) return
    const handle = this.rateLimitTimer
    const result = attempt(() => this.props.deps.clearTimeout(handle))
    if (result instanceof Error) {
      this.log.error({
        action: "poller.rate-limit.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.rateLimitTimer = null
  }

  private maybeWidenInterval(headerValue: string | null): void {
    if (headerValue === null) return
    const required = Number.parseInt(headerValue, 10)
    if (!Number.isFinite(required) || required <= this.effectiveIntervalSec) return

    this.log.info({
      action: "poll.widen-interval",
      message: `widening interval ${this.effectiveIntervalSec}s -> ${required}s per X-Poll-Interval`,
      detail: { from: this.effectiveIntervalSec, to: required },
    })

    this.effectiveIntervalSec = required
    if (this.timer !== null) this.scheduleInterval()
  }

  private processNotifications(raw: unknown[]): void {
    const parsedResults = raw.map((item) => FlumeGitHubNotificationSchema.safeParse(item))

    for (const result of parsedResults) {
      if (result.success) continue
      this.log.warn({
        action: "parse.skip",
        message: "notification did not match schema",
        detail: { issues: result.error.issues.map((i) => ({ path: i.path, message: i.message })) },
      })
    }

    const notifications = parsedResults.flatMap((r) => (r.success ? [r.data] : []))
    const dropped = raw.length - notifications.length

    if (dropped > 0) {
      this.log.warn({
        action: "parse.summary",
        message: `${dropped}/${raw.length} notifications dropped by schema`,
      })
    }

    if (!this.bootstrapped) {
      this.bootstrapped = true
      for (const notification of notifications) {
        this.cache.add(notification.id, notification.updated_at)
      }
      this.advanceCursor()
      this.log.info({
        action: "poller.bootstrap",
        message: `seeded ${notifications.length} existing notifications`,
      })
      this.props.onConnected()
      return
    }

    const fresh = notifications.filter((notification) => {
      if (this.cache.has(notification.id, notification.updated_at)) return false
      this.cache.add(notification.id, notification.updated_at)
      return true
    })

    this.cache.trim()
    this.advanceCursor()

    if (fresh.length > 0) {
      this.log.info({
        action: "poll.fresh",
        message: `${fresh.length} new notifications`,
        detail: { count: fresh.length },
      })
      this.props.onNotifications(fresh)
      return
    }

    this.log.debug({ action: "poll.idle", message: "0 new notifications" })
  }

  private advanceCursor(): void {
    if (this.lastModified !== null) {
      const parsed = new Date(this.lastModified)
      if (!Number.isNaN(parsed.getTime())) {
        const iso = attempt(() => parsed.toISOString())
        if (!(iso instanceof Error)) {
          this.since = iso
          return
        }
      }
    }

    const nowMs = safeNow({ deps: this.props.deps })
    if (!Number.isFinite(nowMs)) {
      this.log.warn({
        action: "cursor.advance.skip",
        message: "deps.now() returned non-finite, leaving since cursor unchanged",
      })
      return
    }

    const iso = attempt(() => new Date(nowMs).toISOString())
    if (iso instanceof Error) {
      const error = safeNormalizeError({ value: iso })
      this.log.warn({
        action: "cursor.advance.skip",
        message: safeErrorMessage({ error }),
        error,
      })
      return
    }
    this.since = iso
  }
}
