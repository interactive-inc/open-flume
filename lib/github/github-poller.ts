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
const MAX_PAGES = 1000
const MAX_TIMER_DELAY_MS = 2_147_483_647

/**
 * GitHub /notifications を条件付きポーリングする (ETag / Last-Modified)。
 * Link: rel="next" を MAX_PAGES まで辿って全ページを集約し、
 * 304 / X-Poll-Interval / レート制限を尊重し、stop() / abort() で in-flight 通信を打ち切る
 */
export class FlumeGitHubPoller {
  private readonly log: FlumeLogger

  private readonly cache = new FlumeGitHubSeenCache({ maxSize: SEEN_CACHE_MAX })

  private timer: FlumeTimerHandle | null = null

  private rateLimitTimer: FlumeTimerHandle | null = null

  private rateLimitPauseActive = false

  private since: string | null = null

  private etag: string | null = null

  private lastModified: string | null = null

  private bootstrapped = false

  private isStoppedFlag = false

  private inFlight = false

  private consecutiveErrors = 0

  private degraded = false

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

    // 初回 poll が rate-limit された場合は handleRateLimit が再開タイマーを握っている。
    // ここで interval を張ると一時停止を打ち消すのでスキップする
    if (this.rateLimitPauseActive) return null

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
    this.rateLimitPauseActive = false
  }

  private scheduleInterval(): void {
    this.clearTimer()

    const intervalResult = attempt(() =>
      this.props.deps.setInterval(
        () => {
          // poll() は内部で attempt 済みで reject しない。将来の変更への保険として log + 握り潰す
          this.poll().catch((err) => {
            const error = safeNormalizeError({ value: err })
            this.log.error({
              action: "poll.unhandled",
              message: safeErrorMessage({ error }),
              error,
            })
          })
        },
        Math.min(this.effectiveIntervalSec * 1000, MAX_TIMER_DELAY_MS),
      ),
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

    // 正常な Error 戻り値と unexpected throw を区別できるよう成功値を object で包む
    const attempted = await attempt(async () => ({ result: await this.pollOnce() }))
    this.inFlight = false

    if (attempted instanceof Error) {
      const cause = safeNormalizeError({ value: attempted })
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
    }

    return attempted.result
  }

  private async pollOnce(): Promise<Error | null> {
    const params = new URLSearchParams({ all: "false", per_page: "50" })
    if (this.since) params.set("since", this.since)

    const response = await this.fetchPage(`${NOTIFICATIONS_URL}?${params}`, true)

    if (this.isStoppedFlag) return null

    if (response instanceof Error) {
      return this.recordFailure({ kind: "network", message: response.message, cause: response })
    }

    this.followPollIntervalHeader(response.headers.get("X-Poll-Interval"))

    if (response.status === 304) {
      this.consecutiveErrors = 0
      this.log.debug({ action: "poll.not-modified", message: "304 Not Modified" })
      this.notifyRecoveredIfDegraded()
      return null
    }

    if (await this.isRateLimited(response)) {
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

    // 検証子は全ページの body 処理が成功するまでコミットしない。
    // 途中で失敗した場合に未処理コンテンツの ETag で 304 を引いて通知を取り零すのを防ぐ
    const nextEtag = response.headers.get("ETag")
    const nextLastModified = response.headers.get("Last-Modified")

    const rawNotifications = await this.collectPages(response)
    if (!Array.isArray(rawNotifications)) return rawNotifications

    this.processNotifications(rawNotifications)

    this.consecutiveErrors = 0
    this.etag = nextEtag
    this.lastModified = nextLastModified
    this.advanceCursor()
    this.notifyRecoveredIfDegraded()
    return null
  }

  /**
   * 先頭ページの body を読み、Link: rel="next" を MAX_PAGES まで辿って
   * 全ページの生 notifications を集約する。2 ページ目以降は無条件リクエスト
   * (If-None-Match / If-Modified-Since なし)。失敗・停止・rate limit 時は
   * Error | null を返し pollOnce がそのまま返す
   */
  private async collectPages(firstResponse: Response): Promise<unknown[] | Error | null> {
    const rawNotifications: unknown[] = []

    let pageResponse = firstResponse

    let pageCount = 1

    while (true) {
      const body = await this.readPageBody(pageResponse)
      if (this.isStoppedFlag) return null
      if (body instanceof Error) {
        return this.recordFailure({ kind: "http", message: body.message, error: body })
      }
      if (body === null) return null

      for (const item of body) rawNotifications.push(item)

      const nextUrl = this.findNextPageUrl(pageResponse.headers.get("Link"))
      if (nextUrl === null) return rawNotifications

      if (pageCount >= MAX_PAGES) {
        const error = new FlumeHttpError({
          message: `pagination exceeded the safety limit of ${MAX_PAGES} pages`,
          status: 0,
        })
        return this.recordFailure({ kind: "http", message: error.message, error })
      }

      const nextResponse = await this.fetchPage(nextUrl, false)
      if (this.isStoppedFlag) return null
      if (nextResponse instanceof Error) {
        return this.recordFailure({
          kind: "network",
          message: nextResponse.message,
          cause: nextResponse,
        })
      }
      if (await this.isRateLimited(nextResponse)) {
        this.handleRateLimit(nextResponse)
        return null
      }
      if (!nextResponse.ok) {
        const error = new FlumeHttpError({
          message: `HTTP ${nextResponse.status}`,
          status: nextResponse.status,
        })
        return this.recordFailure({ kind: "http", message: safeErrorMessage({ error }), error })
      }

      pageResponse = nextResponse
      pageCount++
    }
  }

  /**
   * 1 ページ分を fetch する。条件付きヘッダ (If-None-Match / If-Modified-Since) は
   * 先頭ページのみ付与する
   */
  private async fetchPage(url: string, isFirstPage: boolean): Promise<Response | Error> {
    this.log.debug({ action: "http.request", message: `GET ${url}` })

    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.props.token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    }

    if (isFirstPage && this.etag) headers["If-None-Match"] = this.etag

    if (isFirstPage && this.lastModified) headers["If-Modified-Since"] = this.lastModified

    const response = await attempt(() =>
      this.props.deps.fetch(url, { headers, signal: this.controller?.signal }),
    )

    if (response instanceof Error) {
      this.log.error({
        action: "http.error",
        message: safeErrorMessage({ error: response }),
        error: response,
      })
      return response
    }

    this.log.debug({
      action: "http.response",
      message: `GET ${response.status}`,
      detail: { status: response.status, url },
    })

    return response
  }

  /**
   * body を読んで JSON 配列に解決する。読み取り失敗は FlumeHttpError (呼び出し側で
   * recordFailure する)、JSON 破損・非配列は warn 済みの null (poll ごと破棄) を返す
   */
  private async readPageBody(response: Response): Promise<unknown[] | Error | null> {
    const text = await safeReadText({ response, context: "notifications" })

    if (text instanceof FlumeHttpError) {
      this.log.warn({
        action: "http.body.read",
        message: safeErrorMessage({ error: text }),
        error: text,
      })
      return text
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

    return json
  }

  private findNextPageUrl(linkHeader: string | null): string | null {
    if (linkHeader === null) return null

    for (const part of linkHeader.split(",")) {
      if (!part.includes('rel="next"')) continue
      const match = part.match(/<([^>]+)>/)
      const url = match?.[1]
      if (url !== undefined) return url
    }

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
      this.degraded = true
      this.props.onDisconnected(input.kind === "network" ? "network error" : input.message)
    }

    if (!this.bootstrapped) return error
    return null
  }

  /**
   * onDisconnected 通知後に poll が完全成功したら connected へ戻す。
   * degraded でなければ何もしない (冪等)
   */
  private notifyRecoveredIfDegraded(): void {
    if (!this.degraded) return

    this.degraded = false
    this.props.onConnected()
  }

  /**
   * 429 は常に rate limit。403 は Retry-After 付き、X-RateLimit-Remaining が 0、
   * または本文が secondary rate limit / abuse detection を示す場合
   */
  private async isRateLimited(response: Response): Promise<boolean> {
    if (response.status === 429) return true
    if (response.status !== 403) return false
    if (response.headers.get("Retry-After") !== null) return true
    if (response.headers.get("X-RateLimit-Remaining") === "0") return true

    const cloned = attempt(() => response.clone())
    if (cloned instanceof Error) return false
    const body = await safeReadText({ response: cloned, context: "rate limit response" })
    if (body instanceof Error) return false

    const normalized = body.toLowerCase()
    return normalized.includes("secondary rate limit") || normalized.includes("abuse detection")
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
    this.rateLimitPauseActive = true

    const timerResult = attempt(() =>
      this.props.deps.setTimeout(
        () => {
          this.rateLimitTimer = null
          this.rateLimitPauseActive = false
          if (this.isStoppedFlag) return

          // 一時停止明けは interval 1 周分をさらに待たず、すぐ 1 回 poll する
          this.scheduleInterval()
          this.poll().catch((err) => {
            const error = safeNormalizeError({ value: err })
            this.log.error({
              action: "poll.unhandled",
              message: safeErrorMessage({ error }),
              error,
            })
          })
        },
        Math.min(delaySec * 1000, MAX_TIMER_DELAY_MS),
      ),
    )
    if (timerResult instanceof Error) {
      this.log.error({
        action: "poller.rate-limit.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.rateLimitTimer = null
      if (!this.isStoppedFlag) {
        this.props.onDisconnected("rate-limit timer scheduling rejected by runtime")
      }
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

  /**
   * X-Poll-Interval を双方向に追従する。下限はユーザー指定 interval。
   * ヘッダ欠落・非数値は現在の実効値を維持する
   */
  private followPollIntervalHeader(headerValue: string | null): void {
    if (headerValue === null) return

    const headerSec = Number.parseInt(headerValue, 10)
    if (!Number.isFinite(headerSec)) return

    const nextSec = Math.min(
      Math.max(this.props.interval, headerSec),
      Math.floor(MAX_TIMER_DELAY_MS / 1000),
    )
    if (nextSec === this.effectiveIntervalSec) return

    this.log.info({
      action: "poll.interval.follow",
      message: `adjusting interval ${this.effectiveIntervalSec}s -> ${nextSec}s per X-Poll-Interval`,
      detail: { from: this.effectiveIntervalSec, to: nextSec },
    })

    this.effectiveIntervalSec = nextSec
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
      this.log.info({
        action: "poller.bootstrap",
        message: `seeded ${notifications.length} existing notifications`,
      })
      this.degraded = false
      this.props.onConnected()
      return
    }

    const fresh = notifications.filter((notification) => {
      if (this.cache.has(notification.id, notification.updated_at)) return false
      this.cache.add(notification.id, notification.updated_at)
      return true
    })

    this.cache.trim()

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
