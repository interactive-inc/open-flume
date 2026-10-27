import type {
  FlumeGitHubNotification,
  FlumeLogHandler,
  FlumeRuntimeDeps,
  FlumeTimerHandle,
} from "@/types"
import { FlumeTimeout } from "@/utils/timeout"
import { FlumeLogger } from "@/logger"
import {
  FlumeGitHubNotificationReader,
  type FlumeGitHubCheckpoint,
} from "@/github/github-notification-reader"
import { FlumeGitHubSeenCache } from "@/github/github-seen-cache"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeNormalizeError } from "@/utils/safe-normalize-error"

type Deps = Pick<
  FlumeRuntimeDeps,
  "fetch" | "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout" | "now"
>

type Props = {
  token: string
  interval: number
  requestTimeoutMs?: number
  onNotifications: (notifications: FlumeGitHubNotification[]) => void
  onConnected: () => void
  onDisconnected: (detail: string) => void
  onLog?: FlumeLogHandler
  deps: Deps
}

const CONSECUTIVE_ERRORS_TO_DISCONNECT = 3
const SEEN_CACHE_MAX = 5000
const MAX_TIMER_DELAY_MS = 2_147_483_647

/**
 * GitHub /notifications を条件付きポーリングする (ETag / Last-Modified)。
 * Reader が全ページの取得・検証を成功させた時だけ checkpoint を確定する。
 * 304 / X-Poll-Interval / レート制限を尊重し、stop() / abort() で in-flight 通信を打ち切る
 */
export class FlumeGitHubPoller {
  private readonly log: FlumeLogger

  private readonly cache = new FlumeGitHubSeenCache({ maxSize: SEEN_CACHE_MAX })

  private timer: FlumeTimerHandle | null = null

  private readonly rateLimitTimer: FlumeTimeout

  private intervalGeneration = 0

  private rateLimitPauseActive = false

  private checkpoint: FlumeGitHubCheckpoint = { since: null, etag: null, lastModified: null }

  private readonly reader: FlumeGitHubNotificationReader

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
    this.reader = new FlumeGitHubNotificationReader({ ...props, log: this.log })
    this.rateLimitTimer = new FlumeTimeout({
      deps: props.deps,
      log: this.log,
      action: "poller.rate-limit",
    })
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
    const generation = this.intervalGeneration

    const intervalResult = attempt(() =>
      this.props.deps.setInterval(
        () => {
          if (generation !== this.intervalGeneration || this.isStoppedFlag) return
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
    const controller = this.controller
    if (this.inFlight || this.isStoppedFlag || !controller || this.rateLimitPauseActive) return null
    this.inFlight = true
    const result = await attempt(() => this.reader.read(this.checkpoint, controller.signal))
    this.inFlight = false
    if (this.isStoppedFlag) return null
    if (result instanceof Error) return this.recordFailure(result)

    this.followPollIntervalHeader(result.pollInterval)
    if (result.kind === "rate-limit") {
      this.handleRateLimit(result.delayMs)
      return null
    }
    if (result.kind === "notifications") {
      this.checkpoint = result.checkpoint
      this.processNotifications(result.notifications)
    } else {
      this.log.debug({ action: "poll.not-modified", message: "304 Not Modified" })
    }
    this.consecutiveErrors = 0
    this.notifyRecoveredIfDegraded()
    return null
  }

  private recordFailure(error: Error): Error | null {
    this.consecutiveErrors++
    this.log.error({
      action: "http.error",
      message: safeErrorMessage({ error }),
      error,
      detail: { consecutiveErrors: this.consecutiveErrors },
    })
    if (this.consecutiveErrors >= CONSECUTIVE_ERRORS_TO_DISCONNECT && !this.degraded) {
      this.degraded = true
      this.props.onDisconnected(safeErrorMessage({ error }))
    }
    return this.bootstrapped ? null : error
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

  private handleRateLimit(delayMs: number): void {
    this.log.warn({
      action: "rate.limit",
      message: `rate limited, pausing for ${delayMs / 1000}s`,
      detail: { delaySec: delayMs / 1000 },
    })

    this.clearTimer()
    this.clearRateLimitTimer()
    this.rateLimitPauseActive = true

    const timerResult = this.rateLimitTimer.schedule(
      () => {
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
      Math.min(delayMs, MAX_TIMER_DELAY_MS),
    )
    if (timerResult instanceof Error) {
      this.log.error({
        action: "poller.rate-limit.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      if (!this.isStoppedFlag) {
        this.props.onDisconnected("rate-limit timer scheduling rejected by runtime")
      }
    }
  }

  private clearTimer(): void {
    this.intervalGeneration++
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
    this.rateLimitTimer.cancel()
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

  private processNotifications(notifications: FlumeGitHubNotification[]): void {
    if (!this.bootstrapped) {
      this.bootstrapped = true
      for (const notification of notifications) {
        this.cache.add(notification.id, notification.updated_at)
      }
      this.cache.trim()
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
}
