import { resolveFlumeErrorCode } from "@/errors/resolve-error-code"
import type { FlumeGitHubNotification, FlumeRuntimeDeps } from "@/types"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeGitHubNotificationSchema } from "@/github/github-notification-schema"
import { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
import { safeJsonParse } from "@/utils/safe-json-parse"
import { safeNow } from "@/utils/safe-now"
import { safeReadText } from "@/utils/safe-read-text"
import { withDeadline } from "@/utils/with-deadline"

export type FlumeGitHubCheckpoint = {
  etag: string | null
  lastModified: string | null
  since: string | null
}

type Result = (
  | {
      kind: "notifications"
      notifications: FlumeGitHubNotification[]
      checkpoint: FlumeGitHubCheckpoint
    }
  | { kind: "unchanged" }
  | { kind: "rate-limit"; delayMs: number }
) & { pollInterval: string | null }

type Props = {
  token: string
  requestTimeoutMs?: number
  log: FlumeLogger
  deps: Pick<FlumeRuntimeDeps, "fetch" | "now" | "setTimeout" | "clearTimeout">
}

const ENDPOINT = "https://api.github.com/notifications"
const MAX_PAGES = 1000

/** 一回の取得を全ページで成功か失敗にする。既読・cursor・稼働状態は変更しない。 */
export class FlumeGitHubNotificationReader {
  constructor(private readonly props: Props) {
    Object.freeze(this)
  }

  async read(checkpoint: FlumeGitHubCheckpoint, signal: AbortSignal): Promise<Result | Error> {
    const requested = this.props.requestTimeoutMs
    const timeoutMs =
      typeof requested === "number" && Number.isFinite(requested) && requested > 0
        ? Math.min(requested, 2_147_483_647)
        : 30_000
    const progress = { phase: "request", page: 1 }
    const result = await withDeadline({
      context: () => ({ operation: "github.notifications", ...progress }),
      signal,
      timeoutMs,
      deps: this.props.deps,
      log: this.props.log,
      run: (requestSignal) => this.readPages(checkpoint, requestSignal, progress),
    })
    if (!(result instanceof Error)) return result
    return new FlumeHttpError({
      message: result.message,
      cause: result,
      status: result instanceof FlumeHttpError ? result.status : 0,
      code: result instanceof FlumeHttpError ? result.code : null,
      retryAfterMs: result instanceof FlumeHttpError ? result.retryAfterMs : null,
      errorCode: resolveFlumeErrorCode("http.error", result),
      context: { operation: "github.notifications", ...progress },
    })
  }

  private async readPages(
    checkpoint: FlumeGitHubCheckpoint,
    signal: AbortSignal,
    progress: { phase: string; page: number },
  ): Promise<Result | Error> {
    const params = new URLSearchParams({ all: "false", per_page: "50" })
    if (checkpoint.since) params.set("since", checkpoint.since)
    let url: string | null = `${ENDPOINT}?${params}`
    let first: Response | null = null
    const visited = new Set<string>()
    const notifications: FlumeGitHubNotification[] = []

    while (url !== null) {
      if (signal.aborted) return new Error("operation aborted")
      if (visited.has(url) || visited.size >= MAX_PAGES)
        return new FlumeParseError("invalid or excessive notification pagination")
      visited.add(url)
      progress.phase = "request"
      progress.page = visited.size
      const response = await this.fetchPage(url, first === null ? checkpoint : null, signal)
      if (signal.aborted) return new Error("operation aborted")
      if (response instanceof Error) return response
      if (first === null) first = response
      const pollInterval = first.headers.get("X-Poll-Interval")
      if (response.status === 304 && visited.size === 1) return { kind: "unchanged", pollInterval }
      progress.phase = "response status"
      const rateLimited = await this.isRateLimited(response)
      if (rateLimited)
        return { kind: "rate-limit", delayMs: this.retryDelay(response), pollInterval }
      if (!response.ok)
        return new FlumeHttpError({ message: `HTTP ${response.status}`, status: response.status })

      progress.phase = "response body"
      const page = await this.readBody(response)
      if (page instanceof Error) return page
      for (const notification of page) notifications.push(notification)
      const next = this.nextPage(response.headers.get("Link"))
      if (next instanceof Error) return next
      url = next
    }
    if (first === null) return new FlumeParseError("missing first notification page")
    return {
      kind: "notifications",
      notifications,
      pollInterval: first.headers.get("X-Poll-Interval"),
      checkpoint: this.checkpoint(first, checkpoint, notifications),
    }
  }

  private async fetchPage(
    url: string,
    checkpoint: FlumeGitHubCheckpoint | null,
    signal: AbortSignal,
  ): Promise<Response | Error> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.props.token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    }
    if (checkpoint?.etag) headers["If-None-Match"] = checkpoint.etag
    if (checkpoint?.lastModified) headers["If-Modified-Since"] = checkpoint.lastModified
    this.props.log.debug({ action: "http.request", message: `GET ${url}` })
    const response = await attempt(() =>
      this.props.deps.fetch(url, { headers, signal, redirect: "error" }),
    )
    if (!(response instanceof Error))
      this.props.log.debug({
        action: "http.response",
        message: `GET ${response.status}`,
        detail: { status: response.status, url },
      })
    return response
  }

  private async readBody(response: Response): Promise<FlumeGitHubNotification[] | Error> {
    const text = await safeReadText({ response, context: "notifications" })
    if (text instanceof Error) return text
    const json = safeJsonParse(text)
    if (json instanceof Error) return json
    if (!Array.isArray(json)) return new FlumeParseError("notification response is not an array")
    const notifications: FlumeGitHubNotification[] = []
    for (const item of json) {
      const parsed = FlumeGitHubNotificationSchema.safeParse(item)
      if (!parsed.success)
        return new FlumeParseError("notification did not match schema", { cause: parsed.error })
      notifications.push(parsed.data)
    }
    return notifications
  }

  /** 認証ヘッダを渡す宛先は固定する。不正な next は「末尾」と解釈せず取得全体を失敗させる。 */
  private nextPage(header: string | null): string | null | Error {
    for (const part of header?.split(/,\s*(?=<)/) ?? []) {
      if (!/\brel\s*=\s*"[^"]*\bnext\b[^"]*"/i.test(part)) continue
      const match = part.match(/<([^>]+)>/)
      const url = attempt(() => new URL(match?.[1] ?? "", ENDPOINT))
      if (
        !match ||
        url instanceof Error ||
        url.origin !== "https://api.github.com" ||
        url.pathname !== "/notifications" ||
        url.username ||
        url.password ||
        url.hash
      ) {
        return new FlumeParseError("untrusted notification pagination URL")
      }
      return url.href
    }
    return null
  }

  private async isRateLimited(response: Response): Promise<boolean> {
    if (response.status === 429) return true
    if (response.status !== 403) return false
    if (
      response.headers.has("Retry-After") ||
      response.headers.get("X-RateLimit-Remaining") === "0"
    )
      return true
    const text = await safeReadText({ response, context: "rate limit response" })
    return typeof text === "string" && /secondary rate limit|abuse detection/i.test(text)
  }

  private retryDelay(response: Response): number {
    const now = safeNow({ deps: this.props.deps })
    const raw = response.headers.get("Retry-After")
    const retry =
      raw === null ? NaN : /^\d+$/.test(raw.trim()) ? Number(raw) * 1000 : Date.parse(raw) - now
    const reset = Number(response.headers.get("X-RateLimit-Reset")) * 1000 - now
    if (Number.isFinite(retry) && retry > 0) return retry
    if (Number.isFinite(reset) && reset > 0) return reset
    return 60_000
  }

  private checkpoint(
    response: Response,
    previous: FlumeGitHubCheckpoint,
    notifications: FlumeGitHubNotification[],
  ): FlumeGitHubCheckpoint {
    const lastModified = response.headers.get("Last-Modified")
    // client の時計で cursor を進めない。秒境界を重ね、重複は seen cache が取り除く。
    const timestamps = notifications
      .map((notification) => Date.parse(notification.updated_at))
      .filter(Number.isFinite)
    const serverTime = Date.parse(lastModified ?? response.headers.get("Date") ?? "")
    const latest = Number.isFinite(serverTime)
      ? serverTime
      : timestamps.length > 0
        ? timestamps.reduce((latest, timestamp) => Math.max(latest, timestamp), -Infinity)
        : NaN
    return {
      etag: response.headers.get("ETag"),
      lastModified,
      since: Number.isFinite(latest) ? new Date(latest - 1000).toISOString() : previous.since,
    }
  }
}
