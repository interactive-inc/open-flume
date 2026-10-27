import type { FlumeErrorCode } from "@/errors/error-codes"

const AUTH_ERRORS = new Set([
  "invalid_auth",
  "account_inactive",
  "token_revoked",
  "not_authed",
  "not_allowed_token_type",
  "token_expired",
  "missing_scope",
])

/** HTTP とサービス固有 code を同じ規則で分類し、再接続の判断にも使う。 */
export function slackErrorCode(status: number, code?: string | null): FlumeErrorCode {
  if (status === 401 || status === 403 || (code && AUTH_ERRORS.has(code)))
    return "FLUME_AUTH_FAILED"
  if (status === 429 || code === "ratelimited") return "FLUME_RATE_LIMITED"
  return "FLUME_HTTP_FAILED"
}
