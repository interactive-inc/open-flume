import type { FlumeErrorContext } from "@/errors/error-context"
import type { FlumeErrorCode } from "@/errors/error-codes"

type Props = {
  context?: FlumeErrorContext
  errorCode?: FlumeErrorCode
  message: string
  status: number
  cause?: unknown
  /** API 固有のエラーコード (Slack の `invalid_auth` 等)。呼び出し側が恒久/一時を分類するのに使う */
  code?: string | null
  /** サーバーが `Retry-After` 等で指示した再試行までの待機時間 (ms) */
  retryAfterMs?: number | null
}

export class FlumeHttpError extends Error {
  readonly errorCode: FlumeErrorCode

  readonly context: FlumeErrorContext | null

  readonly status: number

  readonly code: string | null

  readonly retryAfterMs: number | null

  constructor(props: Props) {
    super(props.message, props.cause === undefined ? undefined : { cause: props.cause })
    this.name = "FlumeHttpError"
    this.status = props.status
    this.context = props.context ? Object.freeze({ ...props.context }) : null
    this.code = props.code ?? null
    this.retryAfterMs = props.retryAfterMs ?? null
    this.errorCode = props.errorCode ?? "FLUME_HTTP_FAILED"
    Object.freeze(this)
  }
}
