type Props = {
  message: string
  status: number
  cause?: unknown
  /** API 固有のエラーコード (Slack の `invalid_auth` 等)。呼び出し側が恒久/一時を分類するのに使う */
  code?: string | null
  /** サーバーが `Retry-After` 等で指示した再試行までの待機時間 (ms) */
  retryAfterMs?: number | null
}

export class FlumeHttpError extends Error {
  readonly status: number

  readonly code: string | null

  readonly retryAfterMs: number | null

  constructor(props: Props) {
    super(props.message, props.cause === undefined ? undefined : { cause: props.cause })
    this.name = "FlumeHttpError"
    this.status = props.status
    this.code = props.code ?? null
    this.retryAfterMs = props.retryAfterMs ?? null
    Object.freeze(this)
  }
}
