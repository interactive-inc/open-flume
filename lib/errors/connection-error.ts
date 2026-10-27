import type { FlumeErrorContext } from "@/errors/error-context"
import type { FlumeErrorCode } from "@/errors/error-codes"

type Options = {
  context?: FlumeErrorContext
  errorCode?: FlumeErrorCode
  cause?: unknown
  code?: number
}

/**
 * 接続失敗を表す。Discord Gateway / Slack Socket Mode / その他 WebSocket 系の close で発生。
 * `code` は接続が落ちた際の close code (Discord は 4xxx 帯が再接続可否を示す)
 */
export class FlumeConnectionError extends Error {
  readonly errorCode: FlumeErrorCode

  readonly context: FlumeErrorContext | null

  readonly code: number | null

  constructor(message: string, options?: Options) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeConnectionError"
    this.code = options?.code ?? null
    this.context = options?.context ? Object.freeze({ ...options.context }) : null
    this.errorCode = options?.errorCode ?? "FLUME_CONNECTION_FAILED"
    Object.freeze(this)
  }
}
