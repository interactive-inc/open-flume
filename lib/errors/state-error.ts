import type { FlumeErrorContext } from "@/errors/error-context"
import type { FlumeErrorCode } from "@/errors/error-codes"

type Options = {
  context: FlumeErrorContext
  errorCode?: FlumeErrorCode
  cause?: unknown
}

/** 永続状態の読取・検証・保存の失敗。原因となった IO の例外は cause に保持する。 */
export class FlumeStateError extends Error {
  readonly errorCode: FlumeErrorCode
  readonly context: FlumeErrorContext

  constructor(message: string, options: Options) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeStateError"
    this.errorCode = options.errorCode ?? "FLUME_STATE_FAILED"
    this.context = Object.freeze({ ...options.context })
    Object.freeze(this)
  }
}
