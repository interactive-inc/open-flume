import type { FlumeErrorContext } from "@/errors/error-context"
import type { FlumeErrorCode } from "@/errors/error-codes"

type Options = {
  errorCode: Extract<
    FlumeErrorCode,
    "FLUME_ABORTED" | "FLUME_TIMEOUT" | "FLUME_TIMER_FAILED" | "FLUME_RUNTIME_FAILED"
  >
  context: FlumeErrorContext
  cause?: unknown
}

/** 操作の中断・期限・実行基盤の失敗。通信や保存などの対象には依存しない。 */
export class FlumeOperationError extends Error {
  readonly errorCode: Options["errorCode"]
  readonly context: FlumeErrorContext

  constructor(message: string, options: Options) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeOperationError"
    this.errorCode = options.errorCode
    this.context = Object.freeze({ ...options.context })
    Object.freeze(this)
  }
}
