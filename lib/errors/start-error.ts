import type { FlumeErrorCode } from "@/errors/error-codes"

type Options = {
  errorCode?: FlumeErrorCode
  cause?: unknown
}

export class FlumeStartError extends Error {
  readonly errorCode: FlumeErrorCode

  constructor(message: string, options?: Options) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeStartError"
    this.errorCode = options?.errorCode ?? "FLUME_START_FAILED"
    Object.freeze(this)
  }
}
