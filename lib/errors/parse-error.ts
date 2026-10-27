import type { FlumeErrorCode } from "@/errors/error-codes"

type Options = {
  errorCode?: FlumeErrorCode
  cause?: unknown
}

export class FlumeParseError extends Error {
  readonly errorCode: FlumeErrorCode

  constructor(message: string, options?: Options) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeParseError"
    this.errorCode = options?.errorCode ?? "FLUME_PARSE_FAILED"
    Object.freeze(this)
  }
}
