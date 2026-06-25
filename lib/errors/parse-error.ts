type Options = {
  cause?: unknown
}

export class FlumeParseError extends Error {
  constructor(message: string, options?: Options) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeParseError"
    Object.freeze(this)
  }
}
