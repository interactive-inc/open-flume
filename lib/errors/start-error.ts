type Options = {
  cause?: unknown
}

export class FlumeStartError extends Error {
  constructor(message: string, options?: Options) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeStartError"
    Object.freeze(this)
  }
}
