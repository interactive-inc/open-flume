export class FlumeConnectionError extends Error {

  constructor(message: string) {
    super(message)
    this.name = "FlumeConnectionError"
    Object.freeze(this)
  }
}
