export class FlumeStartError extends Error {

  constructor(message: string) {
    super(message)
    this.name = "FlumeStartError"
    Object.freeze(this)
  }
}
