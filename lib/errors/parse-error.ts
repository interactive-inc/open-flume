export class FlumeParseError extends Error {

  constructor(message: string) {
    super(message)
    this.name = "FlumeParseError"
    Object.freeze(this)
  }
}
