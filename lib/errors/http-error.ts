type Props = {
  message: string
  status: number
  cause?: unknown
}

export class FlumeHttpError extends Error {
  readonly status: number

  constructor(props: Props) {
    super(props.message, props.cause === undefined ? undefined : { cause: props.cause })
    this.name = "FlumeHttpError"
    this.status = props.status
    Object.freeze(this)
  }
}
