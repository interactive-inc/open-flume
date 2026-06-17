type Props = {
  message: string
  status: number
}

export class FlumeHttpError extends Error {

  readonly status: number

  constructor(props: Props) {
    super(props.message)
    this.name = "FlumeHttpError"
    this.status = props.status
    Object.freeze(this)
  }
}
