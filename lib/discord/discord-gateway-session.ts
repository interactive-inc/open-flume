type Props = {
  sessionId: string | null
  resumeUrl: string | null
  seq: number | null
}

export class FlumeDiscordGatewaySession {

  readonly sessionId: string | null

  readonly resumeUrl: string | null

  readonly seq: number | null

  constructor(props: Props) {
    this.sessionId = props.sessionId
    this.resumeUrl = props.resumeUrl
    this.seq = props.seq
    Object.freeze(this)
  }

  static empty(): FlumeDiscordGatewaySession {
    return new FlumeDiscordGatewaySession({ sessionId: null, resumeUrl: null, seq: null })
  }

  canResume(): boolean {
    return this.sessionId !== null
  }

  withSeq(seq: number): FlumeDiscordGatewaySession {
    return new FlumeDiscordGatewaySession({ sessionId: this.sessionId, resumeUrl: this.resumeUrl, seq })
  }

  withReady(sessionId: string, resumeUrl: string): FlumeDiscordGatewaySession {
    return new FlumeDiscordGatewaySession({ sessionId, resumeUrl, seq: this.seq })
  }

  withReset(): FlumeDiscordGatewaySession {
    return FlumeDiscordGatewaySession.empty()
  }
}
