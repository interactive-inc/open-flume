type Options = {
  cause?: unknown
  code?: number
}

/**
 * 接続失敗を表す。Discord Gateway / Slack Socket Mode / その他 WebSocket 系の close で発生。
 * `code` は接続が落ちた際の close code (Discord は 4xxx 帯が再接続可否を示す)
 */
export class FlumeConnectionError extends Error {
  readonly code: number | null

  constructor(message: string, options?: Options) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause })
    this.name = "FlumeConnectionError"
    this.code = options?.code ?? null
    Object.freeze(this)
  }
}
