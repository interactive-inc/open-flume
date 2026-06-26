import type { FlumeLogHandler, FlumeRuntimeDeps, FlumeSlackEnvelope } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeParseError } from "@/errors/parse-error"
import { obtainSlackUrl } from "@/slack/obtain-slack-url"
import { FlumeSlackEnvelopeSchema } from "@/slack/slack-envelope-schema"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeJsonParse } from "@/utils/safe-json-parse"
import { safeStringify } from "@/utils/safe-stringify"

type Deps = Pick<FlumeRuntimeDeps, "WebSocket" | "fetch" | "now">

type Props = {
  appToken: string
  onMessage: (envelope: FlumeSlackEnvelope) => void
  onConnected: () => void
  onDisconnected: () => void
  onLog?: FlumeLogHandler
  deps: Deps
}

type ConnectOptions = {
  signal?: AbortSignal
}

const WS_OPEN = 1

/**
 * Slack Socket Mode の最小 WebSocket 実装。`apps.connections.open` で URL を取得し
 * `type: "hello"` を待ってから connected を通知する。
 * IO 境界は全て `attempt` 経由で扱い、`connect()` は決して reject しない
 */
export class FlumeSlackSocketMode {
  private readonly log: FlumeLogger

  private ws: WebSocket | null = null

  private isStoppedFlag = false

  private hasConnected = false

  private pendingResolve: ((value: FlumeConnectionError | null) => void) | null = null

  private pendingResolved = false

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({
      source: "slack.socket-mode",
      handler: props.onLog,
      deps: props.deps,
    })
  }

  get isStopped(): boolean {
    return this.isStoppedFlag
  }

  async connect(options?: ConnectOptions): Promise<FlumeConnectionError | FlumeHttpError | null> {
    this.log.info({ action: "connect.start", message: "opening WebSocket connection" })

    const url = await obtainSlackUrl({
      appToken: this.props.appToken,
      signal: options?.signal,
      onLog: this.props.onLog,
      deps: this.props.deps,
    })

    if (url instanceof FlumeHttpError) {
      this.log.error({ action: "http.error", message: url.message, error: url })
      return url
    }

    if (url instanceof FlumeConnectionError) {
      this.log.error({ action: "url.error", message: url.message, error: url })
      return url
    }

    if (this.isStoppedFlag) {
      const error = new FlumeConnectionError("stopped before WebSocket open")
      this.log.info({ action: "connect.aborted", message: safeErrorMessage({ error }) })
      return error
    }

    this.log.info({ action: "slack.url.obtained", message: "WebSocket URL obtained" })
    return this.openSocket(url)
  }

  disconnect(): void {
    this.log.info({ action: "disconnect", message: "stopping socket mode" })
    this.isStoppedFlag = true

    this.closeSocket(this.ws)
    this.ws = null
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WS_OPEN
  }

  private openSocket(url: string): Promise<FlumeConnectionError | null> {
    const WS = this.props.deps.WebSocket
    if (!WS) {
      const error = new FlumeConnectionError("WebSocket runtime not available")
      this.log.error({ action: "ws.error", message: safeErrorMessage({ error }), error })
      return Promise.resolve(error)
    }

    this.pendingResolved = false
    this.hasConnected = false

    return new Promise<FlumeConnectionError | null>((resolve) => {
      this.pendingResolve = resolve

      const socketResult = attempt(() => new WS(url))
      if (socketResult instanceof Error) {
        const error = new FlumeConnectionError(
          `WebSocket construction failed: ${safeErrorMessage({ error: socketResult })}`,
          { cause: socketResult },
        )
        this.log.error({
          action: "ws.construct.error",
          message: safeErrorMessage({ error }),
          error,
        })
        this.ws = null
        this.pendingResolved = true
        resolve(error)
        return
      }

      const socket = socketResult
      this.ws = socket
      const listenerResult = attempt(() => {
        socket.addEventListener("message", (ev) => this.safeOnMessage(ev, socket))
        socket.addEventListener("close", (ev) => this.safeOnClose(ev))
        socket.addEventListener("error", () => this.safeOnError())
      })
      if (listenerResult instanceof Error) {
        const error = new FlumeConnectionError(
          `WebSocket listener registration failed: ${safeErrorMessage({ error: listenerResult })}`,
          { cause: listenerResult },
        )
        this.log.error({ action: "ws.listener.error", message: safeErrorMessage({ error }), error })
        this.ws = null
        this.pendingResolved = true
        resolve(error)
      }
    })
  }

  private completeConnect(error: FlumeConnectionError | null): void {
    if (this.pendingResolved || !this.pendingResolve) return
    this.pendingResolved = true
    this.pendingResolve(error)
  }

  private safeOnMessage(ev: MessageEvent, socket: WebSocket): void {
    const r = attempt(() => this.onMessage(String(ev.data), socket))
    if (r instanceof Error) {
      this.log.error({
        action: "ws.message.threw",
        message: safeErrorMessage({ error: r }),
        error: r,
      })
    }
  }

  private safeOnClose(ev: CloseEvent): void {
    const r = attempt(() => this.onClose(ev))
    if (r instanceof Error) {
      this.log.error({
        action: "ws.close.threw",
        message: safeErrorMessage({ error: r }),
        error: r,
      })
    }
  }

  private safeOnError(): void {
    const r = attempt(() => this.onError())
    if (r instanceof Error) {
      this.log.error({
        action: "ws.error.threw",
        message: safeErrorMessage({ error: r }),
        error: r,
      })
    }
  }

  private onMessage(raw: string, socket: WebSocket): void {
    if (this.isStoppedFlag) return

    const json = safeJsonParse(raw)

    if (json instanceof FlumeParseError) {
      this.log.error({
        action: "ws.parse.error",
        message: json.message,
        error: json,
        detail: { length: raw.length },
      })
      return
    }

    if (!isRecord(json)) {
      this.log.error({
        action: "ws.parse.error",
        message: "expected JSON object",
        error: new FlumeParseError(`non-object frame (${typeof json})`),
      })
      return
    }

    this.log.debug({
      action: "ws.recv",
      message: `type=${typeof json.type === "string" ? json.type : "-"} length=${raw.length}`,
      detail: { length: raw.length },
    })

    if (json.type === "hello") {
      this.log.info({ action: "socket.hello", message: "connection ready" })
      this.hasConnected = true
      this.props.onConnected()
      this.completeConnect(null)
      return
    }

    if (json.type === "disconnect") {
      const reason = typeof json.reason === "string" ? json.reason : "unknown"
      this.log.info({
        action: "ws.disconnect.requested",
        message: `reason=${reason}`,
        detail: { reason },
      })
      this.closeSocket(socket)
      return
    }

    if (typeof json.envelope_id === "string") {
      this.log.debug({ action: "ws.ack", message: `envelope_id=${json.envelope_id}` })
      const ack = this.safeSerialize({ envelope_id: json.envelope_id })
      if (ack !== null) this.send(socket, ack)
    }

    const envelope = FlumeSlackEnvelopeSchema.safeParse(json)

    if (envelope.success) {
      this.log.debug({
        action: "envelope.recv",
        message: `type=${envelope.data.type} envelope_id=${envelope.data.envelope_id}`,
        detail: { type: envelope.data.type, envelopeId: envelope.data.envelope_id },
      })
      this.props.onMessage(envelope.data)
      return
    }

    this.log.warn({
      action: "envelope.parse.error",
      message: "unrecognised envelope shape, dropping",
      detail: {
        type: typeof json.type === "string" ? json.type : "unknown",
        issues: envelope.error.issues.map((i) => ({ path: i.path, message: i.message })),
      },
    })
  }

  private onClose(ev: CloseEvent): void {
    this.log.info({
      action: "ws.close",
      message: `code=${ev.code} reason=${ev.reason || "none"}`,
      detail: { code: ev.code, reason: ev.reason },
    })
    this.ws = null

    if (this.hasConnected) {
      this.props.onDisconnected()
    }

    if (!this.pendingResolved) {
      const error = new FlumeConnectionError(`WebSocket closed before hello (code=${ev.code})`, {
        code: ev.code,
      })
      this.completeConnect(error)
    }
  }

  private onError(): void {
    const error = new FlumeConnectionError("WebSocket connection error")
    this.log.error({ action: "ws.error", message: safeErrorMessage({ error }), error })
    this.completeConnect(error)
  }

  private closeSocket(ws: WebSocket | null): void {
    if (ws === null) return

    const result = attempt(() => ws.close())
    if (result instanceof Error) {
      this.log.error({
        action: "ws.close.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
  }

  private send(socket: WebSocket, payload: string): void {
    if (socket.readyState !== WS_OPEN) {
      this.log.warn({
        action: "ws.send",
        message: `ws.send skipped: readyState=${socket.readyState} (not OPEN)`,
        detail: { readyState: socket.readyState },
      })
      return
    }

    const result = attempt(() => socket.send(payload))
    if (result instanceof Error) {
      this.log.error({
        action: "ws.send",
        message: `ws.send failed: ${safeErrorMessage({ error: result })}`,
        error: result,
      })
    }
  }

  private safeSerialize(value: Record<string, unknown>): string | null {
    const result = safeStringify(value)
    if (result instanceof Error) {
      this.log.error({
        action: "ws.send.serialize.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
      return null
    }
    return result
  }
}
