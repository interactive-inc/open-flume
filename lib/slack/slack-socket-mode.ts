import type { FlumeLogHandler, FlumeRuntimeDeps, FlumeSlackEnvelope } from "@/types"
import { FlumeSlackEnvelopeSchema } from "@/slack/slack-envelope-schema"
import { FlumeLogger } from "@/logger"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeParseError } from "@/errors/parse-error"
import { isRecord } from "@/utils/is-record"
import { safeJsonParse } from "@/utils/safe-json-parse"
import { obtainSlackUrl } from "@/slack/obtain-slack-url"

function framePreview(raw: string): string {
  return raw.length > 200 ? `${raw.slice(0, 200)}... (${raw.length} bytes)` : raw
}

type Deps = Pick<FlumeRuntimeDeps, "WebSocket" | "fetch" | "now">

type Props = {
  appToken: string
  onMessage: (envelope: FlumeSlackEnvelope) => void
  onConnected: () => void
  onDisconnected: () => void
  onLog?: FlumeLogHandler
  deps: Deps
}

export class FlumeSlackSocketMode {

  private readonly log: FlumeLogger

  private ws: WebSocket | null = null

  stopped = false

  private pendingResolve: ((value: FlumeConnectionError | FlumeHttpError | null) => void) | null = null

  private pendingResolved = false

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({ source: "slack.socket-mode", handler: props.onLog, deps: props.deps })
  }

  async connect(): Promise<FlumeConnectionError | FlumeHttpError | null> {
    this.log.info({ action: "connect.start", message: "opening WebSocket connection" })

    const url = await obtainSlackUrl({ appToken: this.props.appToken, onLog: this.props.onLog, deps: this.props.deps })

    if (url instanceof FlumeHttpError) {
      this.log.error({ action: "http.error", message: url.message, error: url })
      return url
    }

    this.log.info({ action: "url.obtained", message: "WebSocket URL obtained" })
    return this.openSocket(url)
  }

  disconnect(): void {
    this.log.info({ action: "disconnect", message: "stopping socket mode" })
    this.stopped = true

    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN
  }

  private openSocket(url: string): Promise<FlumeConnectionError | null> {
    this.pendingResolved = false

    return new Promise<FlumeConnectionError | null>((resolve) => {
      this.pendingResolve = resolve
      const socket = new this.props.deps.WebSocket(url)
      this.ws = socket
      socket.addEventListener("message", (ev) => this.onMessage(String(ev.data), socket))
      socket.addEventListener("close", (ev) => this.onClose(ev))
      socket.addEventListener("error", () => this.onError())
    })
  }

  private completeConnect(error: FlumeConnectionError | null): void {
    if (this.pendingResolved || !this.pendingResolve) return
    this.pendingResolved = true
    this.pendingResolve(error)
  }

  private onMessage(raw: string, socket: WebSocket): void {
    this.log.debug({ action: "ws.recv", message: framePreview(raw) })

    const json = safeJsonParse(raw)

    if (!isRecord(json)) {
      this.log.error({ action: "ws.parse-error", message: "invalid JSON", error: new FlumeParseError(raw.slice(0, 200)) })
      return
    }

    if (json.type === "hello") {
      this.log.info({ action: "ws.hello", message: "connection ready" })
      this.props.onConnected()
      this.completeConnect(null)
      return
    }

    if (json.type === "disconnect") {
      const reason = typeof json.reason === "string" ? json.reason : "unknown"
      this.log.info({ action: "ws.disconnect-requested", message: `reason=${reason}`, detail: { reason } })
      socket.close()
      return
    }

    if (typeof json.envelope_id === "string") {
      this.log.debug({ action: "ws.ack", message: `envelope_id=${json.envelope_id}` })
      const ack = JSON.stringify({ envelope_id: json.envelope_id })
      socket.send(ack)
      this.log.debug({ action: "ws.send", message: framePreview(ack) })
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
      action: "envelope.parse-fail",
      message: "unrecognised envelope shape, dropping",
      detail: {
        type: typeof json.type === "string" ? json.type : "unknown",
        issues: envelope.error.issues.map((i) => ({ path: i.path, message: i.message })),
      },
    })
  }

  private onClose(ev: CloseEvent): void {
    this.log.info({ action: "ws.close", message: `code=${ev.code} reason=${ev.reason || "none"}` })
    this.ws = null
    this.props.onDisconnected()
    this.completeConnect(new FlumeConnectionError(`WebSocket closed before hello (code=${ev.code})`))
  }

  private onError(): void {
    this.log.error({ action: "ws.error", message: "WebSocket error event" })
    this.completeConnect(new FlumeConnectionError("WebSocket connection error"))
  }
}
