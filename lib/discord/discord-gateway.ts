import type { FlumeGatewayMessage, FlumeLogHandler, FlumeRuntimeDeps } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeDiscordHeartbeat } from "@/discord/discord-heartbeat"
import { FlumeDiscordGatewaySession } from "@/discord/discord-gateway-session"
import { parseDiscordGatewayMessage } from "@/discord/parse-discord-gateway-message"

type Deps = Pick<
  FlumeRuntimeDeps,
  "WebSocket" | "setInterval" | "clearInterval" | "setTimeout" | "random" | "now"
>

type Props = {
  token: string
  intents: number
  onDispatch: (event: string, data: Record<string, unknown>) => void
  onStatus: (status: "connected" | "disconnected") => void
  onLog?: FlumeLogHandler
  deps: Deps
}

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

function framePreview(raw: string): string {
  return raw.length > 200 ? `${raw.slice(0, 200)}... (${raw.length} bytes)` : raw
}

const OP_DISPATCH = 0
const OP_HEARTBEAT = 1
const OP_IDENTIFY = 2
const OP_RESUME = 6
const OP_RECONNECT = 7
const OP_INVALID_SESSION = 9
const OP_HELLO = 10
const OP_HEARTBEAT_ACK = 11

const OP_NAMES: Record<number, string> = {
  [OP_DISPATCH]: "DISPATCH",
  [OP_HEARTBEAT]: "HEARTBEAT",
  [OP_IDENTIFY]: "IDENTIFY",
  [OP_RESUME]: "RESUME",
  [OP_RECONNECT]: "RECONNECT",
  [OP_INVALID_SESSION]: "INVALID_SESSION",
  [OP_HELLO]: "HELLO",
  [OP_HEARTBEAT_ACK]: "HEARTBEAT_ACK",
}

export class FlumeDiscordGateway {

  private readonly log: FlumeLogger

  private ws: WebSocket | null = null

  private heartbeat: FlumeDiscordHeartbeat | null = null

  session = FlumeDiscordGatewaySession.empty()

  stopped = false

  private pendingResolve: ((value: FlumeConnectionError | null) => void) | null = null

  private pendingResolved = false

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({ source: "discord.gateway", handler: props.onLog, deps: props.deps })
  }

  connect(url?: string): Promise<FlumeConnectionError | null> {
    const target = url ?? GATEWAY_URL
    this.log.info({ action: "connect.start", message: `url=${new URL(target).hostname}` })
    this.pendingResolved = false

    return new Promise<FlumeConnectionError | null>((resolve) => {
      this.pendingResolve = resolve
      const socket = new this.props.deps.WebSocket(target)
      this.ws = socket
      socket.addEventListener("message", (ev) => this.onMessage(String(ev.data), socket))
      socket.addEventListener("close", (ev) => this.onClose(ev))
      socket.addEventListener("error", () => this.onError())
    })
  }

  disconnect(): void {
    this.log.info({ action: "disconnect", message: "shutting down gateway" })
    this.stopped = true
    this.heartbeat?.stop()

    if (this.ws) {
      this.ws.close(1000, "shutdown")
      this.ws = null
    }
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN
  }

  private completeConnect(error: FlumeConnectionError | null): void {
    if (this.pendingResolved || !this.pendingResolve) return
    this.pendingResolved = true
    this.pendingResolve(error)
  }

  private onMessage(raw: string, socket: WebSocket): void {
    this.log.debug({ action: "ws.recv", message: framePreview(raw) })

    const parsed = parseDiscordGatewayMessage(raw)

    if (parsed instanceof FlumeParseError) {
      this.log.error({ action: "ws.parse-error", message: parsed.message, error: parsed })
      return
    }

    this.log.debug({
      action: "ws.frame",
      message: `op=${OP_NAMES[parsed.op] ?? parsed.op} t=${parsed.t ?? "-"} s=${parsed.s ?? "-"}`,
      detail: { op: parsed.op, t: parsed.t, s: parsed.s },
    })

    if (parsed.s !== null) {
      this.session = this.session.withSeq(parsed.s)
    }

    if (parsed.op === OP_HELLO) return this.onHello(parsed)
    if (parsed.op === OP_HEARTBEAT_ACK) return this.onHeartbeatAck()
    if (parsed.op === OP_HEARTBEAT) return this.onHeartbeatRequest()
    if (parsed.op === OP_RECONNECT) return this.onReconnectRequest(socket)
    if (parsed.op === OP_INVALID_SESSION) return this.onInvalidSession(parsed, socket)
    if (parsed.op === OP_DISPATCH) return this.onDispatch(parsed)

    this.log.warn({ action: "ws.unknown-op", message: `unknown op=${parsed.op}`, detail: { op: parsed.op } })
  }

  private onHello(msg: FlumeGatewayMessage): void {
    const interval = typeof msg.d?.heartbeat_interval === "number" ? msg.d.heartbeat_interval : 0
    this.log.info({ action: "hello", message: `heartbeat_interval=${interval}ms` })

    this.heartbeat = new FlumeDiscordHeartbeat({
      deps: this.props.deps,
      onSend: () => {
        this.log.debug({ action: "heartbeat.send", message: `seq=${this.session.seq}` })
        this.send({ op: OP_HEARTBEAT, d: this.session.seq })
      },
      onZombie: () => {
        this.log.warn({ action: "heartbeat.zombie", message: "no ACK received, closing connection" })
        this.ws?.close(4009, "zombie connection")
      },
    })

    this.heartbeat.start(interval)

    if (this.session.canResume()) {
      this.sendResume()
    } else {
      this.sendIdentify()
    }
  }

  private onHeartbeatAck(): void {
    this.log.debug({ action: "heartbeat.ack", message: "received" })
    this.heartbeat?.ack()
  }

  private onHeartbeatRequest(): void {
    this.log.debug({ action: "heartbeat.requested", message: "server requested heartbeat" })
    this.send({ op: OP_HEARTBEAT, d: this.session.seq })
  }

  private onReconnectRequest(socket: WebSocket): void {
    this.log.info({ action: "reconnect.requested", message: "server requested reconnect" })
    socket.close(4000, "reconnect requested")
  }

  private onInvalidSession(msg: FlumeGatewayMessage, socket: WebSocket): void {
    const resumable = !!msg.d
    this.log.warn({ action: "invalid-session", message: `resumable=${resumable}` })
    this.session = this.session.withReset()

    if (resumable) {
      const delay = 1000 + this.props.deps.random() * 4000
      this.log.info({ action: "identify.delayed", message: `re-identify in ${Math.round(delay)}ms` })
      this.props.deps.setTimeout(() => this.sendIdentify(), delay)
    } else {
      socket.close(4000, "invalid session")
    }
  }

  private onDispatch(msg: FlumeGatewayMessage): void {
    if (msg.t === "READY" && msg.d) {
      const sessionId = typeof msg.d.session_id === "string" ? msg.d.session_id : ""
      const resumeUrl = typeof msg.d.resume_gateway_url === "string" ? msg.d.resume_gateway_url : ""
      this.session = this.session.withReady(sessionId, resumeUrl)
      this.log.info({ action: "ready", message: `session=${sessionId}` })
      this.props.onStatus("connected")
      this.completeConnect(null)
    }

    if (msg.t === "RESUMED") {
      this.log.info({ action: "resumed", message: `session=${this.session.sessionId} seq=${this.session.seq}` })
      this.props.onStatus("connected")
      this.completeConnect(null)
    }

    if (msg.t && msg.d) {
      this.props.onDispatch(msg.t, msg.d)
    } else if (msg.t) {
      this.props.onDispatch(msg.t, {})
    }
  }

  private onClose(ev: CloseEvent): void {
    this.log.info({
      action: "ws.close",
      message: `code=${ev.code} reason=${ev.reason || "none"}`,
      detail: { code: ev.code, reason: ev.reason },
    })

    this.ws = null
    this.heartbeat?.stop()
    this.props.onStatus("disconnected")
    this.completeConnect(new FlumeConnectionError(`WebSocket closed before ready (code=${ev.code})`))
  }

  private onError(): void {
    this.log.error({ action: "ws.error", message: "WebSocket error event" })
    this.completeConnect(new FlumeConnectionError("WebSocket connection error"))
  }

  private send(input: { op: number; d?: unknown }): void {
    const payload = JSON.stringify({ op: input.op, d: input.d ?? null })
    this.log.debug({ action: "ws.send", message: `op=${OP_NAMES[input.op] ?? input.op}`, detail: { op: input.op } })
    this.ws?.send(payload)
    this.log.debug({ action: "ws.sent", message: framePreview(payload) })
  }

  private sendIdentify(): void {
    this.log.info({ action: "identify", message: `intents=${this.props.intents}` })

    this.send({
      op: OP_IDENTIFY,
      d: {
        token: this.props.token,
        intents: this.props.intents,
        properties: { os: "linux", browser: "open-flume", device: "open-flume" },
      },
    })
  }

  private sendResume(): void {
    this.log.info({ action: "resume", message: `session=${this.session.sessionId} seq=${this.session.seq}` })

    this.send({
      op: OP_RESUME,
      d: { token: this.props.token, session_id: this.session.sessionId, seq: this.session.seq },
    })
  }
}
