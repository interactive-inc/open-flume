import type {
  FlumeGatewayMessage,
  FlumeLogHandler,
  FlumeRuntimeDeps,
  FlumeTimerHandle,
} from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeDiscordGatewaySession } from "@/discord/discord-gateway-session"
import { FlumeDiscordHeartbeat } from "@/discord/discord-heartbeat"
import { parseFlumeDiscordGatewayMessage } from "@/discord/parse-discord-gateway-message"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeRandom } from "@/utils/safe-random"
import { safeStringify } from "@/utils/safe-stringify"

type Deps = Pick<
  FlumeRuntimeDeps,
  "WebSocket" | "setInterval" | "clearInterval" | "setTimeout" | "clearTimeout" | "random" | "now"
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

// WHATWG WebSocket.OPEN は仕様で 1 に固定。global.WebSocket に依存しないようリテラル参照
const WS_OPEN = 1

// 再接続しても回復不能な Discord Gateway close code 群。
// https://discord.com/developers/docs/topics/opcodes-and-status-codes#gateway-close-event-codes
const TERMINAL_CLOSE_CODES = new Set<number>([
  4004, // Authentication failed
  4010, // Invalid shard
  4011, // Sharding required
  4012, // Invalid API version
  4013, // Invalid intents
  4014, // Disallowed intents
])

/**
 * Discord Gateway v10 の最小実装。HELLO -> IDENTIFY/RESUME -> READY/RESUMED -> dispatch を扱う。
 * READY 後の WebSocket 切断のみ `onStatus("disconnected")` を発火し source 側で再接続する。
 * 終端 close code (4004 / 401x) を受けた場合は stopped 化して再接続を抑止。
 * IO 境界は全て `attempt` 経由で扱い、コンストラクタ throw も `FlumeConnectionError` として返す
 * (`connect()` は決して reject しない)
 */
export class FlumeDiscordGateway {
  private readonly log: FlumeLogger

  private ws: WebSocket | null = null

  private heartbeat: FlumeDiscordHeartbeat | null = null

  private currentSession = FlumeDiscordGatewaySession.empty()

  private isStoppedFlag = false

  private hasConnected = false

  private pendingResolve: ((value: FlumeConnectionError | null) => void) | null = null

  private pendingResolved = false

  private invalidSessionTimer: FlumeTimerHandle | null = null

  constructor(private readonly props: Props) {
    this.log = new FlumeLogger({
      source: "discord.gateway",
      handler: props.onLog,
      deps: props.deps,
    })
  }

  get session(): FlumeDiscordGatewaySession {
    return this.currentSession
  }

  get isStopped(): boolean {
    return this.isStoppedFlag
  }

  connect(url?: string): Promise<FlumeConnectionError | null> {
    const WS = this.props.deps.WebSocket
    if (!WS) {
      const error = new FlumeConnectionError("WebSocket runtime not available")
      this.log.error({ action: "ws.error", message: safeErrorMessage({ error }), error })
      return Promise.resolve(error)
    }

    const target = url ?? GATEWAY_URL
    const hostResult = attempt(() => new URL(target).hostname)
    const host = hostResult instanceof Error ? "unknown" : hostResult
    this.log.info({ action: "connect.start", message: `host=${host}` })
    this.pendingResolved = false
    this.hasConnected = false

    return new Promise<FlumeConnectionError | null>((resolve) => {
      this.pendingResolve = resolve

      const socketResult = attempt(() => new WS(target))
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

  disconnect(): void {
    this.log.info({ action: "disconnect", message: "shutting down gateway" })
    this.isStoppedFlag = true
    this.heartbeat?.stop()
    this.clearInvalidSessionTimer()

    this.closeSocket({ ws: this.ws, code: 1000, reason: "shutdown" })
    this.ws = null
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WS_OPEN
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

    const parsed = parseFlumeDiscordGatewayMessage(raw)

    if (parsed instanceof FlumeParseError) {
      this.log.error({
        action: "ws.parse.error",
        message: parsed.message,
        error: parsed,
        detail: { length: raw.length },
      })
      return
    }

    this.log.debug({
      action: "ws.recv",
      message: `op=${OP_NAMES[parsed.op] ?? parsed.op} t=${parsed.t ?? "-"} s=${parsed.s ?? "-"} length=${raw.length}`,
      detail: { op: parsed.op, t: parsed.t, s: parsed.s, length: raw.length },
    })

    if (parsed.s !== null) {
      this.currentSession = this.currentSession.withSeq(parsed.s)
    }

    if (parsed.op === OP_HELLO) return this.onHello(parsed)
    if (parsed.op === OP_HEARTBEAT_ACK) return this.onHeartbeatAck()
    if (parsed.op === OP_HEARTBEAT) return this.onHeartbeatRequest()
    if (parsed.op === OP_RECONNECT) return this.onReconnectRequest(socket)
    if (parsed.op === OP_INVALID_SESSION) return this.onInvalidSession(parsed, socket)
    if (parsed.op === OP_DISPATCH) return this.onDispatch(parsed)

    this.log.warn({
      action: "ws.op.unknown",
      message: `unknown op=${parsed.op}`,
      detail: { op: parsed.op },
    })
  }

  private onHello(msg: FlumeGatewayMessage): void {
    const d = isRecord(msg.d) ? msg.d : null
    const interval = d && typeof d.heartbeat_interval === "number" ? d.heartbeat_interval : 0
    this.log.info({
      action: "gateway.hello",
      message: `heartbeat_interval=${interval}ms`,
      detail: { interval },
    })

    this.heartbeat?.stop()
    this.heartbeat = new FlumeDiscordHeartbeat({
      log: this.log,
      deps: this.props.deps,
      onSend: () => {
        this.log.debug({ action: "heartbeat.send", message: `seq=${this.currentSession.seq}` })
        this.send({ op: OP_HEARTBEAT, d: this.currentSession.seq })
      },
      onZombie: () => {
        this.log.warn({
          action: "heartbeat.zombie",
          message: "no ACK received, closing connection",
        })
        this.closeSocket({ ws: this.ws, code: 4009, reason: "zombie connection" })
      },
    })

    this.heartbeat.start(interval)

    if (this.currentSession.canResume()) {
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
    this.send({ op: OP_HEARTBEAT, d: this.currentSession.seq })
  }

  private onReconnectRequest(socket: WebSocket): void {
    this.log.info({ action: "ws.reconnect.requested", message: "server requested reconnect" })
    this.closeSocket({ ws: socket, code: 4000, reason: "reconnect requested" })
  }

  private onInvalidSession(msg: FlumeGatewayMessage, socket: WebSocket): void {
    const resumable = msg.d === true || (isRecord(msg.d) && msg.d.resumable === true)
    this.log.warn({
      action: "session.invalid",
      message: `resumable=${resumable}`,
      detail: { resumable },
    })

    const delay = 1000 + safeRandom({ deps: this.props.deps }) * 4000

    if (!resumable) {
      this.currentSession = this.currentSession.withReset()
    }

    this.clearInvalidSessionTimer()
    const timerResult = attempt(() =>
      this.props.deps.setTimeout(() => {
        this.invalidSessionTimer = null
        this.closeSocket({ ws: socket, code: 4000, reason: "invalid session" })
      }, delay),
    )
    if (timerResult instanceof Error) {
      this.log.error({
        action: "session.invalid.timer.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.invalidSessionTimer = null
    } else {
      this.invalidSessionTimer = timerResult
    }
  }

  private onDispatch(msg: FlumeGatewayMessage): void {
    const d = isRecord(msg.d) ? msg.d : null

    if (msg.t === "READY" && d) {
      const sessionId = typeof d.session_id === "string" ? d.session_id : ""
      const resumeUrl = typeof d.resume_gateway_url === "string" ? d.resume_gateway_url : ""
      this.currentSession = this.currentSession.withReady(sessionId, resumeUrl)
      this.log.info({
        action: "gateway.ready",
        message: `session ready`,
        detail: { hasResumeUrl: resumeUrl !== "" },
      })
      this.hasConnected = true
      this.props.onStatus("connected")
      this.completeConnect(null)
    }

    if (msg.t === "RESUMED") {
      this.log.info({ action: "gateway.resumed", message: `seq=${this.currentSession.seq}` })
      this.hasConnected = true
      this.props.onStatus("connected")
      this.completeConnect(null)
    }

    if (msg.t && d) {
      this.props.onDispatch(msg.t, d)
    } else if (msg.t) {
      this.log.debug({
        action: "dispatch.empty",
        message: `dropped ${msg.t} (no payload)`,
        detail: { type: msg.t },
      })
    }
  }

  private onClose(ev: CloseEvent): void {
    const terminal = TERMINAL_CLOSE_CODES.has(ev.code)
    this.log.info({
      action: "ws.close",
      message: `code=${ev.code} reason=${ev.reason || "none"}${terminal ? " (terminal)" : ""}`,
      detail: { code: ev.code, reason: ev.reason, terminal },
    })

    this.ws = null
    this.heartbeat?.stop()
    this.clearInvalidSessionTimer()

    if (terminal) {
      this.isStoppedFlag = true
    }

    if (this.hasConnected || terminal) {
      this.props.onStatus("disconnected")
    }

    if (!this.pendingResolved) {
      const error = new FlumeConnectionError(`WebSocket closed before ready (code=${ev.code})`, {
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

  private clearInvalidSessionTimer(): void {
    if (this.invalidSessionTimer === null) return

    const handle = this.invalidSessionTimer
    const result = attempt(() => this.props.deps.clearTimeout(handle))
    if (result instanceof Error) {
      this.log.error({
        action: "session.invalid.timer.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.invalidSessionTimer = null
  }

  private closeSocket(input: { ws: WebSocket | null; code?: number; reason?: string }): void {
    if (input.ws === null) return

    const ws = input.ws
    const result = attempt(() => {
      if (input.code !== undefined) {
        ws.close(input.code, input.reason ?? "")
      } else {
        ws.close()
      }
    })
    if (result instanceof Error) {
      this.log.error({
        action: "ws.close.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
  }

  private send(input: { op: number; d?: unknown }): void {
    const payload = this.safeSerialize(input)
    if (payload === null) return

    this.log.debug({
      action: "ws.send",
      message: `op=${OP_NAMES[input.op] ?? input.op} length=${payload.length}`,
      detail: { op: input.op, length: payload.length },
    })

    const ws = this.ws
    if (ws === null) {
      this.log.warn({ action: "ws.send", message: "ws.send skipped: socket is null" })
      return
    }
    if (ws.readyState !== WS_OPEN) {
      this.log.warn({
        action: "ws.send",
        message: `ws.send skipped: readyState=${ws.readyState} (not OPEN)`,
        detail: { readyState: ws.readyState },
      })
      return
    }

    const result = attempt(() => ws.send(payload))
    if (result instanceof Error) {
      this.log.error({
        action: "ws.send",
        message: `ws.send failed: ${safeErrorMessage({ error: result })}`,
        error: result,
      })
    }
  }

  private safeSerialize(input: { op: number; d?: unknown }): string | null {
    const result = safeStringify({ op: input.op, d: input.d ?? null })
    if (result instanceof Error) {
      this.log.error({
        action: "ws.send.serialize.error",
        message: safeErrorMessage({ error: result }),
        error: result,
        detail: { op: input.op },
      })
      return null
    }
    return result
  }

  private sendIdentify(): void {
    this.log.info({ action: "gateway.identify", message: `intents=${this.props.intents}` })

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
    this.log.info({ action: "gateway.resume", message: `seq=${this.currentSession.seq}` })

    this.send({
      op: OP_RESUME,
      d: {
        token: this.props.token,
        session_id: this.currentSession.sessionId,
        seq: this.currentSession.seq,
      },
    })
  }
}
