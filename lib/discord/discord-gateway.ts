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
  /** WebSocket 生成から READY/RESUMED までの上限 (ms)。非有限・0 以下は既定 30_000 に丸める */
  handshakeTimeoutMs?: number
  /** 前回接続から引き継ぐ session。`canResume()` なら HELLO 後に IDENTIFY でなく RESUME を送る */
  session?: FlumeDiscordGatewaySession
  onDispatch: (event: string, data: Record<string, unknown>) => void
  onStatus: (status: "connected" | "disconnected") => void
  onLog?: FlumeLogHandler
  deps: Deps
}

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

const GATEWAY_VERSION = "10"

const GATEWAY_ENCODING = "json"

const DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000

// 強制 close (zombie / handshake timeout) 後、close event が届かない場合に teardown を合成するまでの猶予
const FORCED_CLOSE_FALLBACK_MS = 5_000

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

// session が無効化される close code。次の接続は RESUME せず IDENTIFY し直す
const NON_RESUMABLE_CLOSE_CODES = new Set<number>([
  4007, // Invalid seq
  4009, // Session timed out
])

/**
 * Discord Gateway v10 の最小実装。HELLO -> IDENTIFY/RESUME -> READY/RESUMED -> dispatch を扱う。
 * READY 後の WebSocket 切断のみ `onStatus("disconnected")` を発火し source 側で再接続する。
 * 終端 close code (4004 / 401x) を受けた場合は stopped 化して再接続を抑止。
 * close code 4007/4009 と INVALID_SESSION (resumable=false) では session を破棄する
 * (source が次の gateway へ session を引き継ぐ前にここでリセットしておく)。
 * IO 境界は全て `attempt` 経由で扱い、コンストラクタ throw も `FlumeConnectionError` として返す
 * (`connect()` は決して reject しない)
 */
export class FlumeDiscordGateway {
  private readonly log: FlumeLogger

  private ws: WebSocket | null = null

  private heartbeat: FlumeDiscordHeartbeat | null = null

  private currentSession: FlumeDiscordGatewaySession

  private isStoppedFlag = false

  private hasConnected = false

  private pendingResolve: ((value: FlumeConnectionError | null) => void) | null = null

  private pendingResolved = false

  private invalidSessionTimer: FlumeTimerHandle | null = null

  private handshakeTimer: FlumeTimerHandle | null = null

  private forcedCloseFallbackTimer: FlumeTimerHandle | null = null

  private teardownDone = false

  constructor(private readonly props: Props) {
    this.currentSession = props.session ?? FlumeDiscordGatewaySession.empty()
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

    const target = this.resolveTargetUrl(url)
    const hostResult = attempt(() => new URL(target).hostname)
    const host = hostResult instanceof Error ? "unknown" : hostResult
    this.log.info({ action: "connect.start", message: `host=${host}` })
    this.pendingResolved = false
    this.hasConnected = false
    this.teardownDone = false

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
        this.closeSocket({ ws: socket })
        this.ws = null
        this.pendingResolved = true
        resolve(error)
        return
      }

      this.armHandshakeTimer()
    })
  }

  disconnect(): void {
    this.log.info({ action: "disconnect", message: "shutting down gateway" })
    this.isStoppedFlag = true
    this.heartbeat?.stop()
    this.clearInvalidSessionTimer()
    this.clearForcedCloseFallbackTimer()

    // close event が来ない socket でも pending な connect() を確実に解放する
    this.completeConnect(new FlumeConnectionError("gateway disconnected before ready"))

    this.closeSocket({ ws: this.ws, code: 1000, reason: "shutdown" })
    this.ws = null
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WS_OPEN
  }

  /**
   * resume 時は Discord の `resume_gateway_url` に query (?v=10&encoding=json) を付与する
   * (Discord が返す URL に query は付かない)。URL が壊れている場合は session を破棄して
   * 通常の Gateway URL で IDENTIFY し直す
   */
  private resolveTargetUrl(url?: string): string {
    if (url === undefined) return GATEWAY_URL

    const rebuilt = attempt(() => {
      const parsed = new URL(url)
      parsed.searchParams.set("v", GATEWAY_VERSION)
      parsed.searchParams.set("encoding", GATEWAY_ENCODING)
      return parsed.toString()
    })
    if (rebuilt instanceof Error) {
      this.log.warn({
        action: "resume.url.invalid",
        message: `resume url unparseable, dropping session and identifying fresh: ${safeErrorMessage({ error: rebuilt })}`,
      })
      this.currentSession = this.currentSession.withReset()
      return GATEWAY_URL
    }
    return rebuilt
  }

  private resolveHandshakeTimeoutMs(): number {
    const configured = this.props.handshakeTimeoutMs
    if (typeof configured !== "number" || !Number.isFinite(configured) || configured <= 0) {
      return DEFAULT_HANDSHAKE_TIMEOUT_MS
    }
    return configured
  }

  private armHandshakeTimer(): void {
    this.clearHandshakeTimer()

    const timeoutMs = this.resolveHandshakeTimeoutMs()
    const timerResult = attempt(() =>
      this.props.deps.setTimeout(() => {
        this.handshakeTimer = null
        this.onHandshakeTimeout(timeoutMs)
      }, timeoutMs),
    )
    if (timerResult instanceof Error) {
      this.log.error({
        action: "handshake.timer.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.handshakeTimer = null
      return
    }
    this.handshakeTimer = timerResult
  }

  private clearHandshakeTimer(): void {
    if (this.handshakeTimer === null) return

    const handle = this.handshakeTimer
    const result = attempt(() => this.props.deps.clearTimeout(handle))
    if (result instanceof Error) {
      this.log.error({
        action: "handshake.timer.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.handshakeTimer = null
  }

  /**
   * READY/RESUMED が期限内に来なかった half-open socket。connect() を Error で解放して
   * source の再接続経路に乗せ、socket は強制 close する (close event が来なければ
   * fallback が teardown を合成する)
   */
  private onHandshakeTimeout(timeoutMs: number): void {
    if (this.pendingResolved) return

    const error = new FlumeConnectionError(
      `handshake timeout after ${timeoutMs}ms (no READY/RESUMED)`,
    )
    this.log.error({ action: "handshake.timeout", message: safeErrorMessage({ error }), error })
    this.completeConnect(error)
    this.forceClose({ code: 4000, reason: "handshake timeout" })
  }

  /**
   * zombie / handshake timeout / malformed HELLO で自発的に接続を落とす。死んだ TCP 経路では
   * close event が届かないことがあるため fallback timer で teardown を保証する。heartbeat は
   * 即座に止めて onZombie が interval ごとに再発火するのを防ぐ。close code は resume 可能な
   * 4000 を使う (4009 は session 無効化コードと衝突する)
   */
  private forceClose(input: { code: number; reason: string }): void {
    this.heartbeat?.stop()
    this.closeSocket({ ws: this.ws, code: input.code, reason: input.reason })
    this.armForcedCloseFallback()
  }

  private armForcedCloseFallback(): void {
    if (this.teardownDone) return
    if (this.forcedCloseFallbackTimer !== null) return

    const timerResult = attempt(() =>
      this.props.deps.setTimeout(() => {
        this.forcedCloseFallbackTimer = null
        this.synthesizeTeardown()
      }, FORCED_CLOSE_FALLBACK_MS),
    )
    if (timerResult instanceof Error) {
      this.log.error({
        action: "ws.close.fallback.schedule.error",
        message: safeErrorMessage({ error: timerResult }),
        error: timerResult,
      })
      this.forcedCloseFallbackTimer = null
      return
    }
    this.forcedCloseFallbackTimer = timerResult
  }

  private clearForcedCloseFallbackTimer(): void {
    if (this.forcedCloseFallbackTimer === null) return

    const handle = this.forcedCloseFallbackTimer
    const result = attempt(() => this.props.deps.clearTimeout(handle))
    if (result instanceof Error) {
      this.log.error({
        action: "ws.close.fallback.clear.error",
        message: safeErrorMessage({ error: result }),
        error: result,
      })
    }
    this.forcedCloseFallbackTimer = null
  }

  /**
   * 強制 close 後に close event が届かなかった場合の合成 teardown。`onClose` と排他で
   * 一度だけ実行する。READY 後なら `onStatus("disconnected")` で source の再接続に繋ぐ
   * (READY 前は connect() の Error 解決が既に再接続を駆動しているため通知しない)
   */
  private synthesizeTeardown(): void {
    if (this.teardownDone) return
    this.teardownDone = true

    this.log.warn({
      action: "ws.close.synthesized",
      message: "close event not received after forced close, synthesizing teardown",
    })

    this.ws = null
    this.heartbeat?.stop()
    this.clearInvalidSessionTimer()

    if (!this.pendingResolved) {
      this.completeConnect(new FlumeConnectionError("WebSocket force-closed (no close event)"))
    }

    if (this.hasConnected && !this.isStoppedFlag) {
      this.props.onStatus("disconnected")
    }
  }

  private completeConnect(error: FlumeConnectionError | null): void {
    this.clearHandshakeTimer()

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

    if (typeof parsed.s === "number") {
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
    const rawInterval = d === null ? null : d.heartbeat_interval

    if (typeof rawInterval !== "number" || !Number.isFinite(rawInterval) || rawInterval <= 0) {
      // interval 0 のまま setInterval すると heartbeat flood になるためプロトコルエラー扱い
      const error = new FlumeConnectionError(
        "malformed HELLO: heartbeat_interval is not a finite number > 0",
      )
      this.log.error({
        action: "gateway.hello.invalid",
        message: safeErrorMessage({ error }),
        error,
        detail: { intervalType: typeof rawInterval },
      })
      this.completeConnect(error)
      this.forceClose({ code: 4000, reason: "malformed HELLO" })
      return
    }

    const interval = rawInterval
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
        this.forceClose({ code: 4000, reason: "zombie connection" })
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
    this.clearForcedCloseFallbackTimer()

    if (this.teardownDone) {
      // fallback が teardown を合成済み。遅れて届いた close は二重通知になるため無視する
      this.log.debug({
        action: "ws.close.stale",
        message: `ignored close after synthesized teardown (code=${ev.code})`,
        detail: { code: ev.code },
      })
      return
    }
    this.teardownDone = true

    const terminal = TERMINAL_CLOSE_CODES.has(ev.code)
    this.log.info({
      action: "ws.close",
      message: `code=${ev.code} reason=${ev.reason || "none"}${terminal ? " (terminal)" : ""}`,
      detail: { code: ev.code, reason: ev.reason, terminal },
    })

    this.ws = null
    this.heartbeat?.stop()
    this.clearInvalidSessionTimer()

    if (NON_RESUMABLE_CLOSE_CODES.has(ev.code)) {
      // onStatus より前に破棄する: source はこの後の disconnected 通知で session を捕捉する
      this.log.info({
        action: "session.reset",
        message: `close code ${ev.code} invalidates session, next attempt will identify`,
        detail: { code: ev.code },
      })
      this.currentSession = this.currentSession.withReset()
    }

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
