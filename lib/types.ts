import type { z } from "zod/v4"
import type { FlumeLogger } from "@/logger"
import type { FlumeGatewayMessageSchema } from "@/discord/discord-gateway-message-schema"
import type { FlumeGitHubNotificationSchema } from "@/github/github-notification-schema"
import type { FlumeSlackConnectionResponseSchema } from "@/slack/slack-connection-response-schema"
import type { FlumeSlackEnvelopeSchema } from "@/slack/slack-envelope-schema"

// Timer

export type FlumeTimerHandle = unknown

// Runtime DI

export type FlumeRuntimeDeps = {
  fetch(url: string | URL, init?: RequestInit): Promise<Response>
  WebSocket: (new (url: string | URL) => WebSocket) | null
  now(): number
  random(): number
  setTimeout(fn: () => void, ms: number): FlumeTimerHandle
  clearTimeout(id: FlumeTimerHandle): void
  setInterval(fn: () => void, ms: number): FlumeTimerHandle
  clearInterval(id: FlumeTimerHandle): void
}

// Event (discriminated by source)

export type FlumeSourceName = "discord" | "slack" | "github" | "time"

export type FlumeDiscordEvent = {
  source: "discord"
  type: string
  data: Record<string, unknown>
  meta: Record<string, string>
  receivedAt: number
}

export type FlumeSlackEvent = {
  source: "slack"
  type: string
  data: Record<string, unknown>
  meta: Record<string, string>
  receivedAt: number
}

export type FlumeGitHubEvent = {
  source: "github"
  type: "notification"
  data: FlumeGitHubNotification
  meta: Record<string, string>
  receivedAt: number
}

export type FlumeTimeEvent = {
  source: "time"
  type: string
  data: Record<string, unknown>
  meta: Record<string, string>
  receivedAt: number
}

export type FlumeEvent = FlumeDiscordEvent | FlumeSlackEvent | FlumeGitHubEvent | FlumeTimeEvent

export type FlumeEventHandler = (event: FlumeEvent) => void | Promise<void>

// Unified stream (events + 全ログを 1 本の firehose に統合。使う側が kind / level で filter)

export type FlumeStreamItem = { kind: "event"; event: FlumeEvent } | { kind: "log"; log: FlumeLog }

export type FlumeStreamHandler = (item: FlumeStreamItem) => void

/**
 * `FlumeConfluence` が onEvent に渡す item。Flume 単体の `FlumeStreamItem` に
 * `groupId` (`add(id, ...)` で渡したグループ識別子) をスタンプしたもの。
 * 合流ストリームの provenance を呼び出し側で復元する必要がなくなる
 */
export type FlumeConfluenceItem = FlumeStreamItem & { readonly groupId: string }

export type FlumeConfluenceItemHandler = (item: FlumeConfluenceItem) => void

// Pull stream (FlumeRunning.stream() の async iterator オプション)

export type FlumeStreamOverflow = "drop-oldest" | "drop-newest"

export type FlumeStreamOptions = {
  /** バッファ上限 (既定 1000)。consumer が遅れて溢れたら onOverflow に従う */
  buffer?: number
  /** バッファ溢れ時の方針 (既定 "drop-oldest") */
  onOverflow?: FlumeStreamOverflow
}

// Status

export type FlumeStatus = "disconnected" | "connecting" | "connected" | "reconnecting"

export type FlumeSourceStatus = {
  /**
   * 多くは `FlumeSourceName` のいずれかだが、`source.name` getter が throw する
   * 第三者 FlumeSource 実装に備えて `string` まで広げてある (fallback で `"?"`)
   */
  source: string
  status: FlumeStatus
}

// Logging

export type FlumeLogLevel = "debug" | "info" | "warn" | "error"

export type FlumeLog = {
  level: FlumeLogLevel
  source: string
  action: string
  message: string
  error?: Error
  detail?: Record<string, unknown>
  timestamp: number
}

export type FlumeLogHandler = (log: FlumeLog) => void

/** error レベルのログだけを受け取る (Sentry など error 専用の送信先向け) */
export type FlumeErrorHandler = (log: FlumeLog) => void

export type FlumeLogInput = {
  action: string
  message: string
  error?: Error
  detail?: Record<string, unknown>
}

// Reconnect

export type FlumeReconnectOptions = {
  maxAttempts?: number
  baseDelay?: number
  maxDelay?: number
}

export type FlumeReconnectConfig = {
  maxAttempts: number
  baseDelay: number
  maxDelay: number
}

// Source start context (Flume → Source 内部 API)

export type FlumeSourceLocalStatusHandler = (status: FlumeStatus, detail?: string) => void

export type FlumeSourceStartContext = {
  onEvent: FlumeEventHandler
  log: FlumeLogger
  deps: FlumeRuntimeDeps
  /** Source 内部の status 遷移ブリッジ。Flume 公開 API に status callback は無く、遷移は log に出る */
  onStatus?: FlumeSourceLocalStatusHandler
  reconnect: FlumeReconnectConfig | null
  /**
   * Flume.start() に渡された signal をそのまま転送する。
   * source 実装が自前で `fetch(url, { signal })` / `setTimeout` cancel / WS close を
   * host abort 経由で発火させたい時に使う (Flume 自身は最外殻で runClose を駆動するので
   * source は signal を無視しても動作的には停止する — 自然な伝播パスが欲しい場合のみ)。
   * Flume.options.signal が未設定なら省略される。
   */
  signal?: AbortSignal
}

// Source 構築 options — domain config のみ。
// cross-cutting (onEvent firehose / onError / signal / deps / reconnect) は Flume 側で受け取る

export type FlumeDiscordSourceOptions = {
  token: string
  intents?: number
}

export type FlumeSlackSourceOptions = {
  appToken: string
  /**
   * Bot token (`xoxb-`). Slack Socket Mode (受信) には不要だが、ホスト側 (返信や
   * `auth.test` での self 検出) が必ず使うため型で保持を強制する
   */
  botToken: string
  /**
   * Optional frame-silence watchdog. When set, the Slack WebSocket is closed
   * and handed to the reconnect path after this many milliseconds without any
   * inbound frame.
   */
  idleTimeoutMs?: number | null
}

export type FlumeGitHubSourceOptions = {
  token: string
  pollInterval?: number
}

export type FlumeTimeTick = {
  /** cron がマッチした壁時計時刻 (epoch ms)。setTimeout の発火実時刻ではなく予定時刻 */
  firedAt: number
  cron: string
}

/**
 * tick ごとに emit するイベントの上書き内容。全フィールド optional。
 * 省略フィールドは既定値 (type: "tick" / data: tick 内容 / meta: { cron }) になる
 */
export type FlumeTimeMessage = {
  type?: string
  data?: Record<string, unknown>
  meta?: Record<string, string>
}

/**
 * 起動 / 終了をまたいだ状態を 1 つ載せる純粋な DI ポート。flume 内部で fs / db / network を
 * 触らないように、I/O の場所と方式は host が決める。load の失敗は null 復帰扱い、save の
 * 失敗は best-effort (source 側で log するが throw しない)
 */
export type FlumeStatePersister<S> = {
  load(): Promise<S | null>
  save(state: S): Promise<void>
}

/** `FlumeTimeSource` の statePersister が保存する形 */
export type FlumeTimeSourceState = {
  readonly lastFiredAt: number
}

/**
 * `FlumeTimeSource` の起動時 catch-up 方針。statePersister が読めた lastFiredAt から
 * 現在時刻までに過ぎ去った cron マッチを再発火するかどうかを決める。
 *  - "off"      : 何もしない (既定。後方互換)
 *  - "lastOnly" : 直近に過ぎ去ったマッチを 1 件だけ再発火
 *  - "missed"   : maxWindowMs (既定 24h) 以内のすべての過ぎ去ったマッチを順に再発火
 */
export type FlumeCatchupPolicy =
  | { readonly mode: "off" }
  | { readonly mode: "lastOnly" }
  | { readonly mode: "missed"; readonly maxWindowMs?: number }

export type FlumeTimeSourceOptions = {
  /** 5 フィールド cron 式 (minute hour day-of-month month day-of-week)。壁時計 (local time) 基準 */
  cron: string
  message?: (tick: FlumeTimeTick) => FlumeTimeMessage
  /**
   * 起動 / 終了をまたいで `lastFiredAt` を覚えておく口。未指定なら catchup は無効化される
   * (lastFiredAt が分からないので)。flume は fs / db を触らないので host が実装を渡す
   */
  statePersister?: FlumeStatePersister<FlumeTimeSourceState>
  /** statePersister が読めた lastFiredAt を元に過去 tick を再発火する方針 (既定 "off") */
  catchupPolicy?: FlumeCatchupPolicy
}

// Zod inferred types

export type FlumeGatewayMessage = z.infer<typeof FlumeGatewayMessageSchema>

export type FlumeSlackEnvelope = z.infer<typeof FlumeSlackEnvelopeSchema>

export type FlumeSlackConnectionResponse = z.infer<typeof FlumeSlackConnectionResponseSchema>

export type FlumeGitHubNotification = z.infer<typeof FlumeGitHubNotificationSchema>
