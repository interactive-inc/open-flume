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

export type FlumeSourceName = "discord" | "slack" | "github"

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

export type FlumeEvent = FlumeDiscordEvent | FlumeSlackEvent | FlumeGitHubEvent

export type FlumeEventHandler = (event: FlumeEvent) => void | Promise<void>

// Status

export type FlumeStatus = "disconnected" | "connecting" | "connected" | "reconnecting"

export type FlumeStatusEvent = {
  source: string
  status: FlumeStatus
  detail?: string
}

export type FlumeStatusHandler = (event: FlumeStatusEvent) => void

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
  onStatus: FlumeSourceLocalStatusHandler
  reconnect: FlumeReconnectConfig | null
  /**
   * Flume.start() に渡された signal をそのまま転送する。
   * source 実装が自前で `fetch(url, { signal })` / `setTimeout` cancel / WS close を
   * host abort 経由で発火させたい時に使う (Flume 自身は最外殻で runStop を駆動するので
   * source は signal を無視しても動作的には停止する — 自然な伝播パスが欲しい場合のみ)。
   * Flume.options.signal が未設定なら省略される。
   */
  signal?: AbortSignal
}

// Source 構築 options — domain config のみ。
// cross-cutting (handler / onLog / onStatus / signal / deps / reconnect) は Flume 側で受け取る

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
}

export type FlumeGitHubSourceOptions = {
  token: string
  pollInterval?: number
}

// Zod inferred types

export type FlumeGatewayMessage = z.infer<typeof FlumeGatewayMessageSchema>

export type FlumeSlackEnvelope = z.infer<typeof FlumeSlackEnvelopeSchema>

export type FlumeSlackConnectionResponse = z.infer<typeof FlumeSlackConnectionResponseSchema>

export type FlumeGitHubNotification = z.infer<typeof FlumeGitHubNotificationSchema>
