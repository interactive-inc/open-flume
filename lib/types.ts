import type { z } from "zod/v4"
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

export type FlumeHandler = (event: FlumeEvent) => void | Promise<void>

export type FlumeSourceStartOptions = {
  signal?: AbortSignal
}

export type FlumeSource = {
  readonly name: FlumeSourceName
  start(handler: FlumeHandler, options?: FlumeSourceStartOptions): Promise<Error | null>
  stop(): Promise<void>
  status(): FlumeStatus
}

export type FlumeSourceStatus = {
  /**
   * 多くは `FlumeSourceName` のいずれかだが、`source.name` getter が throw する
   * 第三者 `FlumeSource` 実装に備えて `string` まで広げてある (fallback で `"?"`)
   */
  source: string
  status: FlumeStatus
}

// Status

export type FlumeStatus = "disconnected" | "connecting" | "connected" | "reconnecting"

export type FlumeStatusHandler = (status: FlumeStatus, detail?: string) => void

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

// Source options

export type FlumeSourceOptions = {
  reconnect?: boolean | FlumeReconnectOptions
  onStatus?: FlumeStatusHandler
  onLog?: FlumeLogHandler
  signal?: AbortSignal
  deps?: FlumeRuntimeDeps
}

export type FlumeDiscordSourceOptions = FlumeSourceOptions & {
  token: string
  intents?: number
}

export type FlumeSlackSourceOptions = FlumeSourceOptions & {
  appToken: string
  /**
   * Bot token (`xoxb-`). Slack の Socket Mode (受信) には不要だが、ホスト側 (返信や
   * `auth.test` での self 検出) が必ず使うため型で保持を強制する
   */
  botToken: string
}

export type FlumeGitHubSourceOptions = FlumeSourceOptions & {
  token: string
  pollInterval?: number
}

// Zod inferred types

export type FlumeGatewayMessage = z.infer<typeof FlumeGatewayMessageSchema>

export type FlumeSlackEnvelope = z.infer<typeof FlumeSlackEnvelopeSchema>

export type FlumeSlackConnectionResponse = z.infer<typeof FlumeSlackConnectionResponseSchema>

export type FlumeGitHubNotification = z.infer<typeof FlumeGitHubNotificationSchema>
