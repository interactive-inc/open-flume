import type { z } from "zod/v4"
import type { FlumeGatewayMessageSchema } from "@/discord/discord-gateway-message-schema"
import type { FlumeGitHubNotificationSchema } from "@/github/github-notification-schema"
import type { FlumeSlackConnectionResponseSchema } from "@/slack/slack-connection-response-schema"
import type { FlumeSlackEnvelopeSchema } from "@/slack/slack-envelope-schema"

// Timer

export type FlumeTimerHandle = ReturnType<typeof setTimeout>

// Runtime DI

export type FlumeRuntimeDeps = {
  fetch(url: string | URL, init?: RequestInit): Promise<Response>
  WebSocket: new (url: string | URL) => WebSocket
  now(): number
  random(): number
  setTimeout(fn: () => void, ms: number): FlumeTimerHandle
  clearTimeout(id: FlumeTimerHandle): void
  setInterval(fn: () => void, ms: number): FlumeTimerHandle
  clearInterval(id: FlumeTimerHandle): void
}

// Event

export type FlumeSourceName = "discord" | "slack" | "github"

export type FlumeEvent = {
  source: FlumeSourceName
  type: string
  data: unknown
  meta: Record<string, string>
  receivedAt: number
}

export type FlumeHandler = (event: FlumeEvent) => void | Promise<void>

export type FlumeSource = {
  readonly name: FlumeSourceName
  start(handler: FlumeHandler): Promise<Error | null>
  stop(): Promise<void>
  status(): FlumeStatus
}

export type FlumeSourceStatus = {
  name: FlumeSourceName
  status: FlumeStatus
}

// Status

export type FlumeStatus =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"

export type FlumeStatusHandler = (status: FlumeStatus, detail?: string) => void

// Logging

export type FlumeLogLevel =
  | "debug"
  | "info"
  | "warn"
  | "error"

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
   * Bot token (`xoxb-`). Required — used by the host (e.g. funnel) to call
   * `auth.test` for self-detection and to post replies. Flume's Socket Mode
   * transport only needs `appToken` to open the socket, but every realistic
   * consumer needs the bot token too, so the type forces it to be present
   * rather than leaving it optional and failing at runtime.
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
