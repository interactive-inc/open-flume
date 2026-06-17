import type {
  FlumeDiscordSourceOptions,
  FlumeGitHubSourceOptions,
  FlumeLogHandler,
  FlumeReconnectOptions,
  FlumeRuntimeDeps,
  FlumeSlackSourceOptions,
  FlumeStatusHandler,
} from "@/types"
import { createFlumeDefaultDeps } from "@/deps"
import { FlumeDiscordSource } from "@/discord/discord-source"
import { FlumeSlackSource } from "@/slack/slack-source"
import { FlumeGitHubSource } from "@/github/github-source"

type Props = {
  onLog?: FlumeLogHandler
  onStatus?: FlumeStatusHandler
  reconnect?: boolean | FlumeReconnectOptions
  signal?: AbortSignal
  deps?: Partial<FlumeRuntimeDeps>
}

/**
 * 共有設定を持つ DI コンテナ。各 Source に共通の deps / logging / reconnect を注入する
 */
export class Flume {

  private readonly resolvedDeps: FlumeRuntimeDeps

  constructor(private readonly props: Props) {
    this.resolvedDeps = { ...createFlumeDefaultDeps(), ...props.deps }
  }

  discord(options: { token: string; intents?: number }): FlumeDiscordSource {
    const merged: FlumeDiscordSourceOptions = {
      onLog: this.props.onLog,
      onStatus: this.props.onStatus,
      reconnect: this.props.reconnect,
      signal: this.props.signal,
      deps: this.resolvedDeps,
      ...options,
    }

    return new FlumeDiscordSource(merged)
  }

  slack(options: { appToken: string; botToken?: string }): FlumeSlackSource {
    const merged: FlumeSlackSourceOptions = {
      onLog: this.props.onLog,
      onStatus: this.props.onStatus,
      reconnect: this.props.reconnect,
      signal: this.props.signal,
      deps: this.resolvedDeps,
      ...options,
    }

    return new FlumeSlackSource(merged)
  }

  github(options: { token: string; pollInterval?: number }): FlumeGitHubSource {
    const merged: FlumeGitHubSourceOptions = {
      onLog: this.props.onLog,
      onStatus: this.props.onStatus,
      reconnect: this.props.reconnect,
      signal: this.props.signal,
      deps: this.resolvedDeps,
      ...options,
    }

    return new FlumeGitHubSource(merged)
  }
}
