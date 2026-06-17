import type { FlumeLog, FlumeLogHandler, FlumeLogInput, FlumeLogLevel, FlumeRuntimeDeps } from "@/types"

type Props = {
  source: string
  handler?: FlumeLogHandler
  deps: Pick<FlumeRuntimeDeps, "now">
}

export class FlumeLogger {

  constructor(private readonly props: Props) {
    Object.freeze(this)
  }

  debug(entry: FlumeLogInput): void {
    this.emit("debug", entry)
  }

  info(entry: FlumeLogInput): void {
    this.emit("info", entry)
  }

  warn(entry: FlumeLogInput): void {
    this.emit("warn", entry)
  }

  error(entry: FlumeLogInput): void {
    this.emit("error", entry)
  }

  private emit(level: FlumeLogLevel, input: FlumeLogInput): void {
    if (!this.props.handler) return

    const log: FlumeLog = {
      level,
      source: this.props.source,
      action: input.action,
      message: input.message,
      timestamp: this.props.deps.now(),
      error: input.error,
      detail: input.detail,
    }

    this.props.handler(log)
  }
}
