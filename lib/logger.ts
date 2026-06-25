import type {
  FlumeLog,
  FlumeLogHandler,
  FlumeLogInput,
  FlumeLogLevel,
  FlumeRuntimeDeps,
} from "@/types"
import { safeNow } from "@/utils/safe-now"

type Props = {
  source: string
  handler?: FlumeLogHandler
  deps: Pick<FlumeRuntimeDeps, "now">
}

/**
 * 構造化ログを onLog に流す。handler が throw / reject してもループは継続する
 */
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
    const handler = this.props.handler

    if (!handler) return

    const log: FlumeLog = {
      level,
      source: this.props.source,
      action: input.action,
      message: input.message,
      timestamp: safeNow({ deps: this.props.deps }),
      error: input.error,
      detail: input.detail,
    }

    try {
      Promise.resolve(handler(log)).catch(() => {})
    } catch {
      // handler が同期 throw してもロギングループは止めない
    }
  }
}
