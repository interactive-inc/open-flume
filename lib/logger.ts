import { FlumeHttpError } from "@/errors/http-error"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeOperationError } from "@/errors/operation-error"
import { FlumeStateError } from "@/errors/state-error"
import { resolveFlumeErrorCode } from "@/errors/resolve-error-code"
import type {
  FlumeLog,
  FlumeLogHandler,
  FlumeLogInput,
  FlumeLogLevel,
  FlumeRuntimeDeps,
} from "@/types"
import { attempt } from "@/utils/attempt"
import { snapshotValue } from "@/utils/snapshot-value"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"
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

  get handler(): FlumeLogHandler | undefined {
    return this.props.handler
  }

  child(source: string): FlumeLogger {
    return new FlumeLogger({ source, handler: this.props.handler, deps: this.props.deps })
  }

  private emit(level: FlumeLogLevel, input: FlumeLogInput): void {
    const handler = this.props.handler

    if (!handler) return

    const captured = attempt(() => {
      const { action, message, error, errorCode, detail } = input
      return snapshotValue<FlumeLog>({
        level,
        source: this.props.source,
        action,
        message,
        timestamp: safeNow({ deps: this.props.deps }),
        error,
        errorCode:
          errorCode ??
          (level === "error" || level === "warn" || error
            ? resolveFlumeErrorCode(action, error)
            : undefined),
        detail:
          (error instanceof FlumeConnectionError ||
            error instanceof FlumeHttpError ||
            error instanceof FlumeOperationError ||
            error instanceof FlumeStateError) &&
          error.context
            ? { ...error.context, ...detail }
            : detail,
      })
    })
    const log: FlumeLog =
      captured instanceof Error
        ? snapshotValue({
            level: "error",
            source: this.props.source,
            action: "log.snapshot.error",
            message: `Cannot capture log entry: ${safeErrorMessage({ error: captured })}`,
            errorCode: "FLUME_RUNTIME_FAILED",
            error: captured,
            timestamp: safeNow({ deps: this.props.deps }),
          })
        : captured

    safeInvokeCallback({ fn: () => handler(log), onError: () => {} })
  }
}
