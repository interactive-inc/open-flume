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

    const log: FlumeLog = {
      level,
      source: this.props.source,
      action: input.action,
      message: input.message,
      timestamp: safeNow({ deps: this.props.deps }),
      error: input.error,
      errorCode:
        input.errorCode ??
        (level === "error" || level === "warn" || input.error
          ? resolveFlumeErrorCode(input.action, input.error)
          : undefined),
      detail:
        (input.error instanceof FlumeConnectionError ||
          input.error instanceof FlumeHttpError ||
          input.error instanceof FlumeOperationError ||
          input.error instanceof FlumeStateError) &&
        input.error.context
          ? { ...input.error.context, ...input.detail }
          : input.detail,
    }

    safeInvokeCallback({ fn: () => handler(log), onError: () => {} })
  }
}
