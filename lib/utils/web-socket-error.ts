import { FlumeConnectionError } from "@/errors/connection-error"
import type { FlumeErrorContext } from "@/errors/error-context"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"

/** 接続先 URL や socket 自体は記録せず、runtime が提供した例外と段階を保持する。 */
export function webSocketError(props: {
  event: Event
  context: FlumeErrorContext
}): FlumeConnectionError {
  const cause = attempt(() => ("error" in props.event ? props.event.error : undefined))
  const message = attempt(() => ("message" in props.event ? props.event.message : undefined))
  const detail =
    typeof message === "string" && message
      ? message
      : cause !== undefined && cause !== null
        ? safeErrorMessage({ error: cause })
        : "runtime provided no diagnostic details"
  return new FlumeConnectionError(`WebSocket connection error: ${detail}`, {
    cause: cause ?? undefined,
    context: props.context,
  })
}
