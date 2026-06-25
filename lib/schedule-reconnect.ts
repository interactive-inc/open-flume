import type { FlumeStatus } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"
import { FlumeConnectionError } from "@/errors/connection-error"
import { safeErrorMessage } from "@/utils/safe-error-message"

type Props = {
  reconnector: FlumeReconnector | null
  log: FlumeLogger
  setStatus: (status: FlumeStatus) => void
  retry: () => void
}

/**
 * 接続が落ちた際の共通再接続スケジューラ。
 * 再接続の設定状況 (無効 / 中止 / 試行尽き) を見極めてからステータス遷移する。
 * - reconnector が無ければ reconnect.disabled を info ログし disconnected へ
 * - cancel 済みなら reconnect.aborted を info ログし disconnected へ
 * - schedule() が -1 を返したら reconnect.exhausted を error ログし disconnected へ
 * - それ以外は reconnecting へ遷移し reconnect.scheduled を info ログ
 */
export function scheduleFlumeReconnect(props: Props): void {
  if (!props.reconnector) {
    props.log.info({
      action: "reconnect.disabled",
      message: "reconnect is disabled, staying disconnected",
    })
    props.setStatus("disconnected")
    return
  }

  if (props.reconnector.aborted) {
    props.log.info({
      action: "reconnect.aborted",
      message: "reconnector cancelled, staying disconnected",
    })
    props.setStatus("disconnected")
    return
  }

  const delay = props.reconnector.schedule(props.retry)

  if (delay === -1) {
    const error = new FlumeConnectionError(
      `reconnect exhausted after ${props.reconnector.attempt} attempts`,
    )
    props.log.error({
      action: "reconnect.exhausted",
      message: safeErrorMessage({ error }),
      error,
    })
    props.setStatus("disconnected")
    return
  }

  props.setStatus("reconnecting")
  props.log.info({
    action: "reconnect.scheduled",
    message: `next attempt in ${Math.round(delay)}ms`,
    detail: { attempt: props.reconnector.attempt, delayMs: Math.round(delay) },
  })
}
