import type { FlumeStatus } from "@/types"
import { FlumeLogger } from "@/logger"
import { FlumeReconnector } from "@/reconnector"

type Props = {
  reconnector: FlumeReconnector | null
  log: FlumeLogger
  setStatus: (status: FlumeStatus) => void
  retry: () => void
}

/**
 * 接続が落ちた際の共通再接続スケジューラ。reconnector の状態を見て次回試行を予約し、
 * 試行回数が尽きていれば disconnected に落とす
 */
export function scheduleFlumeReconnect(props: Props): void {
  if (!props.reconnector || props.reconnector.aborted) {
    props.setStatus("disconnected")
    return
  }

  props.setStatus("reconnecting")

  const delay = props.reconnector.schedule(props.retry)

  if (delay === -1) {
    props.log.error({ action: "reconnect.exhausted", message: `gave up after ${props.reconnector.attempt} attempts` })
    props.setStatus("disconnected")
    return
  }

  props.log.info({ action: "reconnect.scheduled", message: `next attempt in ${Math.round(delay)}ms` })
}
