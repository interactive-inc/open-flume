import type { FlumeLogger } from "@/logger"
import type { FlumeStatus, FlumeStatusHandler } from "@/types"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeInvokeCallback } from "@/utils/safe-invoke-callback"

type Props = {
  log: FlumeLogger
  onStatus?: FlumeStatusHandler
}

/**
 * Source の `currentStatus` と `onStatus` 通知を集約する。
 * 同一 (status, detail) の連続遷移は冪等に握り潰し、ユーザーコールバックは `safeInvokeCallback`
 * 経由で例外を隔離する
 */
export class FlumeStatusEmitter {
  private currentStatus: FlumeStatus = "disconnected"

  private currentDetail: string | null = null

  constructor(private readonly props: Props) {}

  get value(): FlumeStatus {
    return this.currentStatus
  }

  set(next: FlumeStatus, detail?: string): void {
    const normalizedDetail = detail ?? null

    if (this.currentStatus === next && this.currentDetail === normalizedDetail) return

    const prev = this.currentStatus
    const suffix = detail ? ` (${detail})` : ""

    this.props.log.info({
      action: "status",
      message: `${prev} → ${next}${suffix}`,
      detail: { from: prev, to: next, reason: normalizedDetail },
    })

    this.currentStatus = next
    this.currentDetail = normalizedDetail

    const onStatus = this.props.onStatus
    if (!onStatus) return

    safeInvokeCallback({
      fn: detail !== undefined ? () => onStatus(next, detail) : () => onStatus(next),
      onError: (error) => {
        this.props.log.error({
          action: "onStatus.error",
          message: safeErrorMessage({ error }),
          error,
        })
      },
    })
  }
}
