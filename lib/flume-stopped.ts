import type { FlumeSourceStatus } from "@/types"

type Props = {
  finalStatuses: ReadonlyArray<FlumeSourceStatus>
}

/**
 * 停止済みの終端状態。最終ステータスのスナップショットのみ観測できる
 */
export class FlumeStopped {

  constructor(private readonly props: Props) {
    Object.freeze(this)
  }

  statuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.props.finalStatuses
  }
}
