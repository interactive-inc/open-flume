import type { FlumeSourceStatus } from "@/types"

export type FlumeStopError = {
  source: string
  error: Error
}

type Props = {
  finalStatuses: ReadonlyArray<FlumeSourceStatus>
  stopErrors: ReadonlyArray<FlumeStopError>
}

/**
 * 停止済みの終端状態。最終ステータスと、停止時に source.disconnect が throw した
 * エラー一覧を観測できる。`errors()` を読めば `onLog` を grep せずに「どの source が
 * きれいに stop したか / どれが失敗したか」を直接判定できる
 */
export class FlumeStopped {
  readonly kind = "stopped" as const

  constructor(private readonly props: Props) {
    Object.freeze(this)
  }

  statuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.props.finalStatuses
  }

  /**
   * `runStop` 中に `source.stop()` が rejected で settle した source の名前と
   * 正規化済み Error の組。`onLog` の `flume.stop.failed` と 1:1 対応する。
   * 全 source が clean stop した場合は空配列。
   */
  errors(): ReadonlyArray<FlumeStopError> {
    return this.props.stopErrors
  }
}
