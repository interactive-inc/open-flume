import type { FlumeSourceStatus } from "@/types"

export type FlumeCloseError = {
  source: string
  error: Error
}

type Props = {
  finalStatuses: ReadonlyArray<FlumeSourceStatus>
  closeErrors: ReadonlyArray<FlumeCloseError>
}

/**
 * 停止済みの終端状態。最終ステータスと、停止時に source.disconnect が throw した
 * エラー一覧を観測できる。`errors()` を読めば `onLog` を grep せずに「どの source が
 * きれいに close したか / どれが失敗したか」を直接判定できる
 */
export class FlumeClosed {
  readonly kind = "closed" as const

  private readonly finalStatuses: ReadonlyArray<FlumeSourceStatus>

  private readonly closeErrors: ReadonlyArray<FlumeCloseError>

  constructor(props: Props) {
    this.finalStatuses = Object.freeze(
      props.finalStatuses.map((status) => Object.freeze({ ...status })),
    )
    this.closeErrors = Object.freeze(
      props.closeErrors.map((closeError) => Object.freeze({ ...closeError })),
    )
    Object.freeze(this)
  }

  statuses(): ReadonlyArray<FlumeSourceStatus> {
    return this.finalStatuses
  }

  /**
   * `runClose` 中に `source.stop()` が rejected で settle した source の名前と
   * 正規化済み Error の組。`onEvent` firehose の `flume.close.failed` log と 1:1 対応する。
   * 全 source が clean close した場合は空配列。
   */
  errors(): ReadonlyArray<FlumeCloseError> {
    return this.closeErrors
  }
}
