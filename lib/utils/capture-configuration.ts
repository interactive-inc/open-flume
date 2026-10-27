import { FlumeStartError } from "@/errors/start-error"
import { attempt } from "@/utils/attempt"

/** 設定値を一度だけ取り込む。呼出し側のオブジェクトや IO ポート自体は凍結しない。 */
export function captureConfiguration<T extends object>(
  owner: string,
  read: () => T,
): Readonly<T> | FlumeStartError {
  const value = attempt(() => Object.freeze(read()))
  return value instanceof Error
    ? new FlumeStartError(`${owner}: cannot read configuration`, {
        errorCode: "FLUME_RUNTIME_FAILED",
        cause: value,
      })
    : value
}
