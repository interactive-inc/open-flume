import type { FlumeRuntimeDeps } from "@/types"

type Props = {
  deps: Pick<FlumeRuntimeDeps, "now">
}

/**
 * `deps.now()` を保護する。throw / 非数値 / 非有限値が返った場合は `Date.now()` へ
 * フォールバックする (0 を返すと epoch 1970 が TTL / cron / レート計算へ伝播するため)。
 * `Date.now` 自体まで壊れている病的環境でのみ 0 を返す。
 * IO 境界のため呼び出し側はこの戻り値を信頼できる
 */
export function safeNow(props: Props): number {
  try {
    const value = props.deps.now()
    if (typeof value === "number" && Number.isFinite(value)) return value
  } catch {
    // deps.now の throw は Date.now フォールバックへ
  }

  try {
    return Date.now()
  } catch {
    return 0
  }
}
