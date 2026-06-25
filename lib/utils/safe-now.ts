import type { FlumeRuntimeDeps } from "@/types"

type Props = {
  deps: Pick<FlumeRuntimeDeps, "now">
}

/**
 * `deps.now()` を保護する。throw / 非数値が返った場合は 0 を返す。
 * IO 境界のため呼び出し側はこの戻り値を信頼できる
 */
export function safeNow(props: Props): number {
  try {
    const value = props.deps.now()
    if (typeof value !== "number" || !Number.isFinite(value)) return 0
    return value
  } catch {
    return 0
  }
}
