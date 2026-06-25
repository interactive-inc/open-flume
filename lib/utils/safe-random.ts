import type { FlumeRuntimeDeps } from "@/types"

type Props = {
  deps: Pick<FlumeRuntimeDeps, "random">
}

/**
 * `deps.random()` を保護する。throw / 範囲外値 / 非数値が返った場合は 0.5 を返す。
 * 0 以上 1 未満 (Math.random と同等) の値のみそのまま透過
 */
export function safeRandom(props: Props): number {
  try {
    const value = props.deps.random()
    if (typeof value !== "number" || !Number.isFinite(value)) return 0.5
    if (value < 0 || value >= 1) return 0.5
    return value
  } catch {
    return 0.5
  }
}
