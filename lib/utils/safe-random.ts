import type { FlumeRuntimeDeps } from "@/types"

type Props = {
  deps: Pick<FlumeRuntimeDeps, "random">
}

/**
 * `deps.random()` を保護する。throw / 範囲外値 / 非数値が返った場合は `Math.random()` へ
 * フォールバックする。0 以上 1 未満 (Math.random と同等) の値のみそのまま透過。
 * `Math.random` 自体まで壊れている病的環境でのみ 0.5 を返す
 */
export function safeRandom(props: Props): number {
  try {
    const value = props.deps.random()
    if (typeof value === "number" && Number.isFinite(value) && value >= 0 && value < 1) {
      return value
    }
  } catch {
    // deps.random の throw は Math.random フォールバックへ
  }

  try {
    const native = Math.random()
    if (typeof native === "number" && Number.isFinite(native) && native >= 0 && native < 1) {
      return native
    }
    return 0.5
  } catch {
    return 0.5
  }
}
