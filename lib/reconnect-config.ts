import type { FlumeReconnectConfig, FlumeReconnectOptions } from "@/types"

const DEFAULTS: FlumeReconnectConfig = {
  maxAttempts: Infinity,
  baseDelay: 1000,
  maxDelay: 30_000,
}

/**
 * `baseDelay` として受理できる値のみ透過する。NaN / Infinity / 1ms 未満 /
 * 非数値 / 明示的 `undefined` (spread でデフォルトを潰すケース) は既定値へ落とす。
 * 0 や負値を許すと `maxAttempts: Infinity` と組み合わさって 0ms 再接続ホットループになる
 */
function sanitizeBaseDelay(value: number | undefined): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return DEFAULTS.baseDelay
  if (value < 1) return DEFAULTS.baseDelay
  return value
}

function sanitizeMaxDelay(value: number | undefined, baseDelay: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return Math.max(DEFAULTS.maxDelay, baseDelay)
  }
  if (value < baseDelay) return baseDelay
  return value
}

/** 1 以上の整数 or Infinity のみ透過。NaN / 0 / 負値 / 小数は既定 (Infinity) へ */
function sanitizeMaxAttempts(value: number | undefined): number {
  if (typeof value !== "number" || Number.isNaN(value)) return DEFAULTS.maxAttempts
  if (value === Infinity) return Infinity
  if (!Number.isInteger(value) || value < 1) return DEFAULTS.maxAttempts
  return value
}

/**
 * ユーザー入力の reconnect 指定を検証済み `FlumeReconnectConfig` へ解決する。
 * `false` / `undefined` は再接続無効 (null)。`true` は既定値。
 * オブジェクトはフィールドごとに検証し、不正値 (NaN / 負値 / Infinity delay 等) は
 * 既定値へフォールバックする — throw しない
 */
export function resolveFlumeReconnectConfig(
  input: boolean | FlumeReconnectOptions | undefined,
): FlumeReconnectConfig | null {
  if (input === false || input === undefined) return null

  if (input === true) return { ...DEFAULTS }

  const baseDelay = sanitizeBaseDelay(input.baseDelay)

  return {
    maxAttempts: sanitizeMaxAttempts(input.maxAttempts),
    baseDelay,
    maxDelay: sanitizeMaxDelay(input.maxDelay, baseDelay),
  }
}
