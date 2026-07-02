const DEDUP_WINDOW_MS = 2 * 60 * 60 * 1000

/**
 * DST fall-back では同一壁時計分 (y/m/d/h/min) が 2 つの epoch に存在し、
 * 分単位の epoch ウォークが両方にマッチして二重発火する。fire 直後に計算した
 * 次ターゲットが「直前 fire と同じ壁時計分」かつ「2 時間以内」なら重複と判定する
 */
export function isDstDuplicateFire(firedAt: number, nextTarget: number): boolean {
  if (nextTarget <= firedAt) return false
  if (nextTarget - firedAt > DEDUP_WINDOW_MS) return false

  const fired = new Date(firedAt)
  const next = new Date(nextTarget)

  if (fired.getFullYear() !== next.getFullYear()) return false
  if (fired.getMonth() !== next.getMonth()) return false
  if (fired.getDate() !== next.getDate()) return false
  if (fired.getHours() !== next.getHours()) return false
  return fired.getMinutes() === next.getMinutes()
}
