const DEDUP_WINDOW_MS = 2 * 60 * 60 * 1000

/**
 * DST fall-back では同一壁時計分 (y/m/d/h/min) が 2 つの epoch に存在し、
 * 分単位の epoch ウォークが両方にマッチして二重発火する。fire 直後に計算した
 * 次ターゲットが直近 2 時間の fire のいずれかと同じ壁時計分なら重複と判定する。
 * 複数分にマッチする cron では巻き戻し後の最初の時刻と直前 fire の分が異なるため、
 * 直前 1 件でなく履歴を受け取る
 */
export function isDstDuplicateFire(firedTimes: ReadonlyArray<number>, nextTarget: number): boolean {
  for (let index = firedTimes.length - 1; index >= 0; index--) {
    const firedAt = firedTimes[index]
    if (firedAt === undefined) continue
    if (nextTarget <= firedAt) continue
    if (nextTarget - firedAt > DEDUP_WINDOW_MS) return false
    if (hasSameLocalMinute(firedAt, nextTarget)) return true
  }

  return false
}

function hasSameLocalMinute(firedAt: number, nextTarget: number): boolean {
  const fired = new Date(firedAt)
  const next = new Date(nextTarget)

  if (fired.getFullYear() !== next.getFullYear()) return false
  if (fired.getMonth() !== next.getMonth()) return false
  if (fired.getDate() !== next.getDate()) return false
  if (fired.getHours() !== next.getHours()) return false
  return fired.getMinutes() === next.getMinutes()
}
