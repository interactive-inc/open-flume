/** このモジュール自身が所有する値だけを再利用する。外部の freeze は信用しない。 */
const owned = new WeakSet<object>()

/**
 * 通常の object / array をコピーして凍結する。循環・共有参照を保持し、深さに依存せず処理する。
 * Error・Date・クラス等は外部の opaque な値として保持する。getter の失敗は呼出し境界で attempt する。
 */
export function snapshotValue<T>(input: T): T {
  const copies = new WeakMap<object, object>()
  const pending: { source: object; target: object }[] = []
  const copy = (value: unknown): unknown => {
    if (value === null || typeof value !== "object" || owned.has(value)) return value
    const existing = copies.get(value)
    if (existing) return existing
    const prototype = Object.getPrototypeOf(value)
    if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) return value
    const target: object = Array.isArray(value) ? [] : Object.create(prototype)
    if (Array.isArray(value)) Reflect.set(target, "length", value.length)
    copies.set(value, target)
    pending.push({ source: value, target })
    return target
  }
  const result = copy(input)
  for (let index = 0; index < pending.length; index++) {
    const { source, target } = pending[index]!
    for (const key of Reflect.ownKeys(source)) {
      if (!Object.getOwnPropertyDescriptor(source, key)?.enumerable) continue
      Object.defineProperty(target, key, {
        value: copy(Reflect.get(source, key)),
        enumerable: true,
      })
    }
  }
  // 読取の途中で失敗したグラフは owned に登録しない。
  for (const { target } of pending) {
    Object.freeze(target)
    owned.add(target)
  }
  return result as T
}
