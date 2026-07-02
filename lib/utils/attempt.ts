import { safeNormalizeError } from "@/utils/safe-normalize-error"

function isThenable(value: unknown): value is PromiseLike<unknown> {
  if (typeof value !== "object" && typeof value !== "function") return false
  if (value === null) return false
  return "then" in value && typeof value.then === "function"
}

/**
 * 関数を呼び出して throw / async reject を `T | Error` に変換する。
 *
 * - sync 関数を渡すと `T | Error` を返す
 * - Promise を返す関数を渡すと `Promise<T | Error>` を返す (caller は `await` する)
 * - `new X(...)` や `obj.method(...)` は `() => new X(...)` のようにアローで包む
 *
 * sync / async は arrow の戻り型から TS が推論する。
 * native Promise でない thenable (DI モックや promise ライブラリ) の reject も
 * `Promise.resolve` で吸収する — その代償として、`.then` メソッドを持つ純粋な値を
 * sync overload で返すことはできない (Promise 扱いになる)
 */
export function attempt<T>(fn: () => Promise<T>): Promise<T | Error>
export function attempt<T>(fn: () => T): T | Error
export function attempt<T>(fn: () => T | Promise<T>): T | Error | Promise<T | Error> {
  try {
    const result = fn()
    if (result instanceof Promise) {
      return result.catch((err: unknown): Error => safeNormalizeError({ value: err }))
    }
    if (isThenable(result)) {
      return Promise.resolve(result).then(
        (value): T | Error => value,
        (err: unknown): Error => safeNormalizeError({ value: err }),
      )
    }
    return result
  } catch (err) {
    return safeNormalizeError({ value: err })
  }
}
