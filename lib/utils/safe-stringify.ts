import { FlumeParseError } from "@/errors/parse-error"
import { attempt } from "@/utils/attempt"

/**
 * `JSON.stringify` を `string | Error` に変換するラッパ。
 * cyclic / BigInt / throwing toJSON など標準が throw するケースを Error として返す。
 * `undefined` / function / symbol は `JSON.stringify` が (型定義に反して) `undefined` を
 * 返すため、これも Error に正規化して戻り値を必ず string にする
 */
export function safeStringify(value: unknown): string | Error {
  const result = attempt(() => JSON.stringify(value))
  if (result instanceof Error) return result
  if (typeof result !== "string") {
    return new FlumeParseError("value is not JSON-serializable (undefined / function / symbol)")
  }
  return result
}
