import { attempt } from "@/utils/attempt"

/**
 * `JSON.stringify` を `string | Error` に変換するだけのラッパ。
 * cyclic / BigInt / throwing toJSON など標準が throw するケースを Error として返す
 */
export function safeStringify(value: unknown): string | Error {
  return attempt(() => JSON.stringify(value))
}
