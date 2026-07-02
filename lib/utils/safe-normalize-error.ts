import { safeErrorMessage } from "@/utils/safe-error-message"

type Props = {
  value: unknown
}

/**
 * 任意の値を `Error` インスタンスへ正規化する。すでに Error ならそのまま返し、
 * それ以外は `safeErrorMessage` で安全な文字列化を経由して new Error する。
 * `instanceof` 自体が throw する値 (revoked Proxy 等) や Error コンストラクタが throw する
 * 病的環境でも fallback を返し、決して throw しない
 */
export function safeNormalizeError(props: Props): Error {
  try {
    if (props.value instanceof Error) return props.value
  } catch {
    // revoked Proxy 等は文字列化経由の正規化へ
  }

  const message = safeErrorMessage({ error: props.value })

  try {
    return new Error(message)
  } catch {
    try {
      return new Error("unknown error")
    } catch {
      const fallback: Error = Object.create(Error.prototype)
      fallback.name = "Error"
      fallback.message = "unknown error"
      return fallback
    }
  }
}
