import { safeErrorMessage } from "@/utils/safe-error-message"

type Props = {
  value: unknown
}

/**
 * 任意の値を `Error` インスタンスへ正規化する。すでに Error ならそのまま返し、
 * それ以外は `safeErrorMessage` で安全な文字列化を経由して new Error する。
 * Error コンストラクタ自体が throw する病的環境でも fallback を返し、決して throw しない
 */
export function safeNormalizeError(props: Props): Error {
  if (props.value instanceof Error) return props.value

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
