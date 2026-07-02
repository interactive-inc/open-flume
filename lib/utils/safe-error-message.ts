type Props = {
  error: unknown
}

/**
 * 任意の値から人が読めるメッセージ文字列を取り出す。
 * `Error.message` getter / `Symbol.toPrimitive` / `toString` / `valueOf` が throw しても固定文字列に fallback。
 * `instanceof` 自体が throw する値 (revoked Proxy 等) にも耐える。自身は決して throw しない
 */
export function safeErrorMessage(props: Props): string {
  try {
    if (props.error instanceof Error) {
      const message = props.error.message
      if (typeof message === "string") return message
      return "<non-string error message>"
    }
  } catch {
    return "<unreadable error message>"
  }

  try {
    return String(props.error)
  } catch {
    return "<unprintable error>"
  }
}
