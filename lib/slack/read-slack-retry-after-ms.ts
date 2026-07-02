import { attempt } from "@/utils/attempt"

type Props = {
  response: Response
}

/**
 * `Retry-After` ヘッダを ms で読む。整数秒表記のみ受理し、HTTP-date 形式・非数値・
 * headers 欠落 (モック Response 等) はすべて null。throw しない
 */
export function readSlackRetryAfterMs(props: Props): number | null {
  const raw = attempt(() => props.response.headers.get("retry-after"))

  if (raw instanceof Error) return null

  if (typeof raw !== "string") return null

  const trimmed = raw.trim()

  if (!/^\d+$/.test(trimmed)) return null

  const seconds = Number(trimmed)

  if (!Number.isFinite(seconds)) return null

  return seconds * 1000
}
