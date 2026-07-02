import { FlumeHttpError } from "@/errors/http-error"
import { attempt } from "@/utils/attempt"
import { safeErrorMessage } from "@/utils/safe-error-message"

type Props = {
  response: Response
  context: string
}

/**
 * `response.text()` を保護する。body 読み取り中の reject (接続切断 / 解凍失敗 / 二重消費) を
 * `FlumeHttpError` (status / cause 保持) に変換する。DI モックの `status` getter が throw
 * しても reject しない。log には書かない (呼び出し側で書く)
 */
export async function safeReadText(props: Props): Promise<string | FlumeHttpError> {
  try {
    return await props.response.text()
  } catch (err) {
    const statusResult = attempt(() => props.response.status)
    const status = typeof statusResult === "number" ? statusResult : 0

    return new FlumeHttpError({
      message: `${props.context}: failed to read body: ${safeErrorMessage({ error: err })}`,
      status,
      cause: err,
    })
  }
}
