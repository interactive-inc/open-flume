import { FlumeHttpError } from "@/errors/http-error"
import { safeErrorMessage } from "@/utils/safe-error-message"

type Props = {
  response: Response
  context: string
}

/**
 * `response.text()` を保護する。body 読み取り中の reject (接続切断 / 解凍失敗 / 二重消費) を
 * `FlumeHttpError` (status / cause 保持) に変換する。log には書かない (呼び出し側で書く)
 */
export async function safeReadText(props: Props): Promise<string | FlumeHttpError> {
  try {
    return await props.response.text()
  } catch (err) {
    return new FlumeHttpError({
      message: `${props.context}: failed to read body: ${safeErrorMessage({ error: err })}`,
      status: props.response.status,
      cause: err,
    })
  }
}
