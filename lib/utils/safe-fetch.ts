import type { FlumeRuntimeDeps } from "@/types"
import { FlumeLogger } from "@/logger"

type Props = {
  fetch: FlumeRuntimeDeps["fetch"]
  url: string | URL
  init?: RequestInit
  log: FlumeLogger
}

/**
 * deps.fetch を try/catch で包み、ネットワーク例外を Error として返す。
 * HTTP ステータスの解釈や追加の副作用 (リトライ計数等) は呼び出し側の責務
 */
export async function safeFetch(props: Props): Promise<Response | Error> {
  try {
    return await props.fetch(props.url, props.init)
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    props.log.error({ action: "http.error", message: `network error: ${err.message}`, error: err })
    return err
  }
}
