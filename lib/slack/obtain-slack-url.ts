import type { FlumeLogHandler, FlumeRuntimeDeps } from "@/types"
import { FlumeSlackConnectionResponseSchema } from "@/slack/slack-connection-response-schema"
import { FlumeConnectionError } from "@/errors/connection-error"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeParseError } from "@/errors/parse-error"
import { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"
import { isRecord } from "@/utils/is-record"
import { safeErrorMessage } from "@/utils/safe-error-message"
import { safeJsonParse } from "@/utils/safe-json-parse"
import { safeNormalizeError } from "@/utils/safe-normalize-error"
import { safeReadText } from "@/utils/safe-read-text"

type Props = {
  appToken: string
  signal?: AbortSignal
  onLog?: FlumeLogHandler
  deps: Pick<FlumeRuntimeDeps, "fetch" | "now">
}

const URL_ENDPOINT = "https://slack.com/api/apps.connections.open"

export async function obtainSlackUrl(
  props: Props,
): Promise<string | FlumeHttpError | FlumeConnectionError> {
  const log = new FlumeLogger({ source: "slack.url", handler: props.onLog, deps: props.deps })

  log.debug({ action: "http.request", message: `POST ${URL_ENDPOINT}` })

  const response = await attempt(() =>
    props.deps.fetch(URL_ENDPOINT, {
      method: "POST",
      headers: { Authorization: `Bearer ${props.appToken}` },
      signal: props.signal,
    }),
  )

  if (response instanceof Error) {
    const error = safeNormalizeError({ value: response })
    log.error({ action: "http.error", message: safeErrorMessage({ error }), error })
    return new FlumeConnectionError(`apps.connections.open transport: ${response.message}`, {
      cause: response,
    })
  }

  log.debug({
    action: "http.response",
    message: `POST ${response.status}`,
    detail: { status: response.status, url: URL_ENDPOINT },
  })

  const text = await safeReadText({ response, context: "apps.connections.open" })
  if (text instanceof FlumeHttpError) {
    log.warn({ action: "http.body.read", message: safeErrorMessage({ error: text }), error: text })
    return text
  }

  const json = safeJsonParse(text)

  if (json instanceof FlumeParseError) {
    log.warn({ action: "http.body.parse", message: json.message, error: json })
    return new FlumeHttpError({
      message: `apps.connections.open: invalid JSON body`,
      status: response.status,
      cause: json,
    })
  }

  const peek = isRecord(json) ? json : {}
  log.debug({
    action: "http.body",
    message: "apps.connections.open response",
    detail: { ok: peek.ok, error: peek.error },
  })

  const parsed = FlumeSlackConnectionResponseSchema.safeParse(json)

  if (!parsed.success) {
    log.warn({
      action: "http.body.schema",
      message: "apps.connections.open: invalid response shape",
      detail: { issues: parsed.error.issues.map((i) => ({ path: i.path, message: i.message })) },
    })
    return new FlumeHttpError({
      message: "apps.connections.open: invalid response shape",
      status: response.status,
      cause: parsed.error,
    })
  }

  if (!parsed.data.ok || !parsed.data.url) {
    log.warn({
      action: "slack.api.error",
      message: `apps.connections.open failed: ${parsed.data.error ?? "no url"}`,
    })
    return new FlumeHttpError({
      message: `apps.connections.open failed: ${parsed.data.error ?? "no url"}`,
      status: response.status,
    })
  }

  log.info({ action: "slack.url.obtained", message: "WSS URL obtained" })
  return parsed.data.url
}
