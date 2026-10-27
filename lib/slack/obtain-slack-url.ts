import type { FlumeLogHandler, FlumeRuntimeDeps } from "@/types"
import { FlumeSlackConnectionResponseSchema } from "@/slack/slack-connection-response-schema"
import { readSlackRetryAfterMs } from "@/slack/read-slack-retry-after-ms"
import { slackErrorCode } from "@/slack/slack-error-code"
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
      redirect: "error",
    }),
  )

  if (response instanceof Error) {
    const error = safeNormalizeError({ value: response })
    log.error({ action: "http.error", message: safeErrorMessage({ error }), error })
    return new FlumeConnectionError(`apps.connections.open transport: ${response.message}`, {
      cause: response,
      context: { operation: "slack.apps.connections.open", phase: "HTTP request" },
    })
  }

  log.debug({
    action: "http.response",
    message: `POST ${response.status}`,
    detail: { status: response.status, url: URL_ENDPOINT },
  })

  const retryAfterMs = readSlackRetryAfterMs({ response })
  const httpOk = response.status >= 200 && response.status < 300
  const requestId = attempt(() => response.headers?.get("x-slack-req-id"))
  const context = {
    operation: "slack.apps.connections.open",
    phase: "HTTP response body",
    httpStatus: response.status,
    ...(typeof requestId === "string" ? { requestId } : {}),
  }

  if (response.status === 429) {
    log.warn({
      action: "http.rate-limited",
      message: `apps.connections.open: rate limited (429), retry-after ${retryAfterMs ?? "unknown"}ms`,
      detail: { retryAfterMs },
    })
    return new FlumeHttpError({
      context,
      errorCode: "FLUME_RATE_LIMITED",
      message: "apps.connections.open: rate limited (429)",
      status: response.status,
      retryAfterMs,
    })
  }

  const text = await safeReadText({ response, context: "apps.connections.open" })
  if (text instanceof FlumeHttpError) {
    log.warn({ action: "http.body.read", message: safeErrorMessage({ error: text }), error: text })
    return new FlumeHttpError({
      message: text.message,
      status: response.status,
      cause: text,
      context,
      retryAfterMs,
      errorCode: slackErrorCode(response.status),
    })
  }

  const json = safeJsonParse(text)

  if (json instanceof FlumeParseError) {
    log.warn({ action: "http.body.parse", message: json.message, error: json })
    return new FlumeHttpError({
      context,
      errorCode: httpOk ? "FLUME_PARSE_FAILED" : slackErrorCode(response.status),
      message: `apps.connections.open: invalid JSON body`,
      status: response.status,
      cause: json,
      retryAfterMs,
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
      context,
      errorCode: httpOk ? "FLUME_PARSE_FAILED" : slackErrorCode(response.status),
      message: "apps.connections.open: invalid response shape",
      status: response.status,
      cause: parsed.error,
      retryAfterMs,
    })
  }

  if (!httpOk || !parsed.data.ok || !parsed.data.url) {
    const errorCode =
      !parsed.data.url && parsed.data.ok && httpOk
        ? "FLUME_PROTOCOL_INVALID"
        : slackErrorCode(response.status, parsed.data.error)
    const message = `apps.connections.open failed: HTTP ${response.status}, ${parsed.data.error ?? (httpOk ? "no url" : "unsuccessful HTTP status")}`
    log.warn({
      action: "slack.api.error",
      errorCode,
      message,
      detail: { ...context, code: parsed.data.error ?? null },
    })
    return new FlumeHttpError({
      context,
      errorCode,
      message,
      status: response.status,
      code: parsed.data.error ?? null,
      retryAfterMs,
    })
  }

  log.info({ action: "slack.url.obtained", message: "WSS URL obtained" })
  return parsed.data.url
}
