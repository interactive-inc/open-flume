import type { FlumeLogHandler, FlumeRuntimeDeps } from "@/types"
import { FlumeSlackConnectionResponseSchema } from "@/slack/slack-connection-response-schema"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeLogger } from "@/logger"
import { isRecord } from "@/utils/is-record"
import { safeFetch } from "@/utils/safe-fetch"

type Props = {
  appToken: string
  onLog?: FlumeLogHandler
  deps: Pick<FlumeRuntimeDeps, "fetch" | "now">
}

export async function obtainSlackUrl(props: Props): Promise<string | FlumeHttpError> {
  const log = new FlumeLogger({ source: "slack.url", handler: props.onLog, deps: props.deps })
  const url = "https://slack.com/api/apps.connections.open"

  log.debug({ action: "http.request", message: `POST ${url}` })

  const response = await safeFetch({
    fetch: props.deps.fetch,
    url,
    init: { method: "POST", headers: { Authorization: `Bearer ${props.appToken}` } },
    log,
  })
  if (response instanceof Error) return new FlumeHttpError({ message: response.message, status: 0 })

  log.debug({ action: "http.response", message: `POST ${response.status}`, detail: { status: response.status, url } })

  const raw: unknown = await response.json()
  const peek = isRecord(raw) ? raw : {}
  log.debug({ action: "http.body", message: "apps.connections.open response", detail: { ok: peek.ok, error: peek.error } })

  const parsed = FlumeSlackConnectionResponseSchema.safeParse(raw)

  if (!parsed.success) {
    log.warn({
      action: "parse.fail",
      message: "apps.connections.open: invalid response shape",
      detail: { issues: parsed.error.issues.map((i) => ({ path: i.path, message: i.message })) },
    })
    return new FlumeHttpError({ message: "apps.connections.open: invalid response shape", status: response.status })
  }

  if (!parsed.data.ok || !parsed.data.url) {
    log.warn({ action: "api.fail", message: `apps.connections.open failed: ${parsed.data.error ?? "no url"}` })
    return new FlumeHttpError({
      message: `apps.connections.open failed: ${parsed.data.error ?? "no url"}`,
      status: response.status,
    })
  }

  log.info({ action: "url.obtained", message: "WSS URL obtained" })
  return parsed.data.url
}

