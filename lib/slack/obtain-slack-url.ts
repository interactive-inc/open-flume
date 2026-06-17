import type { FlumeLogHandler, FlumeRuntimeDeps } from "@/types"
import { FlumeSlackConnectionResponseSchema } from "@/schema"
import { FlumeHttpError } from "@/errors/http-error"
import { FlumeLogger } from "@/logger"

type Props = {
  appToken: string
  onLog?: FlumeLogHandler
  deps: Pick<FlumeRuntimeDeps, "fetch" | "now">
}

export async function obtainSlackUrl(props: Props): Promise<string | FlumeHttpError> {
  const log = new FlumeLogger({ source: "slack.url", handler: props.onLog, deps: props.deps })
  const url = "https://slack.com/api/apps.connections.open"

  log.debug({ action: "http.request", message: `POST ${url}` })

  const response = await safeFetch(props, url, log)
  if (response instanceof FlumeHttpError) return response

  log.debug({ action: "http.response", message: `POST ${response.status}`, detail: { status: response.status, url } })

  const raw: unknown = await response.json()
  log.debug({ action: "http.body", message: "apps.connections.open response", detail: { ok: isOk(raw), error: errorField(raw) } })

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

function isOk(raw: unknown): unknown {
  return (raw as { ok?: unknown } | null)?.ok
}

function errorField(raw: unknown): unknown {
  return (raw as { error?: unknown } | null)?.error
}

async function safeFetch(props: Props, url: string, log: FlumeLogger): Promise<Response | FlumeHttpError> {
  try {
    return await props.deps.fetch(url, {
      method: "POST",
      headers: { Authorization: `Bearer ${props.appToken}` },
    })
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    log.error({ action: "http.error", message: `network error: ${err.message}`, error: err })
    return new FlumeHttpError({ message: err.message, status: 0 })
  }
}
