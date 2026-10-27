import type { FlumeErrorContext } from "@/errors/error-context"
import { FlumeConnectionError } from "@/errors/connection-error"
import type { FlumeRuntimeDeps } from "@/types"
import type { FlumeLogger } from "@/logger"
import { attempt } from "@/utils/attempt"

type Props<T> = {
  context: () => FlumeErrorContext
  log: FlumeLogger
  run: (signal: AbortSignal) => Promise<T>
  signal?: AbortSignal
  timeoutMs: number
  deps: Pick<FlumeRuntimeDeps, "setTimeout" | "clearTimeout">
}

/** fetch と body 読取を同じ期限で囲む。中断非対応の IO も待ち続けず、遅延結果は採用しない。 */
export async function withDeadline<T>(props: Props<T>): Promise<T | Error> {
  const controller = new AbortController()
  const interrupted = Promise.withResolvers<Error>()
  const abort = (error: Error) => {
    interrupted.resolve(error)
    controller.abort()
  }
  const onAbort = () =>
    abort(
      new FlumeConnectionError("operation aborted", {
        errorCode: "FLUME_ABORTED",
        context: props.context(),
      }),
    )
  const listener = attempt(() => props.signal?.addEventListener("abort", onAbort, { once: true }))
  if (listener instanceof Error) return listener
  if (props.signal?.aborted) onAbort()

  let active = true
  const timer = attempt(() =>
    props.deps.setTimeout(() => {
      if (active)
        abort(
          new FlumeConnectionError(`operation timed out after ${props.timeoutMs}ms`, {
            errorCode: "FLUME_TIMEOUT",
            context: props.context(),
          }),
        )
    }, props.timeoutMs),
  )
  const result =
    timer instanceof Error
      ? new FlumeConnectionError(`timer scheduling failed: ${timer.message}`, {
          errorCode: "FLUME_TIMER_FAILED",
          cause: timer,
          context: props.context(),
        })
      : controller.signal.aborted
        ? await interrupted.promise
        : await Promise.race([attempt(() => props.run(controller.signal)), interrupted.promise])
  active = false
  if (timer instanceof Error) controller.abort()
  else {
    const cleared = attempt(() => props.deps.clearTimeout(timer))
    if (cleared instanceof Error)
      props.log.error({ action: "deadline.clear.error", message: cleared.message, error: cleared })
  }
  const detached = attempt(() => props.signal?.removeEventListener("abort", onAbort))
  if (detached instanceof Error)
    props.log.error({ action: "deadline.detach.error", message: detached.message, error: detached })
  return result
}
